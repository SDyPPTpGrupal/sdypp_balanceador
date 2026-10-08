"""El cliente HTTP del sistema de colas. Lo usa el balanceador.

Es deliberadamente chico: publicar un pedido, recolectar una respuesta y pedir
el estado. Nada de la lógica de la cola vive acá — si algo de esto tuviera que
decidir qué se reintenta, sería señal de que la cola quedó a medio sacar.

**Por qué `http.client` y no `urllib.request`.** `urllib` abre una conexión TCP
nueva por request y la cierra. Acá hay una request por cada request del usuario
más un long-poll permanente por recolector: pagar un handshake cada vez le
agregaría un round-trip a algo que ya cruza la red de más que antes. `http.client`
deja reusar la conexión, así que se mantiene una pileta chica de conexiones
abiertas y cada hilo saca una, la usa y la devuelve.
"""

import http.client
import json
import threading
from collections import deque
from urllib.parse import urlsplit


class ErrorCola(Exception):
    """No se pudo hablar con el sistema de colas. Es distinto de que la cola
    conteste que está llena: eso es una respuesta, esto es que no hay nadie."""

    def __init__(self, mensaje, enviado=False):
        super().__init__(mensaje)
        self.enviado = enviado


class ClienteCola:

    def __init__(self, url, token="", timeout=5.0, conexiones=8):
        partes = urlsplit(url if "://" in url else f"http://{url}")
        self.host = partes.hostname or "127.0.0.1"
        self.puerto = partes.port or 80
        self.url = f"http://{self.host}:{self.puerto}"
        self.token = token
        self.timeout = timeout
        self._maximo = conexiones
        self._libres = deque()
        self._lock = threading.Lock()

    # -- lo que usa el balanceador --

    def publicar_pedido(self, pedido):
        """Devuelve (codigo_http, datos). 202 = encolado, 503 = cola llena."""
        return self._pedir("POST", "/pedidos", pedido)

    def tomar_respuesta(self, destinatario, espera):
        """La próxima respuesta para `destinatario`, como (codigo, datos).

        `espera` es un long-poll: la cola cuelga la conexión hasta que aparezca
        una respuesta o pasen esos segundos. El timeout de socket va más alto
        que la espera, o el cliente cortaría justo antes de que la cola conteste.
        """
        return self._pedir("POST", "/respuestas/tomar",
                           {"destinatario": destinatario, "espera": espera},
                           timeout=espera + self.timeout)

    def estado(self):
        """El detalle de las dos colas, para /health. None si la cola no responde."""
        try:
            codigo, datos = self._pedir("GET", "/estado")
        except ErrorCola:
            return None
        return datos if codigo == 200 else None

    # -- la pileta de conexiones --

    def _sacar(self):
        with self._lock:
            if self._libres:
                return self._libres.popleft(), True
        return http.client.HTTPConnection(self.host, self.puerto, timeout=self.timeout), False

    def _guardar(self, conexion):
        with self._lock:
            if len(self._libres) < self._maximo:
                self._libres.append(conexion)
                return
        conexion.close()

    def cerrar(self):
        with self._lock:
            libres, self._libres = self._libres, deque()
        for c in libres:
            c.close()

    def _pedir(self, metodo, ruta, cuerpo=None, timeout=None):
        """Una request. Reintenta mientras el fallo sea de una conexión reusada.

        El reintento es sólo para el caso de la conexión rancia: la cola cerró su
        lado por keep-alive timeout y no nos enteramos hasta que escribimos. Con
        una conexión **recién abierta** no se reintenta nada — un fallo ahí es un
        fallo de verdad y reintentarlo a ciegas duplicaría pedidos, que es
        exactamente lo que no queremos en `POST /personas`.

        Se descarta de a una y no se corta en el segundo intento porque el pool
        puede tener varias malas a la vez: un bug de un lado o del otro las
        rompe todas juntas, y rendirse en la segunda haría fallar requests que
        una tercera conexión habría atendido bien.
        """
        cuerpo_crudo = json.dumps(cuerpo, ensure_ascii=False).encode() if cuerpo is not None else None
        cabeceras = {"Content-Type": "application/json; charset=utf-8"}
        if self.token:
            cabeceras["X-Cola-Token"] = self.token

        for _ in range(self._maximo + 1):
            conexion, reusada = self._sacar()
            # El timeout se fija en cada request y no al crear la conexión: la
            # misma conexión reusada puede haber servido antes un long-poll de
            # 20 s y ahora un POST que tiene que fallar rápido.
            espera = self.timeout if timeout is None else timeout
            conexion.timeout = espera
            if conexion.sock is not None:
                conexion.sock.settimeout(espera)
            try:
                conexion.request(metodo, ruta, body=cuerpo_crudo, headers=cabeceras)
                respuesta = conexion.getresponse()
                datos = respuesta.read()
                codigo = respuesta.status
            except (OSError, http.client.HTTPException) as e:
                conexion.close()
                if reusada:
                    continue        # conexión rancia: se descarta y se prueba otra
                enviado = not isinstance(e, ConnectionRefusedError)
                raise ErrorCola(f"{type(e).__name__}: {e}", enviado=enviado) from e
            self._guardar(conexion)
            if codigo == 204 or not datos:
                return codigo, {}
            try:
                return codigo, json.loads(datos)
            except ValueError:
                return codigo, {}
        raise ErrorCola("todas las conexiones del pool fallaron", enviado=False)
