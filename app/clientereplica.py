"""Cliente replicado para el sistema de colas (Raft-lite master/slave).

Encapsula el descubrimiento de master mediante lista de semillas (seed list),
el cacheo del master en estado estacionario, el seguimiento de redirecciones 421
exactamente una vez sin duplicar requests POST, backoff cuando el cluster no tiene
líder y la invalidación de caché ante errores de conexión.
"""

import random
import threading
import time
from urllib.parse import urlsplit

from clientecola import ClienteCola, ErrorCola


def _normalizar_url(url):
    partes = urlsplit(url if "://" in url else f"http://{url}")
    host = partes.hostname or "127.0.0.1"
    puerto = partes.port or 80
    return f"http://{host}:{puerto}"


class ClienteReplica:
    """Cliente para un cluster replicado de colas."""

    def __init__(
        self,
        urls,
        token="",
        timeout=5.0,
        conexiones=8,
        backoff_inicial=0.1,
        backoff_maximo=1.0,
        presupuesto=5.0,
    ):
        if not urls:
            raise ValueError("urls no puede ser vacía")

        self._urls = [_normalizar_url(u) for u in urls]
        self._seed_set = set(self._urls)
        self.token = token
        self.timeout = timeout
        self.conexiones = conexiones
        self.backoff_inicial = backoff_inicial
        self.backoff_maximo = backoff_maximo
        self.presupuesto = presupuesto

        self._lock = threading.Lock()
        self._clientes = {}
        self._offset = 0
        self._known_slaves = set()

        # Un solo nodo es trivialmente el master inicial
        if len(self._urls) == 1:
            self._master = self._urls[0]
        else:
            self._master = None

    def _obtener_cliente(self, url):
        with self._lock:
            if url not in self._clientes:
                self._clientes[url] = ClienteCola(
                    url,
                    token=self.token,
                    timeout=self.timeout,
                    conexiones=self.conexiones,
                )
            return self._clientes[url]

    def master_conocido(self):
        """Retorna la URL del master actualmente cacheado, o None."""
        with self._lock:
            return self._master

    def _descubrir_master(self, presupuesto_restante):
        """Sondea la lista de semillas vía GET /health hasta encontrar un master."""
        t0 = time.monotonic()
        limite = t0 + max(0.0, presupuesto_restante)
        actual_backoff = self.backoff_inicial

        while time.monotonic() < limite:
            with self._lock:
                start = self._offset
                self._offset = (self._offset + 1) % len(self._urls)
            candidatos = [
                self._urls[(start + i) % len(self._urls)]
                for i in range(len(self._urls))
            ]

            for url in candidatos:
                try:
                    cli = self._obtener_cliente(url)
                    timeout = min(self.timeout, max(0.5, limite - time.monotonic()))
                    codigo, datos = cli._pedir("GET", "/health", timeout=timeout)
                    if codigo == 200 and isinstance(datos, dict):
                        rol = datos.get("rol")
                        master_conocido = datos.get("masterConocido")
                        if master_conocido:
                            master_conocido = _normalizar_url(master_conocido)

                        if rol == "slave":
                            self._known_slaves.add(url)
                        else:
                            self._known_slaves.discard(url)

                        if rol == "master":
                            with self._lock:
                                self._master = url
                                self._known_slaves.discard(url)
                            return url

                        if (
                            master_conocido
                            and master_conocido in self._seed_set
                            and master_conocido not in self._known_slaves
                        ):
                            with self._lock:
                                self._master = master_conocido
                                self._known_slaves.discard(master_conocido)
                            return master_conocido
                except (ErrorCola, Exception):
                    continue

            # No se encontró master en esta pasada: backoff con jitter
            jitter = actual_backoff * random.uniform(-0.2, 0.2)
            dormir = max(0.01, actual_backoff + jitter)
            actual_backoff = min(actual_backoff * 2.0, self.backoff_maximo)

            tiempo_restante = limite - time.monotonic()
            if tiempo_restante <= 0:
                break
            time.sleep(min(dormir, tiempo_restante))

        return None

    def _ejecutar(self, invocar, es_idempotente=False):
        limite = time.monotonic() + self.presupuesto

        while time.monotonic() < limite:
            with self._lock:
                master = self._master

            # Si no hay master o el que conocemos pasó a ser slave, descubrimos
            if not master or master in self._known_slaves:
                master = self._descubrir_master(limite - time.monotonic())
                if not master:
                    return 503, {"error": "el sistema de colas no responde"}

            cli = self._obtener_cliente(master)
            try:
                codigo, datos = invocar(cli)
            except ErrorCola as e:
                with self._lock:
                    if self._master == master:
                        self._master = None
                # Un fallo en conexión fresca con estado desconocido no se reintenta a ciegas
                if getattr(e, "enviado", False) or not es_idempotente:
                    raise
                if time.monotonic() >= limite:
                    return 503, {"error": "el sistema de colas no responde"}
                continue

            # Manejo de redirección 421 Misdirected Request
            if codigo == 421:
                self._known_slaves.add(master)
                raw_redir = datos.get("master") if isinstance(datos, dict) else None
                redir = _normalizar_url(raw_redir) if raw_redir else None

                # Verificación de lista blanca (allowlist)
                if redir and redir in self._seed_set and redir != master:
                    with self._lock:
                        self._master = redir
                    self._known_slaves.discard(redir)

                    # Reintento único (one-shot redirect)
                    cli_redir = self._obtener_cliente(redir)
                    try:
                        codigo2, datos2 = invocar(cli_redir)
                    except ErrorCola as e:
                        with self._lock:
                            if self._master == redir:
                                self._master = None
                        if getattr(e, "enviado", False) or not es_idempotente:
                            raise
                        continue

                    if codigo2 == 421:
                        # Segundo 421: ciclo o redescubrimiento necesario, no loopear
                        self._known_slaves.add(redir)
                        with self._lock:
                            if self._master == redir:
                                self._master = None
                        master = self._descubrir_master(limite - time.monotonic())
                        if not master:
                            return 503, {"error": "el sistema de colas no responde"}
                        continue
                    else:
                        return codigo2, datos2
                else:
                    # Redirección inválida o desconocida -> master: null, correr descubrimiento
                    with self._lock:
                        if self._master == master:
                            self._master = None
                    master = self._descubrir_master(limite - time.monotonic())
                    if not master:
                        return 503, {"error": "el sistema de colas no responde"}
                    continue

            # Respuesta exitosa u otro código definitivo
            with self._lock:
                self._master = master
                self._known_slaves.discard(master)
            return codigo, datos

        return 503, {"error": "el sistema de colas no responde"}

    def publicar_pedido(self, pedido):
        """Publica un pedido en el master. Devuelve (codigo, datos)."""
        return self._ejecutar(lambda c: c.publicar_pedido(pedido), es_idempotente=False)

    def tomar_respuesta(self, destinatario, espera):
        """Toma la próxima respuesta desde el master. Devuelve (codigo, datos)."""
        return self._ejecutar(
            lambda c: c.tomar_respuesta(destinatario, espera),
            es_idempotente=True,
        )

    def estado(self):
        """Consulta el estado del cluster en el master, o None."""
        with self._lock:
            master = self._master
        if master:
            try:
                cli = self._obtener_cliente(master)
                res = cli.estado()
                if res is not None:
                    return res
            except Exception:
                pass
        master = self._descubrir_master(self.presupuesto)
        if master:
            try:
                cli = self._obtener_cliente(master)
                return cli.estado()
            except Exception:
                return None
        return None

    def instancias(self):
        """Devuelve el estado de salud de todos los nodos en la lista de semillas."""
        res = []
        for url in self._urls:
            try:
                cli = self._obtener_cliente(url)
                codigo, datos = cli._pedir("GET", "/health", timeout=min(self.timeout, 1.0))
                if codigo == 200 and isinstance(datos, dict):
                    res.append({
                        "url": url,
                        "instancia": datos.get("instancia", ""),
                        "rol": datos.get("rol", "desconocido"),
                        "termino": datos.get("termino", 0),
                    })
                else:
                    res.append({
                        "url": url,
                        "instancia": "",
                        "rol": "desconocido",
                        "termino": 0,
                    })
            except Exception:
                res.append({
                    "url": url,
                    "instancia": "",
                    "rol": "caido",
                    "termino": 0,
                })
        return res

    def cerrar(self):
        """Cierra todas las conexiones del pool."""
        with self._lock:
            clientes = list(self._clientes.values())
            self._clientes.clear()
            self._master = None
        for c in clientes:
            c.cerrar()
