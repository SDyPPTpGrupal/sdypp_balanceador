"""Pruebas del sistema de colas por HTTP, con un servidor de verdad.

Acá no se prueba la lógica de las colas (eso está en `test_colas.py`) sino el
contrato que ven los dos lados: el balanceador de un lado y los workers de las
réplicas del otro. Es el contrato entre repos que tiene que implementar quien
escriba el worker, así que si algo de esto cambia hay que avisarle al equipo.

Se levanta el servidor en un puerto libre y se lo habla con `ClienteCola`, que
es el cliente real del balanceador: así una prueba verde quiere decir que los
dos extremos hablan el mismo idioma, no que cada uno habla consigo mismo.

    python -m unittest discover -s tests -v
"""

import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "cola"))
sys.path.insert(0, os.path.join(RAIZ, "app"))

import servidor  # noqa: E402
from clientecola import ClienteCola, ErrorCola  # noqa: E402
from colas import Sistema  # noqa: E402

BALANCEADOR = "balanceador@casa-tomas"
REPLICA = "10.0.0.1:8080"


class ConServidor(unittest.TestCase):
    """Levanta la cola en un puerto libre y la baja al terminar."""

    token = ""
    cota_pedidos = 10

    def setUp(self):
        servidor.SISTEMA = Sistema(self.cota_pedidos, 10, reserva=0.2, ttl_respuestas=5)
        servidor.TOKEN = self.token
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        servidor.DIRECTORIO_LOGS = tmp.name
        self.http = ThreadingHTTPServer(("127.0.0.1", 0), servidor.Manejador)
        self.http.daemon_threads = True
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.addCleanup(self.http.server_close)
        self.addCleanup(self.http.shutdown)
        self.url = f"http://127.0.0.1:{self.http.server_address[1]}"
        self.cliente = ClienteCola(self.url, self.token, timeout=3)
        self.addCleanup(self.cliente.cerrar)

    # El lado del worker no tiene cliente propio todavía (lo escribe quien haga
    # el worker de la réplica), así que se lo habla con urllib pelado: es
    # exactamente lo que va a hacer él, con el mismo JSON.
    def worker(self, ruta, cuerpo):
        pedido = urllib.request.Request(
            self.url + ruta, data=json.dumps(cuerpo).encode(), method="POST",
            headers={"Content-Type": "application/json",
                     **({"X-Cola-Token": self.token} if self.token else {})})
        try:
            with urllib.request.urlopen(pedido, timeout=5) as r:
                crudo = r.read()
                return r.status, json.loads(crudo) if crudo else {}
        except urllib.error.HTTPError as e:
            crudo = e.read()
            return e.code, json.loads(crudo) if crudo else {}

    def publicar(self, id="p1", operacion="GET /personas", idempotente=True, presupuesto_ms=5000):
        return self.cliente.publicar_pedido({
            "id": id, "operacion": operacion, "parametros": {}, "idempotente": idempotente,
            "destinatario": BALANCEADOR, "presupuestoMs": presupuesto_ms})


class PruebasCicloCompleto(ConServidor):

    def test_publicar_tomar_responder_recolectar(self):
        """El camino feliz entero, que es el contrato que implementa el worker."""
        codigo, datos = self.publicar()
        self.assertEqual(codigo, 202)
        self.assertEqual(datos["id"], "p1")

        codigo, pedido = self.worker("/pedidos/tomar", {"consumidor": REPLICA, "espera": 1})
        self.assertEqual(codigo, 200)
        self.assertEqual(pedido["operacion"], "GET /personas")
        self.assertEqual(pedido["intento"], 1)
        self.assertGreater(pedido["quedaMs"], 0)
        # No viaja ningún instante absoluto: los relojes de cuatro casas no
        # están sincronizados y `quedaMs` no depende del reloj del que lo lee.
        self.assertNotIn("vence_en", pedido)

        codigo, _ = self.worker("/respuestas", {
            "id": "p1", "estado": "OK", "contenido": {"personas": []},
            "atendidoPor": REPLICA, "app": "python"})
        self.assertEqual(codigo, 202)

        codigo_resp, respuesta = self.cliente.tomar_respuesta(BALANCEADOR, 1)
        self.assertEqual(codigo_resp, 200)
        self.assertEqual(respuesta["estado"], "OK")
        self.assertEqual(respuesta["contenido"], {"personas": []})
        self.assertEqual(respuesta["atendidoPor"], REPLICA)
        self.assertEqual(respuesta["intentos"], [REPLICA])

    def test_tomar_sin_nada_devuelve_204_y_no_un_cuerpo_vacio(self):
        """204 y no 200 con null: el worker distingue "no hay trabajo" de "hay
        trabajo pero vino mal" sin tener que mirar el cuerpo."""
        codigo, cuerpo = self.worker("/pedidos/tomar", {"consumidor": REPLICA, "espera": 0})
        self.assertEqual(codigo, 204)
        self.assertEqual(cuerpo, {})

    def test_el_long_poll_espera_al_pedido_que_todavia_no_llego(self):
        """Es lo que hace que un pedido no espere a la próxima vuelta de un
        bucle de sondeo: el worker ya está colgado y sale apenas aparece."""
        def publicar_en_un_rato():
            time.sleep(0.2)
            self.publicar()

        threading.Thread(target=publicar_en_un_rato, daemon=True).start()
        antes = time.monotonic()
        codigo, pedido = self.worker("/pedidos/tomar", {"consumidor": REPLICA, "espera": 3})
        tardo = time.monotonic() - antes
        self.assertEqual(codigo, 200)
        self.assertGreaterEqual(tardo, 0.2)
        self.assertLess(tardo, 1.0)

    def test_la_cola_llena_contesta_503_con_los_numeros(self):
        """El balanceador lo traduce a un 503 para el cliente y lo anota con la
        ocupación: es el dato que dice si hay que agregar réplicas o achicar el
        presupuesto."""
        self.cota_pedidos = 1
        self.setUp()
        self.assertEqual(self.publicar(id="a")[0], 202)
        codigo, datos = self.publicar(id="b")
        self.assertEqual(codigo, 503)
        self.assertEqual(datos["esperando"], 1)
        self.assertEqual(datos["cota"], 1)

    def test_devolver_lo_deja_disponible_para_otro(self):
        """El drenado de un blue/green: la réplica que se va suelta lo que tenía
        en la mano en vez de hacer esperar los segundos de la reserva."""
        self.publicar()
        self.worker("/pedidos/tomar", {"consumidor": REPLICA, "espera": 0})
        codigo, _ = self.worker("/pedidos/devolver", {"id": "p1", "consumidor": REPLICA})
        self.assertEqual(codigo, 200)
        codigo, pedido = self.worker("/pedidos/tomar", {"consumidor": "10.0.0.2:8080", "espera": 0})
        self.assertEqual(codigo, 200)
        self.assertEqual(pedido["intento"], 2)

    def test_la_respuesta_repetida_se_rechaza_con_409(self):
        self.publicar()
        self.worker("/pedidos/tomar", {"consumidor": REPLICA, "espera": 0})
        cuerpo = {"id": "p1", "estado": "OK", "contenido": {}, "atendidoPor": REPLICA}
        self.assertEqual(self.worker("/respuestas", cuerpo)[0], 202)
        codigo, datos = self.worker("/respuestas", cuerpo)
        self.assertEqual(codigo, 409)
        self.assertEqual(datos["resultado"], "desconocido")

    def test_la_reserva_vencida_reasigna_sin_que_nadie_avise(self):
        """La réplica se murió con el pedido en la mano. No hay quien avise: el
        recuperador de la cola lo nota por la reserva y lo vuelve a ofrecer.
        Es el `docker stop` de la demo."""
        self.publicar()
        self.worker("/pedidos/tomar", {"consumidor": "replica-muerta", "espera": 0})
        time.sleep(0.25)
        servidor.SISTEMA.recuperar()
        codigo, pedido = self.worker("/pedidos/tomar", {"consumidor": "10.0.0.2:8080", "espera": 1})
        self.assertEqual(codigo, 200)
        self.assertEqual(pedido["intento"], 2)


class PruebasValidacion(ConServidor):

    def test_falta_lo_obligatorio(self):
        self.assertEqual(self.worker("/pedidos", {"operacion": "GET /"})[0], 400)
        self.assertEqual(self.worker("/pedidos", {"destinatario": BALANCEADOR})[0], 400)
        self.assertEqual(self.worker("/pedidos/tomar", {})[0], 400)
        self.assertEqual(self.worker("/respuestas", {"id": "p1"})[0], 400)
        self.assertEqual(self.worker("/respuestas/tomar", {})[0], 400)

    def test_el_default_de_idempotente_es_el_seguro(self):
        """Ante la duda no se reintenta. Un pedido marcado idempotente por error
        se puede ejecutar dos veces; uno marcado de más sólo pierde un reintento."""
        self.cliente.publicar_pedido({"id": "p1", "operacion": "POST /personas",
                                      "destinatario": BALANCEADOR})
        self.worker("/pedidos/tomar", {"consumidor": "replica-muerta", "espera": 0})
        time.sleep(0.25)
        servidor.SISTEMA.recuperar()
        self.assertEqual(self.worker("/pedidos/tomar", {"consumidor": "otra", "espera": 0})[0], 204)

    def test_el_presupuesto_tiene_techo(self):
        """Sin esto, un cliente podría ocupar un lugar de la cola por horas."""
        servidor.PRESUPUESTO_MAXIMO = 1
        self.addCleanup(setattr, servidor, "PRESUPUESTO_MAXIMO", 60)
        self.publicar(presupuesto_ms=3_600_000)
        _, pedido = self.worker("/pedidos/tomar", {"consumidor": REPLICA, "espera": 0})
        self.assertLessEqual(pedido["quedaMs"], 1000)

    def test_la_espera_del_long_poll_tiene_techo(self):
        """Nadie retiene un hilo del servidor más que ESPERA_MAXIMA."""
        servidor.ESPERA_MAXIMA = 0.2
        self.addCleanup(setattr, servidor, "ESPERA_MAXIMA", 30)
        antes = time.monotonic()
        self.assertEqual(self.worker("/pedidos/tomar",
                                     {"consumidor": REPLICA, "espera": 9999})[0], 204)
        self.assertLess(time.monotonic() - antes, 1.0)

    def test_cuerpo_que_no_es_json(self):
        pedido = urllib.request.Request(self.url + "/pedidos", data=b"{no", method="POST")
        with self.assertRaises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(pedido, timeout=5)
        self.assertEqual(e.exception.code, 400)


class PruebasToken(ConServidor):
    """Sin token, cualquiera en el tailnet podría publicar pedidos falsos o
    —peor— tomarlos y quedarse con tráfico real de usuarios."""

    token = "secreto-de-prueba"

    def test_con_token_anda(self):
        self.assertEqual(self.publicar()[0], 202)

    def test_sin_token_es_403(self):
        pelado = ClienteCola(self.url, "", timeout=3)
        self.addCleanup(pelado.cerrar)
        codigo, _ = pelado.publicar_pedido({"id": "p1", "operacion": "GET /",
                                            "destinatario": BALANCEADOR})
        self.assertEqual(codigo, 403)

    def test_health_no_pide_token(self):
        """Lo consulta el HEALTHCHECK del contenedor, que no tiene por qué llevar
        el secreto adentro. No revela nada que no se vea igual desde afuera."""
        with urllib.request.urlopen(self.url + "/health", timeout=5) as r:
            self.assertEqual(r.status, 200)
            self.assertEqual(json.loads(r.read())["cola"], "sana")

    def test_el_403_no_envenena_la_conexion(self):
        """Contestar sin leer el cuerpo deja esos bytes en el socket, y con
        keep-alive el servidor los toma como la línea de pedido de la request
        siguiente. Se nota como un 400 en una request que estaba bien."""
        pelado = ClienteCola(self.url, "", timeout=3)
        self.addCleanup(pelado.cerrar)
        pedido = {"id": "p1", "operacion": "GET /", "destinatario": BALANCEADOR}
        self.assertEqual(pelado.publicar_pedido(pedido)[0], 403)
        self.assertEqual(pelado.publicar_pedido(pedido)[0], 403)   # misma conexión reusada

    def test_estado_si_pide_token(self):
        """`/estado` sí: dice qué réplicas consumen y cuánto tienen en vuelo."""
        with self.assertRaises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(self.url + "/estado", timeout=5)
        self.assertEqual(e.exception.code, 403)


class PruebasCliente(ConServidor):
    """El cliente del balanceador: lo poco que hace, que lo haga bien."""

    def test_la_conexion_se_reusa_entre_requests(self):
        """Una request por request del usuario más un long-poll permanente: con
        una conexión nueva cada vez se paga un handshake de más por pedido."""
        for i in range(3):
            self.assertEqual(self.publicar(id=f"p{i}")[0], 202)
        self.assertEqual(len(self.cliente._libres), 1)

    def test_un_204_no_envenena_la_conexion(self):
        """El 204 del long-poll vacío es la respuesta MÁS común de la cola: con
        el sistema ocioso es lo único que pasa. Si dejara bytes en el socket
        —los clientes HTTP no leen el cuerpo de un 204—, el próximo pedido
        moriría con `BadStatusLine` sobre esa misma conexión reusada, y el
        síntoma aparecería recién cuando llega tráfico real.
        """
        self.assertEqual(self.cliente.tomar_respuesta(BALANCEADOR, 0)[0], 204)   # 204
        self.assertEqual(len(self.cliente._libres), 1)                    # la guardó
        self.assertEqual(self.publicar()[0], 202)                         # y sirve
        self.assertEqual(self.cliente.tomar_respuesta(BALANCEADOR, 0)[0], 204)
        self.assertEqual(self.worker("/pedidos/tomar", {"consumidor": REPLICA, "espera": 0})[0],
                         200)

    def test_el_ciclo_entero_sobre_una_sola_conexion(self):
        """Lo que hace el recolector todo el día: long-polls vacíos intercalados
        con respuestas de verdad, siempre sobre la misma conexión."""
        for i in range(3):
            self.assertEqual(self.cliente.tomar_respuesta(BALANCEADOR, 0)[0], 204)
            self.publicar(id=f"p{i}")
            self.worker("/pedidos/tomar", {"consumidor": REPLICA, "espera": 0})
            self.worker("/respuestas", {"id": f"p{i}", "estado": "OK", "contenido": {},
                                        "atendidoPor": REPLICA})
            codigo_resp, resp = self.cliente.tomar_respuesta(BALANCEADOR, 1)
            self.assertEqual(codigo_resp, 200)
            self.assertEqual(resp["id"], f"p{i}")

    def test_la_cola_caida_es_un_error_distinto_de_la_cola_llena(self):
        """"No hay nadie" y "no entra" se atienden distinto: una es 503 por
        saturación y la otra es que el sistema de colas se cayó."""
        muerto = ClienteCola("http://127.0.0.1:1", timeout=0.5)
        with self.assertRaises(ErrorCola):
            muerto.publicar_pedido({"id": "p1", "operacion": "GET /",
                                    "destinatario": BALANCEADOR})
        self.assertIsNone(muerto.estado())

    def test_estado_trae_los_consumidores(self):
        self.publicar()
        self.worker("/pedidos/tomar", {"consumidor": REPLICA, "espera": 0})
        estado = self.cliente.estado()
        self.assertEqual(estado["pedidos"]["enVuelo"], 1)
        self.assertEqual(estado["consumidores"][REPLICA]["enVuelo"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
