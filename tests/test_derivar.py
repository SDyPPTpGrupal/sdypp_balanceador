"""Pruebas de derivar(): el camino handler → cola → recolector → handler.

Con una cola de mentira en memoria, porque lo que se prueba acá no es la cola
(eso está en `test_colas.py` y `test_servidor_cola.py`) sino lo que el
balanceador hace con lo que la cola le contesta: qué código HTTP sale, qué
queda en la bitácora y qué pasa cuando la cola no está.

Ese último caso es el importante del refactor: sacar la cola a un proceso
aparte agregó un modo de falla que antes no existía —la cola caída— y el
balanceador tiene que contestarlo rápido y decirlo, no colgar al cliente.

    python -m unittest discover -s tests -v
"""

import os
import sys
import tempfile
import threading
import time
import unittest
from collections import deque

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "app"))

import balanceador  # noqa: E402
from clientecola import ErrorCola  # noqa: E402

IDENTIDAD = "balanceador@casa-de-prueba"


class ColaFalsa:
    """El sistema de colas, de mentira y en memoria.

    Tiene la misma interfaz que `ClienteCola` —las tres cosas que el balanceador
    le pide— y además un `atender()` que hace de worker de réplica, para poder
    escribir las pruebas como "llega el pedido, tal réplica contesta tal cosa".
    """

    url = "http://cola-falsa"

    def __init__(self):
        self.publicados = []
        self.caida = False
        self.respuesta_a_publicar = (202, {})
        self._respuestas = deque()
        self._hay = threading.Condition()
        self.estado_falso = {"pedidos": {"esperando": 0, "enVuelo": 0, "cota": 100},
                             "respuestas": {"pendientes": 0}, "consumidores": {}}

    # -- lo que usa el balanceador --

    def publicar_pedido(self, pedido):
        if self.caida:
            raise ErrorCola("connection refused")
        self.publicados.append(pedido)
        return self.respuesta_a_publicar

    def tomar_respuesta(self, destinatario, espera):
        if self.caida:
            raise ErrorCola("connection refused")
        limite = time.monotonic() + espera
        with self._hay:
            while not self._respuestas:
                if time.monotonic() >= limite:
                    return 204, {}
                self._hay.wait(timeout=0.05)
            return 200, self._respuestas.popleft()

    def estado(self):
        return None if self.caida else self.estado_falso

    def master_conocido(self):
        return None if self.caida else self.url

    def instancias(self):
        if self.caida:
            return [{"url": self.url, "instancia": "cola-falsa", "rol": "caido", "termino": 0}]
        return [{"url": self.url, "instancia": "cola-falsa", "rol": "master", "termino": 1}]

    # -- lo que hace de worker en las pruebas --

    def atender(self, estado="OK", contenido=None, atendido_por="10.0.0.1:8080",
                intentos=None, id=None, espera_ms=7):
        pedido = self.publicados[-1] if id is None else \
            next(p for p in self.publicados if p["id"] == id)
        self.entregar({
            "id": pedido["id"],
            "operacion": pedido["operacion"],
            "estado": estado,
            "contenido": contenido if contenido is not None else {},
            "atendidoPor": atendido_por,
            "app": "python",
            "intentos": intentos or [atendido_por],
            "esperaMs": espera_ms,
        })

    def entregar(self, respuesta):
        with self._hay:
            self._respuestas.append(respuesta)
            self._hay.notify()


class ConBalanceador(unittest.TestCase):
    """Balanceador con la cola falsa y un recolector de verdad."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        balanceador.DIRECTORIO_LOGS = self._tmp.name
        balanceador.ARCHIVO_BITACORA = os.path.join(self._tmp.name, "bitacora.log")
        balanceador.PRESUPUESTO = 1.0
        balanceador.GRACIA = 0.3
        balanceador.IDENTIDAD = IDENTIDAD
        # El long-poll del recolector, corto: en producción son 20 s y acá cada
        # test tendría que esperarlos para que el hilo mire si lo pararon.
        balanceador.ESPERA_RECOLECTOR = 0.05
        balanceador.ESPERA_REINTENTO = 0.05
        balanceador.ESPERAS.clear()

        self.cola = ColaFalsa()
        balanceador.CLIENTE = self.cola
        balanceador.POOL = balanceador.Pool()

        self.vivo = True
        self.recolector = threading.Thread(
            target=balanceador.recolectar, args=(lambda: self.vivo,), daemon=True)
        self.recolector.start()
        self.addCleanup(self.parar)

    def parar(self):
        self.vivo = False
        self.recolector.join(timeout=2)

    def bitacora(self):
        try:
            with open(balanceador.ARCHIVO_BITACORA, encoding="utf-8") as f:
                return f.read()
        except OSError:
            return ""


class PruebasCaminoFeliz(ConBalanceador):

    def test_el_pedido_viaja_con_todo_lo_que_la_cola_necesita(self):
        """El pedido es datos, no un callable: tiene que poder llegar como JSON
        hasta un worker que quizá ni siquiera esté escrito en Python."""
        threading.Timer(0.05, lambda: self.cola.atender(contenido={"pong": "hola"})).start()
        balanceador.derivar("POST /echo", {"ping": "hola"}, cliente="10.0.0.9")
        p = self.cola.publicados[0]
        self.assertEqual(p["operacion"], "POST /echo")
        self.assertEqual(p["parametros"], {"ping": "hola"})
        self.assertEqual(p["destinatario"], IDENTIDAD)
        self.assertEqual(p["cliente"], "10.0.0.9")
        self.assertEqual(p["presupuestoMs"], 1000)
        self.assertRegex(p["id"], r"^[0-9a-f]{32}$")

    def test_ok_devuelve_el_contenido_tal_cual_y_quien_lo_dio(self):
        """El balanceador no reescribe el payload: lo arma quien tiene los datos.
        Es lo que hace que agregar un campo a la app no obligue a tocar acá."""
        threading.Timer(0.05, lambda: self.cola.atender(
            contenido={"personas": [{"id": 1}], "servidoPor": "python-1"})).start()
        codigo, contenido, destino, detalle = balanceador.derivar("GET /personas")
        self.assertIsNone(codigo)
        self.assertEqual(contenido["personas"], [{"id": 1}])
        self.assertEqual(destino, "10.0.0.1:8080")
        self.assertIn("espera=7ms", detalle)
        self.assertRegex(detalle, r"req=[0-9a-f]{32}")
        self.assertNotIn("intentos=", detalle)      # un solo intento

    def test_el_estado_de_la_replica_se_traduce_a_http(self):
        threading.Timer(0.05, lambda: self.cola.atender(
            estado="ALREADY_EXISTS", contenido={"error": "legajo repetido"})).start()
        codigo, contenido, destino, detalle = balanceador.derivar(
            "POST /personas", {"legajo": 1}, idempotente=False)
        self.assertEqual(codigo, 409)
        self.assertEqual(contenido, {"error": "legajo repetido"})
        self.assertTrue(detalle.startswith("ALREADY_EXISTS "))

    def test_la_reasignacion_queda_en_el_detalle(self):
        """`intentos=a→b` es la evidencia de que el pedido cambió de réplica sin
        que el cliente se entere. Es lo que se muestra en la demo junto al
        `docker stop`."""
        threading.Timer(0.05, lambda: self.cola.atender(
            atendido_por="10.0.0.2:8080",
            intentos=["10.0.0.1:8080", "10.0.0.2:8080"])).start()
        codigo, _, destino, detalle = balanceador.derivar("GET /personas")
        self.assertIsNone(codigo)
        self.assertEqual(destino, "10.0.0.2:8080")
        self.assertIn("intentos=10.0.0.1:8080→10.0.0.2:8080", detalle)

    def test_el_mismo_id_esta_en_el_pedido_y_en_la_bitacora(self):
        """Es lo que permite seguir una request por la bitácora del balanceador,
        la de la cola y la de la réplica que la atendió."""
        threading.Timer(0.05, lambda: self.cola.atender()).start()
        _, _, _, detalle = balanceador.derivar("GET /")
        balanceador.bitacora("GET /", 200, "10.0.0.1:8080", detalle)
        self.assertIn(self.cola.publicados[0]["id"], self.bitacora())


class PruebasCuandoAlgoFalla(ConBalanceador):

    def test_la_cola_caida_es_503_en_el_acto(self):
        """El modo de falla que este refactor agrega: sin cola no se atiende
        nada, aunque las cuatro réplicas estén perfectas. Hay que contestarlo
        rápido y nombrarlo, no colgar al cliente el presupuesto entero."""
        self.cola.caida = True
        antes = time.monotonic()
        codigo, contenido, destino, detalle = balanceador.derivar("GET /")
        self.assertEqual(codigo, 503)
        self.assertLess(time.monotonic() - antes, 0.2)
        self.assertIn("colas no responde", contenido["error"])
        self.assertTrue(detalle.startswith("cola caída:"))

    def test_la_cola_llena_es_503_con_los_numeros(self):
        self.cola.respuesta_a_publicar = (503, {"error": "cola llena", "esperando": 100,
                                                "cota": 100})
        codigo, contenido, _, detalle = balanceador.derivar("GET /")
        self.assertEqual(codigo, 503)
        self.assertEqual(contenido, {"error": "cola llena"})
        self.assertIn("cola llena (100/100)", detalle)

    def test_la_cola_que_rechaza_el_pedido_es_502_y_no_503(self):
        """Un 400 de la cola es un error nuestro armando el pedido, no
        saturación: mezclarlos haría buscar el problema en el lugar equivocado."""
        self.cola.respuesta_a_publicar = (400, {"error": "faltan operacion y/o destinatario"})
        codigo, _, _, detalle = balanceador.derivar("GET /")
        self.assertEqual(codigo, 502)
        self.assertIn("HTTP 400", detalle)

    def test_el_vencimiento_lo_informa_la_cola(self):
        """El camino normal de un 504: la cola sabe cuándo venció el pedido y
        manda la respuesta. El handler no tiene que esperar su propio timeout."""
        threading.Timer(0.05, lambda: self.cola.atender(
            estado="DEADLINE_EXCEEDED", contenido={"error": "venció esperando en la cola"},
            atendido_por=None, intentos=[])).start()
        antes = time.monotonic()
        codigo, contenido, _, detalle = balanceador.derivar("GET /")
        self.assertEqual(codigo, 504)
        self.assertLess(time.monotonic() - antes, 0.5)     # mucho menos que el presupuesto
        self.assertIn("venció esperando", contenido["error"])

    def test_si_la_cola_se_queda_muda_el_handler_se_rinde_solo(self):
        """La red de contención: si salta este timeout y no el de la cola, el
        problema es la cola, y el detalle lo dice para no buscarlo en las réplicas."""
        antes = time.monotonic()
        codigo, contenido, destino, detalle = balanceador.derivar("GET /")
        tardo = time.monotonic() - antes
        self.assertEqual(codigo, 504)
        self.assertGreaterEqual(tardo, 1.0)                # presupuesto
        self.assertLess(tardo, 1.6)                        # presupuesto + gracia
        self.assertIsNone(destino)
        self.assertIn("sin noticias de la cola", detalle)

    def test_una_respuesta_que_llega_tarde_no_rompe_nada(self):
        """La que llegó después de que su handler se rindió, o la segunda de un
        pedido que dos réplicas atendieron. Se anota y se tira: despertar a
        nadie es exactamente lo que hay que hacer con ella."""
        balanceador.derivar("GET /")                        # se rinde por timeout
        self.cola.atender()
        for _ in range(40):
            if "TARDE" in self.bitacora():
                break
            time.sleep(0.05)
        self.assertIn("llegó sin nadie esperándola", self.bitacora())
        self.assertEqual(balanceador.ESPERAS, {})

    def test_no_queda_basura_en_esperas_despues_de_cada_camino(self):
        """`ESPERAS` crece con cada request: si un camino de error se olvidara de
        limpiarlo, el balanceador perdería memoria de a una request por vez."""
        self.cola.caida = True
        balanceador.derivar("GET /")
        self.cola.caida = False
        self.cola.respuesta_a_publicar = (503, {"esperando": 1, "cota": 1})
        balanceador.derivar("GET /")
        self.cola.respuesta_a_publicar = (202, {})
        threading.Timer(0.05, lambda: self.cola.atender()).start()
        balanceador.derivar("GET /")
        self.assertEqual(balanceador.ESPERAS, {})


class PruebasSalud(ConBalanceador):

    def test_los_contadores_por_replica_salen_de_la_cola(self):
        """El balanceador ya no puede contarlos: los pedidos no pasan por él."""
        self.cola.estado_falso["consumidores"] = {
            "10.0.0.1:8080": {"enVuelo": 2, "atendidos": 40, "ultimoPedidoHaceMs": 120}}
        balanceador.POOL.agregar("10.0.0.1:8080")
        balanceador.POOL.agregar("10.0.0.2:8080")
        backends, cola = balanceador.backends_json()
        por_destino = {b["destino"]: b for b in backends}
        self.assertEqual(por_destino["10.0.0.1:8080"]["enVuelo"], 2)
        self.assertEqual(por_destino["10.0.0.1:8080"]["atendidos"], 40)
        self.assertTrue(por_destino["10.0.0.1:8080"]["consumiendo"])
        self.assertIsNotNone(cola)

    def test_una_replica_sana_que_no_consume_se_ve(self):
        """El síntoma nuevo de este diseño: el contenedor vive y contesta el
        health gRPC, pero su worker no arrancó o no alcanza la cola. Antes no se
        podía ni representar, porque el worker era nuestro."""
        b = balanceador.POOL.agregar("10.0.0.3:8080")
        b.sano = True
        backends, _ = balanceador.backends_json()
        self.assertTrue(backends[0]["sano"])
        self.assertFalse(backends[0]["consumiendo"])
        self.assertIsNone(backends[0]["ultimoPedidoHaceMs"])

    def test_sin_cola_no_hay_servicio_aunque_sobren_replicas(self):
        self.cola.caida = True
        b = balanceador.POOL.agregar("10.0.0.1:8080")
        b.sano = True
        backends, cola = balanceador.backends_json()
        self.assertIsNone(cola)
        self.assertEqual(backends[0]["enVuelo"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
