"""Pruebas de los pedidos asincrónicos: `Prefer: respond-async` y los tickets.

Contra el handler HTTP de verdad, porque buena parte del contrato son
cabeceras (`Location`, `Retry-After`, `Preference-Applied`), y con una cola de
mentira que, como la real, guarda las respuestas por destinatario: es lo que
permite retirar la de un ticket sin tocar las demás.

    python -m unittest discover -s tests -v
"""

import http.client
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from collections import defaultdict, deque
from http.server import ThreadingHTTPServer

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "app"))

import balanceador  # noqa: E402
from clientecola import ErrorCola  # noqa: E402


class ColaPorDestinatario:
    """La cola, de mentira: una caja de respuestas por destinatario, como la real."""

    def __init__(self):
        self.publicados = []
        self.caida = False
        self.retiros = 0
        self._cajas = defaultdict(deque)

    def publicar_pedido(self, pedido):
        if self.caida:
            raise ErrorCola("connection refused")
        self.publicados.append(pedido)
        return 202, {"id": pedido["id"], "encolado": True}

    def tomar_respuesta(self, destinatario, espera):
        if self.caida:
            raise ErrorCola("connection refused")
        self.retiros += 1
        caja = self._cajas.get(destinatario)
        if not caja:
            return 204, {}
        return 200, caja.popleft()

    # -- lo que hace de worker --

    def atender(self, estado="OK", contenido=None):
        pedido = self.publicados[-1]
        self._cajas[pedido["destinatario"]].append({
            "id": pedido["id"], "operacion": pedido["operacion"], "estado": estado,
            "contenido": contenido if contenido is not None else {},
            "atendidoPor": "10.0.0.1:8080", "app": "python",
            "intentos": ["10.0.0.1:8080"], "esperaMs": 3,
        })


class ConServidor(unittest.TestCase):

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        balanceador.DIRECTORIO_LOGS = tmp.name
        balanceador.ARCHIVO_BITACORA = os.path.join(tmp.name, "bitacora.log")
        balanceador.PRESUPUESTO_ASYNC = 60.0
        balanceador.TTL_TICKET = 60.0
        self.cola = ColaPorDestinatario()
        balanceador.CLIENTE = self.cola

        self.servidor = ThreadingHTTPServer(("127.0.0.1", 0), balanceador.ManejadorPublico)
        self.servidor.daemon_threads = True
        threading.Thread(target=self.servidor.serve_forever, daemon=True).start()
        self.addCleanup(self.servidor.server_close)
        self.addCleanup(self.servidor.shutdown)

    def pedir(self, metodo, ruta, cuerpo=None, asincronico=False):
        """(codigo, cabeceras, contenido) — el contenido ya sin el sobre."""
        conexion = http.client.HTTPConnection("127.0.0.1", self.servidor.server_port, timeout=5)
        cabeceras = {"Content-Type": "application/json"}
        if asincronico:
            cabeceras["Prefer"] = "respond-async"
        crudo = json.dumps(cuerpo).encode() if cuerpo is not None else None
        conexion.request(metodo, ruta, body=crudo, headers=cabeceras)
        r = conexion.getresponse()
        datos = json.loads(r.read())
        conexion.close()
        self.assertEqual(datos["Code"], r.status)       # el sobre no cambia
        return r.status, r.headers, datos["contenido"]

    def alta_asincronica(self):
        return self.pedir("POST", "/personas", {"nombre": "Ada", "legajo": "1234"},
                          asincronico=True)


class PruebasTicket(ConServidor):

    def test_el_alta_vuelve_en_el_acto_con_un_ticket(self):
        """Nadie atendió el pedido todavía: el cliente no se queda esperando."""
        antes = time.monotonic()
        codigo, cabeceras, contenido = self.alta_asincronica()
        self.assertLess(time.monotonic() - antes, 1)
        self.assertEqual(codigo, 202)
        self.assertEqual(contenido["estado"], "pendiente")
        ticket = contenido["id"]
        self.assertRegex(ticket, r"^[0-9a-f]{32}\.\d+$")
        self.assertEqual(cabeceras["Location"], f"/pedidos/{ticket}")
        self.assertEqual(cabeceras["Retry-After"], "1")
        self.assertEqual(cabeceras["Preference-Applied"], "respond-async")

    def test_el_pedido_va_a_la_caja_del_ticket_y_no_a_la_de_los_recolectores(self):
        _, _, contenido = self.alta_asincronica()
        pedido = self.cola.publicados[0]
        self.assertEqual(pedido["id"], contenido["id"])
        self.assertEqual(pedido["destinatario"], f"ticket:{contenido['id']}")
        self.assertNotEqual(pedido["destinatario"], balanceador.IDENTIDAD)
        # Nadie tiene una conexión abierta: puede esperar lo que dure una caída.
        self.assertEqual(pedido["presupuestoMs"], 60000)
        # El legajo se normaliza igual que por el camino sincrónico, y el alta
        # sigue sin reintentarse.
        self.assertEqual(pedido["parametros"], {"nombre": "Ada", "legajo": 1234})
        self.assertFalse(pedido["idempotente"])

    def test_pendiente_hasta_que_contesta_un_worker(self):
        _, _, contenido = self.alta_asincronica()
        codigo, cabeceras, pendiente = self.pedir("GET", f"/pedidos/{contenido['id']}")
        self.assertEqual(codigo, 202)
        self.assertEqual(pendiente["estado"], "pendiente")
        self.assertEqual(cabeceras["Retry-After"], "1")

        self.cola.atender(contenido={"persona": {"id": 7, "nombre": "Ada", "legajo": 1234}})
        codigo, _, listo = self.pedir("GET", f"/pedidos/{contenido['id']}")
        self.assertEqual(codigo, 201)                   # un alta es 201, igual que sincrónica
        self.assertEqual(listo["persona"]["id"], 7)

    def test_la_respuesta_se_entrega_una_sola_vez(self):
        """Retirarla la saca de la cola: el cliente tiene que quedarse con ella."""
        _, _, contenido = self.alta_asincronica()
        self.cola.atender(contenido={"persona": {"id": 7}})
        self.assertEqual(self.pedir("GET", f"/pedidos/{contenido['id']}")[0], 201)
        self.assertEqual(self.pedir("GET", f"/pedidos/{contenido['id']}")[0], 202)

    def test_una_lectura_exitosa_es_200(self):
        _, _, contenido = self.pedir("GET", "/", asincronico=True)
        self.cola.atender(contenido={"app": "python"})
        codigo, _, identidad = self.pedir("GET", f"/pedidos/{contenido['id']}")
        self.assertEqual(codigo, 200)
        self.assertEqual(identidad["app"], "python")

    def test_el_error_del_worker_se_traduce_igual_que_sincronico(self):
        _, _, contenido = self.alta_asincronica()
        self.cola.atender(estado="ALREADY_EXISTS", contenido={"error": "legajo repetido"})
        codigo, _, error = self.pedir("GET", f"/pedidos/{contenido['id']}")
        self.assertEqual(codigo, 409)
        self.assertEqual(error["error"], "legajo repetido")

    def test_un_ticket_mal_formado_es_404_sin_preguntarle_a_la_cola(self):
        codigo, _, _ = self.pedir("GET", "/pedidos/cualquier-cosa")
        self.assertEqual(codigo, 404)
        self.assertEqual(self.cola.retiros, 0)

    def test_un_ticket_vencido_es_404_sin_preguntarle_a_la_cola(self):
        """Pasado el presupuesto más el TTL de la cola ya no puede haber nada en
        la caja: se sabe por la hora que lleva el ticket, sin guardar nada."""
        viejo = f"{'a' * 32}.{int(time.time() - 121)}"
        codigo, _, error = self.pedir("GET", f"/pedidos/{viejo}")
        self.assertEqual(codigo, 404)
        self.assertEqual(error["error"], "ticket desconocido o vencido")
        self.assertEqual(self.cola.retiros, 0)

    def test_con_la_cola_caida_no_hay_ticket(self):
        self.cola.caida = True
        codigo, cabeceras, error = self.alta_asincronica()
        self.assertEqual(codigo, 503)
        self.assertIsNone(cabeceras.get("Location"))
        self.assertEqual(error["error"], "el sistema de colas no responde")

    def test_la_validacion_corre_antes_de_dar_ticket(self):
        codigo, _, _ = self.pedir("POST", "/personas", {"nombre": "Ada", "legajo": "x"},
                                  asincronico=True)
        self.assertEqual(codigo, 400)
        self.assertEqual(self.cola.publicados, [])


class PruebasPrefer(unittest.TestCase):
    """Qué cuenta como pedir respuesta asincrónica (RFC 7240)."""

    def asincronico(self, prefer):
        class Falso:
            headers = {"Prefer": prefer} if prefer is not None else {}
        return balanceador.Manejador.asincronico(Falso())

    def test_formas_validas(self):
        for prefer in ("respond-async", "RESPOND-ASYNC", "wait=5, respond-async",
                       "respond-async; foo=bar"):
            self.assertTrue(self.asincronico(prefer), prefer)

    def test_sin_el_header_sigue_siendo_sincronico(self):
        for prefer in (None, "", "wait=5", "respond-asyncx", "return=minimal"):
            self.assertFalse(self.asincronico(prefer), prefer)


if __name__ == "__main__":
    unittest.main()
