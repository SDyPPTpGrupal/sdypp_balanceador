"""Pruebas de derivar(): el camino entero handler → cola → worker → handler,
con el Pool y los Backend reales pero sin ninguna réplica escuchando.

Necesita los stubs generados (app/contrato_pb2*.py), como el balanceador.

    python -m unittest discover -s tests -v
"""

import os
import sys
import tempfile
import threading
import time
import unittest

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app"))

import balanceador  # noqa: E402
from cola import Cola  # noqa: E402
from test_worker import ErrorRpc  # noqa: E402


class PruebasDerivar(unittest.TestCase):

    def setUp(self):
        # Cola y pool nuevos por test, y la bitácora a un directorio temporal.
        self._tmp = tempfile.TemporaryDirectory()
        balanceador.DIRECTORIO_LOGS = self._tmp.name
        balanceador.ARCHIVO_BITACORA = os.path.join(self._tmp.name, "bitacora.log")
        balanceador.WORKERS_POR_REPLICA = 1
        balanceador.FALLOS_PARA_SACAR = 1
        balanceador.PRESUPUESTO = 2.0
        balanceador.COLA = Cola(cota=10)
        balanceador.POOL = balanceador.Pool(balanceador.COLA)

    def tearDown(self):
        for b in balanceador.POOL.todos():
            balanceador.POOL.quitar(b.destino)
        self._tmp.cleanup()

    def replica(self, destino, sana=True):
        """Un Backend real apuntando a un puerto donde no hay nadie. Nunca se le
        hace un RPC de verdad: `llamar` decide qué contestar según el stub."""
        b = balanceador.POOL.agregar(destino)
        b.sano = sana
        return b

    def test_sin_replicas_contesta_503_en_el_acto(self):
        antes = time.monotonic()
        codigo, cuerpo, destino, detalle = balanceador.derivar("GET /", lambda *a, **k: "x")
        self.assertEqual(codigo, 503)
        self.assertLess(time.monotonic() - antes, 0.1)
        self.assertIsNone(destino)
        self.assertEqual(detalle, "pool vacío")

    def test_cola_llena_contesta_503_sin_esperar(self):
        balanceador.COLA = Cola(cota=0)
        self.replica("127.0.0.1:1", sana=False)
        antes = time.monotonic()
        codigo, cuerpo, destino, detalle = balanceador.derivar("GET /", lambda *a, **k: "x")
        self.assertEqual(codigo, 503)
        self.assertLess(time.monotonic() - antes, 0.1)
        self.assertEqual(cuerpo, {"error": "cola llena"})
        self.assertIn("cola llena (0/0)", detalle)
        self.assertIn("req=", detalle)

    def test_nadie_lo_atiende_contesta_504_al_vencer_el_presupuesto(self):
        balanceador.PRESUPUESTO = 0.3
        self.replica("127.0.0.1:1", sana=False)      # su worker duerme: no saca nada
        antes = time.monotonic()
        codigo, cuerpo, destino, detalle = balanceador.derivar("GET /", lambda *a, **k: "x")
        tardo = time.monotonic() - antes
        self.assertEqual(codigo, 504)
        self.assertGreaterEqual(tardo, 0.3)
        self.assertLess(tardo, 0.6)
        self.assertIsNone(destino)                     # nadie lo intentó
        self.assertTrue(detalle.startswith("venció "))
        self.assertIn("req=", detalle)

    def test_ok_devuelve_la_respuesta_y_quien_la_dio(self):
        self.replica("127.0.0.1:1")
        codigo, r, destino, detalle = balanceador.derivar(
            "POST /echo", lambda stub, timeout, metadata: "pong")
        self.assertIsNone(codigo)
        self.assertEqual(r, "pong")
        self.assertEqual(destino, "127.0.0.1:1")
        self.assertRegex(detalle, r"^req=[0-9a-f]{32}$")   # un solo intento: sin `intentos=`

    def test_error_grpc_se_traduce_a_http(self):
        self.replica("127.0.0.1:1")

        def llamar(stub, timeout, metadata):
            raise ErrorRpc(grpc.StatusCode.ALREADY_EXISTS, "legajo repetido")

        codigo, cuerpo, destino, detalle = balanceador.derivar("POST /personas", llamar,
                                                               idempotente=False)
        self.assertEqual(codigo, 409)
        self.assertEqual(cuerpo, {"error": "legajo repetido"})
        self.assertEqual(destino, "127.0.0.1:1")
        self.assertTrue(detalle.startswith("ALREADY_EXISTS req="))

    def test_reasignacion_deja_el_camino_en_el_detalle(self):
        caida = self.replica("127.0.0.1:1")           # sana según el pool, pero no contesta
        viva = self.replica("127.0.0.1:2", sana=False)

        def llamar(stub, timeout, metadata):
            if stub is caida.stub:
                raise ErrorRpc(grpc.StatusCode.UNAVAILABLE, "connection refused")
            return "pong"

        def revivir_la_otra():
            # Cuando la caída ya falló y salió de rotación, entra la otra.
            while caida.sano:
                time.sleep(0.02)
            viva.sano = True

        threading.Thread(target=revivir_la_otra, daemon=True).start()
        codigo, r, destino, detalle = balanceador.derivar("GET /personas", llamar)
        self.assertIsNone(codigo)
        self.assertEqual(r, "pong")
        self.assertEqual(destino, "127.0.0.1:2")
        self.assertIn("intentos=127.0.0.1:1→127.0.0.1:2", detalle)
        self.assertFalse(caida.sano)                   # la sacó el worker, no el vigilante
        with open(balanceador.ARCHIVO_BITACORA, encoding="utf-8") as f:
            self.assertIn("sale de rotación (falló una request)", f.read())


if __name__ == "__main__":
    unittest.main()
