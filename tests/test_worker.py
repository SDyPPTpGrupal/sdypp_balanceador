"""Pruebas del Worker con un stub, un backend y un pool falsos.

Acá se prueba la regla de reintento, que es contrato: qué pasa con cada código
gRPC según la operación sea lectura o escritura.

    python -m unittest discover -s tests -v
"""

import os
import sys
import time
import unittest

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app"))

from cola import Cola, Pedido, Worker  # noqa: E402

OK = grpc.StatusCode.OK
UNAVAILABLE = grpc.StatusCode.UNAVAILABLE
DEADLINE = grpc.StatusCode.DEADLINE_EXCEEDED
INVALID = grpc.StatusCode.INVALID_ARGUMENT


class ErrorRpc(grpc.RpcError):
    """Lo que tira un stub de verdad cuando el RPC falla: tiene code() y details()."""

    def __init__(self, codigo, detalle="falló"):
        self._codigo, self._detalle = codigo, detalle

    def code(self):
        return self._codigo

    def details(self):
        return self._detalle


class BackendFalso:
    def __init__(self, destino="replica:1", sano=True):
        self.destino = destino
        self.sano = sano
        self.stub = object()
        self.soltados = []

    def soltar(self, worker):
        self.soltados.append(worker)


class PoolFalso:
    """Como el Pool real con BA_FALLOS_PARA_SACAR=1: un fallo saca la réplica."""

    def __init__(self, *backends):
        self._backends = set(backends)
        self.fallos = []

    def tiene(self, backend):
        return backend in self._backends

    def fallo(self, backend):
        self.fallos.append(backend)
        backend.sano = False

    def quitar(self, backend):
        self._backends.discard(backend)


def pedido(llamar, idempotente=True, presupuesto=5.0, cliente=None):
    return Pedido(operacion="GET /", llamar=llamar, idempotente=idempotente,
                  vence_en=time.monotonic() + presupuesto, cliente=cliente)


def responde(valor):
    return lambda stub, timeout, metadata: valor


def falla(codigo, detalle="falló"):
    def llamar(stub, timeout, metadata):
        raise ErrorRpc(codigo, detalle)
    return llamar


def armar(sano=True):
    backend = BackendFalso(sano=sano)
    pool = PoolFalso(backend)
    cola = Cola(cota=10)
    return backend, pool, cola, Worker(backend, cola, pool)


class PruebasAtender(unittest.TestCase):
    """atender() a mano, sin hilo: un intento, un resultado."""

    def test_ok_dispara_listo_con_la_respuesta(self):
        backend, pool, cola, worker = armar()
        p = pedido(responde("pong"))
        worker.atender(p)
        self.assertTrue(p.listo.is_set())
        self.assertEqual(p.resultado, (OK, "pong"))
        self.assertEqual(p.intentos, ["replica:1"])
        self.assertEqual(worker.atendidos, 1)
        self.assertEqual(pool.fallos, [])

    def test_llamar_recibe_el_stub_el_tiempo_que_queda_y_la_metadata(self):
        backend, pool, cola, worker = armar()
        visto = {}

        def llamar(stub, timeout, metadata):
            visto.update(stub=stub, timeout=timeout, metadata=dict(metadata))
            return "ok"

        p = pedido(llamar, presupuesto=3.0, cliente="100.64.0.9")
        worker.atender(p)
        self.assertIs(visto["stub"], backend.stub)
        self.assertGreater(visto["timeout"], 2.5)
        self.assertLessEqual(visto["timeout"], 3.0)
        self.assertEqual(visto["metadata"]["x-request-id"], p.request_id)
        self.assertEqual(visto["metadata"]["x-forwarded-for"], "100.64.0.9")

    def test_sin_cliente_no_manda_x_forwarded_for(self):
        backend, pool, cola, worker = armar()
        visto = {}
        worker.atender(pedido(lambda s, timeout, metadata: visto.update(metadata=dict(metadata))))
        self.assertNotIn("x-forwarded-for", visto["metadata"])

    def test_unavailable_devuelve_al_frente_y_avisa_al_pool(self):
        backend, pool, cola, worker = armar()
        p = pedido(falla(UNAVAILABLE))
        worker.atender(p)
        self.assertFalse(p.listo.is_set())            # no terminó: lo va a sacar otro
        self.assertEqual(cola.largo(), 1)
        self.assertIs(cola.get(lambda: True), p)
        self.assertEqual(pool.fallos, [backend])       # el mismo contador que usa el vigilante
        self.assertEqual(p.intentos, ["replica:1"])

    def test_unavailable_en_escritura_tambien_reintenta(self):
        backend, pool, cola, worker = armar()
        p = pedido(falla(UNAVAILABLE), idempotente=False)
        worker.atender(p)
        self.assertFalse(p.listo.is_set())
        self.assertEqual(cola.largo(), 1)

    def test_deadline_en_escritura_termina_sin_reintentar(self):
        backend, pool, cola, worker = armar()
        p = pedido(falla(DEADLINE, "tardó"), idempotente=False)
        worker.atender(p)
        self.assertTrue(p.listo.is_set())
        self.assertEqual(p.resultado, (DEADLINE, "tardó"))
        self.assertEqual(cola.largo(), 0)
        self.assertEqual(pool.fallos, [])              # tardar no es estar caída

    def test_deadline_en_lectura_reintenta_si_queda_tiempo(self):
        backend, pool, cola, worker = armar()
        p = pedido(falla(DEADLINE), idempotente=True, presupuesto=5.0)
        worker.atender(p)
        self.assertFalse(p.listo.is_set())
        self.assertEqual(cola.largo(), 1)
        self.assertEqual(pool.fallos, [])

    def test_deadline_en_lectura_sin_tiempo_termina(self):
        backend, pool, cola, worker = armar()

        def tarda_y_falla(stub, timeout, metadata):
            time.sleep(0.15)
            raise ErrorRpc(DEADLINE)

        p = pedido(tarda_y_falla, idempotente=True, presupuesto=0.1)
        worker.atender(p)
        self.assertTrue(p.listo.is_set())
        self.assertEqual(p.resultado[0], DEADLINE)
        self.assertEqual(cola.largo(), 0)

    def test_vencido_en_la_cola_no_hace_el_rpc(self):
        backend, pool, cola, worker = armar()
        llamadas = []
        p = pedido(lambda *a, **k: llamadas.append(1), presupuesto=-0.01)
        worker.atender(p)
        self.assertEqual(llamadas, [])
        self.assertEqual(p.resultado[0], DEADLINE)
        self.assertEqual(p.intentos, [])

    def test_otro_error_termina_con_ese_codigo(self):
        backend, pool, cola, worker = armar()
        p = pedido(falla(INVALID, "legajo inválido"))
        worker.atender(p)
        self.assertEqual(p.resultado, (INVALID, "legajo inválido"))
        self.assertEqual(cola.largo(), 0)
        self.assertEqual(pool.fallos, [])

    def test_una_excepcion_inesperada_no_deja_al_handler_esperando(self):
        backend, pool, cola, worker = armar()

        def rompe(stub, timeout, metadata):
            raise ValueError("bug")

        p = pedido(rompe)
        worker.atender(p)
        self.assertTrue(p.listo.is_set())
        self.assertEqual(p.resultado[0], grpc.StatusCode.INTERNAL)
        self.assertIn("ValueError", p.resultado[1])


class PruebasHilo(unittest.TestCase):
    """El worker corriendo de verdad como hilo."""

    def test_saca_de_la_cola_y_atiende(self):
        backend, pool, cola, worker = armar()
        worker.start()
        p = pedido(responde("pong"))
        cola.put(p)
        self.assertTrue(p.listo.wait(timeout=2))
        self.assertEqual(p.resultado, (OK, "pong"))
        time.sleep(0.05)
        self.assertEqual(worker.estado, "libre")
        pool.quitar(backend)
        worker.join(timeout=2)

    def test_duerme_si_la_replica_no_esta_sana_y_no_saca_nada(self):
        backend, pool, cola, worker = armar(sano=False)
        worker.start()
        p = pedido(responde("pong"))
        cola.put(p)
        time.sleep(0.3)
        self.assertFalse(p.listo.is_set())
        self.assertEqual(cola.largo(), 1)              # sigue ahí para otro
        self.assertEqual(worker.estado, "durmiendo")
        backend.sano = True                            # el vigilante la devolvió
        self.assertTrue(p.listo.wait(timeout=2))
        pool.quitar(backend)
        worker.join(timeout=2)

    def test_termina_cuando_lo_sacan_del_pool(self):
        backend, pool, cola, worker = armar()
        worker.start()
        time.sleep(0.05)
        pool.quitar(backend)                           # POST /admin/backends {"quitar": [...]}
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(worker.estado, "terminando")
        self.assertEqual(backend.soltados, [worker])   # avisó al irse

    def test_reasignacion_la_replica_caida_no_se_queda_con_el_pedido(self):
        caida = BackendFalso("caida:1")
        viva = BackendFalso("viva:1")
        pool = PoolFalso(caida, viva)
        cola = Cola(cota=10)
        w_caida = Worker(caida, cola, pool)
        w_viva = Worker(viva, cola, pool)

        def segun_replica(stub, timeout, metadata):
            if stub is caida.stub:
                raise ErrorRpc(UNAVAILABLE, "connection refused")
            return "pong"

        w_caida.start()
        p = pedido(segun_replica)
        cola.put(p)
        time.sleep(0.2)                                # la caída lo intentó y lo devolvió
        self.assertFalse(caida.sano)
        self.assertFalse(p.listo.is_set())
        w_viva.start()
        self.assertTrue(p.listo.wait(timeout=2))
        self.assertEqual(p.resultado, (OK, "pong"))
        self.assertEqual(p.intentos, ["caida:1", "viva:1"])
        self.assertEqual(pool.fallos, [caida])
        pool.quitar(caida)
        pool.quitar(viva)
        w_caida.join(timeout=2)
        w_viva.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
