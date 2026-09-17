"""Pruebas de Pedido y Cola: sin gRPC, sin red, sin balanceador.

    python -m unittest discover -s tests -v
"""

import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app"))

from cola import Cola, Pedido  # noqa: E402


def pedido(nombre="GET /", presupuesto=5.0):
    return Pedido(operacion=nombre, llamar=lambda *a, **k: None, idempotente=True,
                  vence_en=time.monotonic() + presupuesto)


class PruebasPedido(unittest.TestCase):

    def test_queda_es_el_tiempo_hasta_vencer(self):
        p = pedido(presupuesto=1.0)
        self.assertGreater(p.queda(), 0.9)
        self.assertLessEqual(p.queda(), 1.0)

    def test_queda_es_negativo_si_ya_vencio(self):
        self.assertLess(pedido(presupuesto=-1.0).queda(), 0)

    def test_cada_pedido_tiene_su_request_id(self):
        a, b = pedido(), pedido()
        self.assertEqual(len(a.request_id), 32)
        self.assertNotEqual(a.request_id, b.request_id)
        self.assertFalse(a.listo.is_set())
        self.assertEqual(a.intentos, [])


class PruebasCola(unittest.TestCase):

    def test_put_respeta_la_cota_y_no_bloquea(self):
        cola = Cola(cota=2)
        self.assertTrue(cola.put(pedido("a")))
        self.assertTrue(cola.put(pedido("b")))
        antes = time.monotonic()
        self.assertFalse(cola.put(pedido("c")))
        self.assertLess(time.monotonic() - antes, 0.1)
        self.assertEqual(cola.largo(), 2)

    def test_get_saca_en_orden_de_llegada(self):
        cola = Cola(cota=10)
        cola.put(pedido("a"))
        cola.put(pedido("b"))
        self.assertEqual(cola.get(lambda: True).operacion, "a")
        self.assertEqual(cola.get(lambda: True).operacion, "b")
        self.assertEqual(cola.largo(), 0)

    def test_devolver_al_frente_lo_deja_primero(self):
        cola = Cola(cota=10)
        cola.put(pedido("a"))
        cola.put(pedido("b"))
        a = cola.get(lambda: True)
        cola.devolver_al_frente(a)
        self.assertEqual(cola.get(lambda: True).operacion, "a")
        self.assertEqual(cola.get(lambda: True).operacion, "b")

    def test_devolver_al_frente_ignora_la_cota(self):
        cola = Cola(cota=1)
        cola.put(pedido("a"))
        cola.devolver_al_frente(pedido("reasignado"))
        self.assertEqual(cola.largo(), 2)
        self.assertEqual(cola.get(lambda: True).operacion, "reasignado")

    def test_get_bloquea_hasta_que_haya_un_pedido(self):
        cola = Cola(cota=10)
        sacado = []

        def worker():
            sacado.append(cola.get(lambda: True))

        hilo = threading.Thread(target=worker, daemon=True)
        hilo.start()
        time.sleep(0.2)
        self.assertEqual(sacado, [])          # sigue bloqueado: no hay nada
        cola.put(pedido("a"))
        hilo.join(timeout=2)
        self.assertFalse(hilo.is_alive())
        self.assertEqual(sacado[0].operacion, "a")

    def test_get_sale_con_none_cuando_deja_de_estar_vivo(self):
        cola = Cola(cota=10)
        vivo = threading.Event()
        vivo.set()
        resultado = []

        def worker():
            resultado.append(cola.get(vivo.is_set))

        hilo = threading.Thread(target=worker, daemon=True)
        hilo.start()
        time.sleep(0.2)
        vivo.clear()                          # la réplica se cayó
        hilo.join(timeout=2)
        self.assertFalse(hilo.is_alive())     # salió en menos de un ciclo de ESPERA
        self.assertEqual(resultado, [None])

    def test_get_no_saca_nada_si_ya_no_esta_vivo(self):
        cola = Cola(cota=10)
        cola.put(pedido("a"))
        self.assertIsNone(cola.get(lambda: False))
        self.assertEqual(cola.largo(), 1)     # el pedido queda para otro worker

    def test_un_pedido_lo_saca_un_solo_worker(self):
        cola = Cola(cota=1000)
        n = 200
        for i in range(n):
            cola.put(pedido(str(i)))
        sacados = []
        candado = threading.Lock()

        def worker():
            while True:
                p = cola.get(lambda: cola.largo() > 0)
                if p is None:
                    return
                with candado:
                    sacados.append(p.operacion)

        hilos = [threading.Thread(target=worker, daemon=True) for _ in range(8)]
        for h in hilos:
            h.start()
        for h in hilos:
            h.join(timeout=5)
        self.assertEqual(sorted(sacados, key=int), [str(i) for i in range(n)])


if __name__ == "__main__":
    unittest.main()
