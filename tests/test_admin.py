"""Pruebas del plano de control: cómo entra un backend al pool y con qué `app`.

Importa porque el CD conmuta con un POST incremental: lo que no se nombra en el
JSON tiene que quedar intacto. De eso depende que un deploy de Python no toque
las réplicas Java, y al revés.

    python -m unittest discover -s tests -v
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app"))

from balanceador import Pool, normalizar_destino  # noqa: E402
from cola import Cola  # noqa: E402


class PruebasNormalizar(unittest.TestCase):
    """La forma corta existe por compatibilidad; la larga es la que manda el CD."""

    def test_forma_corta_asume_python(self):
        self.assertEqual(normalizar_destino("10.0.0.1:8080"), ("10.0.0.1:8080", "python"))

    def test_forma_larga_respeta_el_app(self):
        self.assertEqual(normalizar_destino({"destino": "10.0.0.4:8111", "app": "java"}),
                         ("10.0.0.4:8111", "java"))

    def test_forma_larga_sin_app_cae_en_python(self):
        self.assertEqual(normalizar_destino({"destino": "10.0.0.1:8080"}),
                         ("10.0.0.1:8080", "python"))

    def test_app_vacio_cae_en_python(self):
        self.assertEqual(normalizar_destino({"destino": "10.0.0.1:8080", "app": ""}),
                         ("10.0.0.1:8080", "python"))

    def test_basura_se_descarta(self):
        self.assertEqual(normalizar_destino({}), (None, None))
        self.assertEqual(normalizar_destino({"app": "java"}), (None, None))
        self.assertEqual(normalizar_destino(None), (None, None))
        self.assertEqual(normalizar_destino(42), (None, None))


class PruebasPool(unittest.TestCase):

    def setUp(self):
        self.pool = Pool(Cola(10))

    def agregar(self, destino, app="python"):
        return self.pool.agregar(destino, app)

    def destinos(self):
        return sorted(b.destino for b in self.pool.todos())

    def test_el_app_llega_hasta_el_json_de_health(self):
        """Si el CD mandara strings pelados, las réplicas Java figurarían como
        Python acá: no rompe el ruteo, pero /health mentiría."""
        self.agregar("10.0.0.4:8111", "java")
        self.assertEqual(self.pool.todos()[0].como_json()["app"], "java")

    def test_no_se_agrega_dos_veces_el_mismo_destino(self):
        self.assertIsNotNone(self.agregar("10.0.0.1:8080"))
        self.assertIsNone(self.agregar("10.0.0.1:8080"))
        self.assertEqual(len(self.pool.todos()), 1)

    def test_quitar_devuelve_si_estaba(self):
        self.agregar("10.0.0.1:8080")
        self.assertTrue(self.pool.quitar("10.0.0.1:8080"))
        self.assertFalse(self.pool.quitar("10.0.0.1:8080"))

    def test_un_deploy_de_python_no_toca_las_java(self):
        """El caso real: dos casas Python y dos Java en el pool. El CD conmuta las
        Python y el POST ni menciona a las Java, que tienen que quedar igual —
        mismos objetos, con sus workers y sus contadores."""
        self.agregar("10.0.0.1:8090", "python")
        self.agregar("10.0.0.2:8080", "python")
        java_1 = self.agregar("10.0.0.4:8111", "java")
        java_2 = self.agregar("10.0.0.4:8112", "java")

        # Lo que manda el CD: primero agregar, después quitar, en el mismo POST.
        for destino in ("10.0.0.1:8091", "10.0.0.2:8081"):
            self.agregar(destino, "python")
        for destino in ("10.0.0.1:8090", "10.0.0.2:8080"):
            self.pool.quitar(destino)

        self.assertEqual(self.destinos(),
                         ["10.0.0.1:8091", "10.0.0.2:8081", "10.0.0.4:8111", "10.0.0.4:8112"])
        # Por identidad y no por destino: son los mismos backends, no unos nuevos.
        vivos = {b.destino: b for b in self.pool.todos()}
        self.assertIs(vivos["10.0.0.4:8111"], java_1)
        self.assertIs(vivos["10.0.0.4:8112"], java_2)
        self.assertEqual(vivos["10.0.0.4:8111"].como_json()["app"], "java")

    def test_reagregar_un_destino_crea_otro_backend(self):
        """`tiene()` compara por identidad justamente por esto: los workers del
        backend viejo tienen que irse aunque el destino vuelva a estar en el pool."""
        viejo = self.agregar("10.0.0.1:8080")
        self.pool.quitar("10.0.0.1:8080")
        nuevo = self.agregar("10.0.0.1:8080")
        self.assertIsNot(viejo, nuevo)
        self.assertFalse(self.pool.tiene(viejo))
        self.assertTrue(self.pool.tiene(nuevo))


if __name__ == "__main__":
    unittest.main(verbosity=2)
