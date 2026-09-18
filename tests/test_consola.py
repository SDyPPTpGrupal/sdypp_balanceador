"""Pruebas de la consola del balanceador.

Se prueba el archivo que termina siendo el `--env-file` y el `docker run` que
arma. Lo que más importa acá es que el plano de control no quede expuesto sin
querer: quien alcanza `/admin/backends` decide a dónde va todo el tráfico.

    python -m unittest discover -s tests -v
"""

import os
import shutil
import sys
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import consola  # noqa: E402


class PruebasConfig(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ba-consola-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.original = consola.CONFIG
        consola.CONFIG = os.path.join(self.tmp, "balanceador.env")
        self.addCleanup(setattr, consola, "CONFIG", self.original)

    def test_defaults_sin_archivo(self):
        cfg = consola.leer_config()
        self.assertEqual(cfg["BA_PUERTO"], "8080")
        self.assertEqual(cfg["BA_ADMIN_BIND"], "127.0.0.1")

    def test_el_pool_arranca_vacio(self):
        """Lo llena el CD al conmutar. Precargarlo haría que el balanceador diga
        que tiene réplicas que quizá ya no existen."""
        self.assertEqual(consola.leer_config()["BA_BACKENDS"], "")

    def test_guarda_todas_las_claves(self):
        consola.guardar_config({"BA_CASA": "casa-tomas"})
        guardado = consola.leer_config()
        for clave in consola.DEFAULTS:
            self.assertIn(clave, guardado)

    def test_el_archivo_queda_en_600(self):
        consola.guardar_config(dict(consola.DEFAULTS))
        self.assertEqual(os.stat(consola.CONFIG).st_mode & 0o777, 0o600)


class PruebasValidaciones(unittest.TestCase):

    def test_puertos(self):
        self.assertIsNone(consola.es_puerto("8080"))
        self.assertIsNone(consola.es_puerto("65535"))
        for malo in ("0", "65536", "-1", "ocho mil", ""):
            self.assertIsNotNone(consola.es_puerto(malo), malo)

    def test_numeros(self):
        self.assertIsNone(consola.es_numero("3"))
        self.assertIsNone(consola.es_numero("0.5"))
        self.assertIsNotNone(consola.es_numero("tres"))


class PruebasOrdenDocker(unittest.TestCase):

    def test_usa_env_file_y_monta_los_logs(self):
        orden = " ".join(consola.orden_docker())
        self.assertIn("--env-file", orden)
        self.assertIn("logs:/app/logs", orden)

    def test_network_host(self):
        """Para que el plano de control quede realmente en el loopback de la
        máquina y el balanceador alcance las réplicas del tailnet."""
        self.assertIn("--network host", " ".join(consola.orden_docker()))

    def test_no_pide_privilegios_de_mas(self):
        orden = " ".join(consola.orden_docker())
        self.assertNotIn("--privileged", orden)
        self.assertNotIn("docker.sock", orden)


class PruebasURLs(unittest.TestCase):

    def test_el_control_siempre_se_consulta_por_loopback(self):
        """Aunque BA_ADMIN_BIND sea una IP del tailnet: la consola corre en la
        misma máquina, y pegarle por la IP pública sería pasar por la red para
        hablar con uno mismo."""
        cfg = dict(consola.DEFAULTS, BA_ADMIN_BIND="100.101.15.93", BA_PUERTO_ADMIN="8081")
        self.assertEqual(consola.url_admin(cfg), "http://127.0.0.1:8081")


if __name__ == "__main__":
    unittest.main(verbosity=2)
