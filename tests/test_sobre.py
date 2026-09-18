"""Pruebas del sobre del contrato: toda respuesta al cliente tiene la misma forma.

El profesor lo pidió explícito: el contrato contra el cliente no puede cambiar de
forma según cómo salió la operación. Un cliente que tiene que mirar si vino la
clave `error` o si vinieron los campos al ras está adivinando el esquema en cada
request, y basta un camino de error que nadie probó para que se rompa.

Lo que se fija acá:

  * el plano público envuelve **siempre**: éxito, error de validación, 404, 503;
  * `Code` repite el código HTTP de la línea de estado — no se pueden separar;
  * `contenido` es siempre un objeto, nunca un string ni null;
  * el plano de control (/admin) **no** envuelve: lo consume el CD, no un cliente.

    python -m unittest discover -s tests -v
"""

import io
import json
import os
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "app"))  # balanceador, verificador
sys.path.insert(0, RAIZ)                       # consola.py, que vive en la raíz

from balanceador import Manejador, ManejadorAdmin, ManejadorPublico  # noqa: E402


def responder_con(clase, codigo, cuerpo):
    """Corre `clase.responder` sin abrir un socket.

    Se instancia con __new__ a propósito: el __init__ de BaseHTTPRequestHandler
    atiende la conexión entera, y acá lo único que se prueba es qué bytes deja
    en wfile. Los tres send_* se reemplazan por sondas.
    """
    h = clase.__new__(clase)
    h.wfile = io.BytesIO()
    estado = []
    cabeceras = {}
    h.send_response = lambda c, *a: estado.append(c)
    h.send_header = lambda k, v: cabeceras.__setitem__(k, v)
    h.end_headers = lambda: None

    h.responder(codigo, cuerpo)
    crudo = h.wfile.getvalue()
    return estado[0], cabeceras, json.loads(crudo), crudo


class PruebasSobrePublico(unittest.TestCase):
    """Todo lo que sale por el puerto público va envuelto."""

    def envolver(self, codigo, cuerpo):
        return responder_con(ManejadorPublico, codigo, cuerpo)

    def test_el_exito_va_envuelto(self):
        _, _, cuerpo, _ = self.envolver(200, {"app": "python", "version": 3})
        self.assertEqual(cuerpo, {"Code": 200, "contenido": {"app": "python", "version": 3}})

    def test_el_error_tiene_exactamente_la_misma_forma(self):
        """El punto de todo el ejercicio: mismas claves arriba, salga como salga."""
        _, _, ok, _ = self.envolver(200, {"app": "python"})
        _, _, mal, _ = self.envolver(404, {"error": "no existe"})
        self.assertEqual(sorted(ok), sorted(mal))
        self.assertEqual(sorted(ok), ["Code", "contenido"])

    def test_code_repite_el_codigo_http(self):
        for codigo in (200, 201, 400, 403, 404, 500, 503, 504):
            estado, _, cuerpo, _ = self.envolver(codigo, {"x": 1})
            self.assertEqual(estado, codigo, "la línea de estado cambió")
            self.assertEqual(cuerpo["Code"], codigo, "el sobre no coincide con el HTTP")

    def test_el_contenido_es_siempre_un_objeto(self):
        """Un `contenido` string o null obliga al cliente a chequear el tipo."""
        for cuerpo in ({}, {"error": "no existe"}, {"personas": []}):
            _, _, sobre, _ = self.envolver(200, cuerpo)
            self.assertIsInstance(sobre["contenido"], dict)

    def test_no_se_pierde_nada_del_contenido(self):
        payload = {"servidoPor": "python", "persona": {"id": 7, "legajo": 195157},
                   "anidado": {"lista": [1, 2, {"hondo": True}]}}
        _, _, sobre, _ = self.envolver(201, payload)
        self.assertEqual(sobre["contenido"], payload)

    def test_una_lista_tambien_entra_entera(self):
        """Hoy ningún handler devuelve una lista al ras, pero si alguien la
        devuelve tiene que quedar adentro del sobre, no reemplazarlo."""
        _, _, sobre, _ = self.envolver(200, [1, 2, 3])
        self.assertEqual(sobre, {"Code": 200, "contenido": [1, 2, 3]})

    def test_los_acentos_viajan_sin_escapar(self):
        _, cabeceras, _, crudo = self.envolver(503, {"error": "sin réplicas"})
        self.assertIn("réplicas".encode(), crudo)
        self.assertEqual(cabeceras["Content-Type"], "application/json; charset=utf-8")

    def test_content_length_cuenta_el_sobre(self):
        """Si midiera el cuerpo sin envolver, el cliente se cuelga esperando
        bytes que no llegan, o lee de más y rompe el keep-alive."""
        _, cabeceras, _, crudo = self.envolver(200, {"error": "ñandú"})
        self.assertEqual(int(cabeceras["Content-Length"]), len(crudo))


class PruebasPlanoDeControl(unittest.TestCase):
    """El /admin no envuelve: el CD y las consolas lo leen al ras."""

    def test_admin_no_envuelve(self):
        _, _, cuerpo, _ = responder_con(ManejadorAdmin, 200, {"backends": []})
        self.assertEqual(cuerpo, {"backends": []})

    def test_el_default_de_la_base_es_no_envolver(self):
        """Quien agregue un manejador nuevo tiene que optar por el sobre a mano."""
        self.assertFalse(Manejador.sobre)
        self.assertFalse(ManejadorAdmin.sobre)
        self.assertTrue(ManejadorPublico.sobre)


class PruebasDesenvolver(unittest.TestCase):
    """Los dos clientes propios (verificador y consola) abren el sobre igual."""

    def clientes(self):
        from consola import desenvolver as de_consola
        from verificador import desenvolver as de_verificador
        return (de_consola, de_verificador)

    def test_abren_el_sobre(self):
        for abrir in self.clientes():
            self.assertEqual(abrir({"Code": 200, "contenido": {"a": 1}}), {"a": 1})

    def test_dejan_pasar_lo_que_no_viene_envuelto(self):
        """Tolerancia a propósito: sirven contra un balanceador viejo o contra
        una réplica directo, sin que el script explote."""
        for abrir in self.clientes():
            self.assertEqual(abrir({"backends": []}), {"backends": []})
            self.assertIsNone(abrir(None))

    def test_no_confunden_un_contenido_que_se_llama_igual(self):
        """Hace falta que estén las dos claves; con una sola no es un sobre."""
        for abrir in self.clientes():
            self.assertEqual(abrir({"contenido": "algo"}), {"contenido": "algo"})
            self.assertEqual(abrir({"Code": 7}), {"Code": 7})


if __name__ == "__main__":
    unittest.main(verbosity=2)
