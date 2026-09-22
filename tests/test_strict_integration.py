import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import balanceador
from clientereplica import ClienteReplica, ErrorCola
import consola


class FakeQueueNodeHandler(BaseHTTPRequestHandler):
    """Handler de HTTP server para simular nodos del clúster de colas."""

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        node = self.server.node_state
        if self.path == "/health":
            body = {
                "vivo": True,
                "instancia": node.get("instancia", "cola-node"),
                "rol": node.get("rol", "slave"),
                "termino": node.get("termino", 1),
                "masterConocido": node.get("masterConocido"),
                "contrato": "1.0",
                "pedidos": node.get("pedidos", {"esperando": 0, "enVuelo": 0, "cota": 100}),
                "consumidores": node.get("consumidores", {}),
            }
            self._responder(200, body)
        elif self.path == "/estado":
            if node.get("rol") != "master":
                self._responder(421, {"error": "no-soy-master", "master": node.get("masterConocido")})
            else:
                body = {
                    "pedidos": node.get("pedidos", {"esperando": 0, "enVuelo": 0, "cota": 100}),
                    "consumidores": node.get("consumidores", {}),
                    "respuestas": {"pendientes": 0},
                }
                self._responder(200, body)
        else:
            self._responder(404, {"error": "no encontrado"})

    def do_POST(self):
        node = self.server.node_state
        length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            req_body = json.loads(raw_body.decode())
        except Exception:
            req_body = {}

        node["posts_recibidos"].append((self.path, req_body))

        if node.get("rol") != "master":
            self._responder(421, {"error": "no-soy-master", "master": node.get("masterConocido")})
            return

        if self.path == "/pedidos":
            self._responder(202, {"id": req_body.get("id", "p123"), "encolado": True})
        elif self.path == "/respuestas/tomar":
            if node.get("respuestas_pendientes"):
                resp = node["respuestas_pendientes"].pop(0)
                self._responder(200, resp)
            else:
                self._responder(204, {})
        else:
            self._responder(400, {"error": "ruta invalida"})

    def _responder(self, code, data):
        crudo = json.dumps(data).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(crudo)))
        self.end_headers()
        self.wfile.write(crudo)


class TestStrictIntegration(unittest.TestCase):
    """Pruebas estrictas de integración para Persona C (balanceador + clúster de cola)."""

    def setUp(self):
        self.servers = []
        self.ports = []
        self.nodes = []

        # Crear 3 nodos HTTP de mentira
        for i in range(3):
            server = ThreadingHTTPServer(("127.0.0.1", 0), FakeQueueNodeHandler)
            port = server.server_address[1]
            node_state = {
                "port": port,
                "url": f"http://127.0.0.1:{port}",
                "instancia": f"cola-{port}",
                "rol": "slave",
                "termino": 1,
                "masterConocido": None,
                "posts_recibidos": [],
                "respuestas_pendientes": [],
            }
            server.node_state = node_state
            t = threading.Thread(target=server.serve_forever, daemon=True)
            t.start()
            self.servers.append(server)
            self.ports.append(port)
            self.nodes.append(node_state)

        # Configurar 127.0.0.1:port[0] como master inicialmente
        self.nodes[0]["rol"] = "master"
        self.nodes[0]["termino"] = 5
        self.nodes[1]["masterConocido"] = self.nodes[0]["url"]
        self.nodes[2]["masterConocido"] = self.nodes[0]["url"]

        self.seed_urls = [n["url"] for n in self.nodes]

    def tearDown(self):
        for s in self.servers:
            s.shutdown()

    def test_descubrimiento_seed_list_y_failover_master(self):
        """Verifica que ClienteReplica encuentre al master en la lista de semillas."""
        cli = ClienteReplica(self.seed_urls, timeout=1.0)
        try:
            self.assertEqual(cli.master_conocido(), None)

            # Publicar pedido p1 debe descubrir al master (nodo 0) y enviar p1
            codigo, datos = cli.publicar_pedido({"id": "p1", "operacion": "GET /personas", "destinatario": "bal"})
            self.assertEqual(codigo, 202)
            self.assertEqual(cli.master_conocido(), self.nodes[0]["url"])

            # Ahora el master 0 cae y el nodo 1 pasa a ser master en término 6
            self.nodes[0]["rol"] = "slave"
            self.nodes[0]["masterConocido"] = self.nodes[1]["url"]
            self.nodes[1]["rol"] = "master"
            self.nodes[1]["termino"] = 6
            self.nodes[2]["masterConocido"] = self.nodes[1]["url"]

            # La siguiente petición p2 a nodo 0 recibe 421 y se redirige a nodo 1
            codigo, datos = cli.publicar_pedido({"id": "p2", "operacion": "POST /personas", "destinatario": "bal"})
            self.assertEqual(codigo, 202)
            self.assertEqual(cli.master_conocido(), self.nodes[1]["url"])

            # Verificar historial de peticiones
            posts_n0 = [p for p in self.nodes[0]["posts_recibidos"] if p[0] == "/pedidos"]
            posts_n1 = [p for p in self.nodes[1]["posts_recibidos"] if p[0] == "/pedidos"]
            self.assertEqual(len(posts_n0), 2)  # p1 exitoso + p2 que devolvió 421
            self.assertEqual(len(posts_n1), 1)  # p2 redirigido
            self.assertEqual(posts_n0[0][1]["id"], "p1")
            self.assertEqual(posts_n0[1][1]["id"], "p2")
            self.assertEqual(posts_n1[0][1]["id"], "p2")
        finally:
            cli.cerrar()

    def test_no_redirecciona_a_host_no_permitido(self):
        """Si un nodo malicioso intenta redirigir a un host fuera de la seed list, se ignora."""
        self.nodes[0]["rol"] = "slave"
        self.nodes[0]["masterConocido"] = "http://malicious-host.com:9999"

        cli = ClienteReplica(self.seed_urls, timeout=0.5, presupuesto=1.0)
        try:
            codigo, datos = cli.publicar_pedido({"id": "p_sec", "operacion": "GET /personas", "destinatario": "bal"})
            self.assertEqual(codigo, 503)
        finally:
            cli.cerrar()

    def test_previene_bucle_infinito_de_redirecciones_421(self):
        """Dos nodos que se redirigen mutuamente no causan un bucle infinito."""
        self.nodes[0]["rol"] = "slave"
        self.nodes[0]["masterConocido"] = self.nodes[1]["url"]
        self.nodes[1]["rol"] = "slave"
        self.nodes[1]["masterConocido"] = self.nodes[0]["url"]
        self.nodes[2]["rol"] = "slave"
        self.nodes[2]["masterConocido"] = None

        cli = ClienteReplica(self.seed_urls, timeout=0.5, presupuesto=1.0)
        try:
            t0 = time.monotonic()
            codigo, datos = cli.publicar_pedido({"id": "p_loop", "operacion": "GET /personas", "destinatario": "bal"})
            duracion = time.monotonic() - t0

            self.assertEqual(codigo, 503)
            self.assertLess(duracion, 2.5)
        finally:
            cli.cerrar()

    def test_salud_balanceador_con_cluster_en_diferentes_estados(self):
        """Verifica la respuesta de salud() en balanceador ante clúster sano, eligiendo y caído."""
        cli = ClienteReplica(self.seed_urls, timeout=0.5)
        try:
            with patch.object(balanceador, "CLIENTE", cli):
                class FakeHandler:
                    sobre = False
                    def responder(self, c, data):
                        self.code = c
                        self.data = data

                # Estado 1: Clúster Sano
                h = FakeHandler()
                balanceador.ManejadorPublico.salud(h)
                self.assertEqual(h.data["cola"]["estado"], "sana")
                nodos = h.data["cola"]["nodos"]
                self.assertEqual(len(nodos), 3)
                masters = [n for n in nodos if n["rol"] == "master"]
                self.assertEqual(len(masters), 1)
                self.assertEqual(masters[0]["termino"], 5)

                # Estado 2: Clúster Eligiendo (sin master)
                self.nodes[0]["rol"] = "slave"
                h = FakeHandler()
                balanceador.ManejadorPublico.salud(h)
                self.assertEqual(h.data["cola"]["estado"], "eligiendo")

                # Estado 3: Clúster Caído (todos apagados)
                for s in self.servers:
                    s.shutdown()
                h = FakeHandler()
                balanceador.ManejadorPublico.salud(h)
                self.assertEqual(h.data["cola"]["estado"], "caída")
        finally:
            cli.cerrar()

    def test_consola_generacion_lista_semillas(self):
        """Verifica que la consola configure correctamente la lista de semillas en BA_COLA_URL."""
        self.assertIn(",", consola.DEFAULTS["BA_COLA_URL"])
        self.assertEqual(len(consola.DEFAULTS["BA_COLA_URL"].split(",")), 3)


if __name__ == "__main__":
    unittest.main()
