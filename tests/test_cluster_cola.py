"""Integration tests for ClienteReplica against a fake queue cluster.

Exercises multi-node fake cluster behavior:
- Cold-start discovery against 3 fake nodes
- 421 redirect followed once without duplicate POST body delivery
- Bounded backoff when all nodes are slaves with no master, and 503 when cluster is down
- Route /pedidos/tomar is never sent to a node reporting rol: "slave"
"""

import http.server
import json
import os
import sys
import threading
import time
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "app"))

from clientereplica import ClienteReplica  # noqa: E402


class FakeClusterNode:
    """Minimal scripted HTTP server acting as a queue cluster node."""

    def __init__(self, rol="master", master_conocido=None):
        self.rol = rol
        self.master_conocido = master_conocido
        self.received_requests = []
        self.received_bodies = []
        self._lock = threading.Lock()

        node = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                node._handle(self, "GET")

            def do_POST(self):
                node._handle(self, "POST")

            def log_message(self, fmt, *args):
                pass

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_port
        self.url = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def _handle(self, req_handler, method):
        length = int(req_handler.headers.get("Content-Length", 0))
        raw_body = req_handler.rfile.read(length) if length > 0 else b""
        body = None
        if raw_body:
            try:
                body = json.loads(raw_body.decode("utf-8"))
            except Exception:
                body = raw_body

        with self._lock:
            self.received_requests.append((method, req_handler.path))
            if body is not None:
                self.received_bodies.append(body)

        path = req_handler.path
        if path == "/health":
            resp = {
                "cola": "sana",
                "rol": self.rol,
                "masterConocido": self.master_conocido,
            }
            self._send_json(req_handler, 200, resp)
        elif path == "/pedidos":
            if self.rol == "slave":
                self._send_json(
                    req_handler,
                    421,
                    {"error": "no-soy-master", "master": self.master_conocido},
                )
            else:
                pid = body.get("id", "p1") if isinstance(body, dict) else "p1"
                self._send_json(req_handler, 202, {"id": pid})
        elif path == "/respuestas/tomar" or path == "/pedidos/tomar":
            if self.rol == "slave":
                self._send_json(
                    req_handler,
                    421,
                    {"error": "no-soy-master", "master": self.master_conocido},
                )
            else:
                self._send_json(req_handler, 204, {})
        else:
            self._send_json(req_handler, 404, {"error": "not found"})

    def _send_json(self, req_handler, status, data):
        payload = json.dumps(data).encode("utf-8")
        req_handler.send_response(status)
        req_handler.send_header("Content-Type", "application/json; charset=utf-8")
        req_handler.send_header("Content-Length", str(len(payload)))
        req_handler.end_headers()
        req_handler.wfile.write(payload)

    def shutdown(self):
        try:
            self._server.shutdown()
            self._server.server_close()
        except Exception:
            pass
        self._thread.join(timeout=2.0)


class TestClusterCola(unittest.TestCase):

    def setUp(self):
        self.nodes = []

    def tearDown(self):
        for node in self.nodes:
            node.shutdown()

    def create_node(self, rol="master", master_conocido=None):
        node = FakeClusterNode(rol=rol, master_conocido=master_conocido)
        self.nodes.append(node)
        return node

    # 5.20 Fake-cluster cold-start discovery
    def test_cold_start_discovery_three_node_cluster(self):
        node2 = self.create_node(rol="master", master_conocido=None)
        node1 = self.create_node(rol="slave", master_conocido=node2.url)
        node3 = self.create_node(rol="slave", master_conocido=node2.url)

        client = ClienteReplica([node1.url, node2.url, node3.url], token="t")
        try:
            codigo, datos = client.publicar_pedido({"id": "p-init"})
            self.assertEqual(codigo, 202)
            self.assertEqual(client.master_conocido(), node2.url)
        finally:
            client.cerrar()

    # 5.21 421 followed once with POST body sent exactly once
    def test_421_redirect_never_duplicates_post_write(self):
        node2 = self.create_node(rol="master")
        # node1 pretends to be master in cache first, then steps down to slave
        node1 = self.create_node(rol="master")

        client = ClienteReplica([node1.url, node2.url], token="t")
        try:
            # Warm up cache on node1
            client.publicar_pedido({"id": "warmup"})
            self.assertEqual(client.master_conocido(), node1.url)

            # node1 steps down and points to node2
            node1.rol = "slave"
            node1.master_conocido = node2.url

            # Publish a critical non-idempotent write
            pedido = {"id": "pedido-no-duplicar", "operacion": "POST /transacciones"}
            codigo, datos = client.publicar_pedido(pedido)

            self.assertEqual(codigo, 202)
            self.assertEqual(client.master_conocido(), node2.url)

            # Node 1 received the POST once and answered 421 (never processed)
            node1_posts = [req for req in node1.received_requests if req == ("POST", "/pedidos")]
            self.assertEqual(len(node1_posts), 2)  # warmup + pedido-no-duplicar

            # Eventual master (node2) received the POST body EXACTLY ONCE
            node2_posts = [b for b in node2.received_bodies if isinstance(b, dict) and b.get("id") == "pedido-no-duplicar"]
            self.assertEqual(len(node2_posts), 1)
        finally:
            client.cerrar()

    # 5.22 Bounded backoff when all slaves, and 503 when cluster down
    def test_all_slaves_no_master_produces_bounded_backoff(self):
        node1 = self.create_node(rol="slave", master_conocido=None)
        node2 = self.create_node(rol="slave", master_conocido=None)
        node3 = self.create_node(rol="slave", master_conocido=None)

        client = ClienteReplica(
            [node1.url, node2.url, node3.url],
            token="t",
            backoff_inicial=0.05,
            backoff_maximo=0.1,
            presupuesto=0.3,
        )
        try:
            t0 = time.monotonic()
            codigo, datos = client.publicar_pedido({"id": "p-slaves"})
            dur = time.monotonic() - t0

            self.assertEqual(codigo, 503)
            self.assertGreaterEqual(dur, 0.25)
            # Total requests across all nodes should be bounded (not busy-spinning)
            total_reqs = (
                len(node1.received_requests)
                + len(node2.received_requests)
                + len(node3.received_requests)
            )
            self.assertLess(total_reqs, 30)
        finally:
            client.cerrar()

    def test_whole_cluster_down_surfaces_503(self):
        # Dead URLs on ports with nothing listening
        dead_urls = ["http://127.0.0.1:1", "http://127.0.0.1:2", "http://127.0.0.1:3"]
        client = ClienteReplica(
            dead_urls,
            token="t",
            backoff_inicial=0.05,
            backoff_maximo=0.1,
            presupuesto=0.25,
        )
        try:
            codigo, datos = client.publicar_pedido({"id": "dead"})
            self.assertEqual(codigo, 503)
            self.assertIn("no responde", datos.get("error", ""))
        finally:
            client.cerrar()

    # 5.23 /pedidos/tomar never sent to a node reporting rol: "slave"
    def test_pedidos_tomar_never_sent_to_known_slave(self):
        node1 = self.create_node(rol="slave", master_conocido=None)
        node2 = self.create_node(rol="master")

        client = ClienteReplica([node1.url, node2.url], token="t")
        try:
            # First operation discovers node1 is slave, node2 is master
            codigo, _ = client.tomar_respuesta("consumidor1", 0)
            self.assertEqual(client.master_conocido(), node2.url)

            # Verify node1 never received any request to /respuestas/tomar or /pedidos/tomar
            node1_data_reqs = [
                req for req in node1.received_requests
                if req[1] in ("/respuestas/tomar", "/pedidos/tomar", "/pedidos")
            ]
            self.assertEqual(len(node1_data_reqs), 0)
        finally:
            client.cerrar()


if __name__ == "__main__":
    unittest.main()
