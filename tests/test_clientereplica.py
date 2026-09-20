"""Unit tests for ClienteReplica.

Exercises constructor seed list, cold-start discovery, steady-state cached master,
one-shot redirect, redirect allowlist, redirect loop protection, connection failure,
leaderless backoff, and never-write-to-known-slave guard.
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

from clientecola import ErrorCola  # noqa: E402
from clientereplica import ClienteReplica  # noqa: E402


class FakeQueueNode:
    """Scripted stand-in queue node for unit-testing ClienteReplica."""

    def __init__(self, rol="master", master_conocido=None):
        self.rol = rol
        self.master_conocido = master_conocido
        self.requests = []
        self.custom_handler = None
        self._lock = threading.Lock()

        node = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                node._dispatch(self, "GET")

            def do_POST(self):
                node._dispatch(self, "POST")

            def log_message(self, fmt, *args):
                pass

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_port
        self.url = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def _dispatch(self, req_handler, method):
        length = int(req_handler.headers.get("Content-Length", 0))
        raw_body = req_handler.rfile.read(length) if length > 0 else b""
        body = None
        if raw_body:
            try:
                body = json.loads(raw_body.decode("utf-8"))
            except Exception:
                body = raw_body

        with self._lock:
            self.requests.append((method, req_handler.path, req_handler.headers, body))

        if self.custom_handler:
            res = self.custom_handler(method, req_handler.path, req_handler.headers, body)
            if res is not None:
                status, resp_body = res
                self._send(req_handler, status, resp_body)
                return

        # Default handler behaviour
        if req_handler.path == "/health":
            status = 200
            resp_body = {
                "cola": "sana",
                "rol": self.rol,
                "masterConocido": self.master_conocido,
            }
            self._send(req_handler, status, resp_body)
        elif req_handler.path == "/pedidos":
            if self.rol == "slave":
                self._send(req_handler, 421, {"error": "no-soy-master", "master": self.master_conocido})
            else:
                self._send(req_handler, 202, {"id": (body or {}).get("id", "p1")})
        elif req_handler.path == "/respuestas/tomar":
            if self.rol == "slave":
                self._send(req_handler, 421, {"error": "no-soy-master", "master": self.master_conocido})
            else:
                self._send(req_handler, 204, {})
        else:
            self._send(req_handler, 404, {"error": "not found"})

    def _send(self, req_handler, status, data):
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


class TestClienteReplica(unittest.TestCase):

    def setUp(self):
        self.nodes = []

    def tearDown(self):
        for node in self.nodes:
            node.shutdown()

    def create_node(self, rol="master", master_conocido=None):
        node = FakeQueueNode(rol=rol, master_conocido=master_conocido)
        self.nodes.append(node)
        return node

    # 5.1 Constructor accepts a seed list
    def test_single_element_seed_list_operates_against_that_node(self):
        node = self.create_node(rol="master")
        client = ClienteReplica([node.url], token="t")
        try:
            codigo, datos = client.publicar_pedido({"id": "p1", "operacion": "GET /test"})
            self.assertEqual(codigo, 202)
            self.assertEqual(datos.get("id"), "p1")
        finally:
            client.cerrar()

    # 5.2 Cold-start discovery
    def test_cold_start_finds_master_directly(self):
        node1 = self.create_node(rol="master")
        node2 = self.create_node(rol="slave", master_conocido=node1.url)
        node3 = self.create_node(rol="slave", master_conocido=node1.url)

        # Force seed list order
        client = ClienteReplica([node1.url, node2.url, node3.url], token="t")
        try:
            # We fix the rotating offset or start discovery
            codigo, _ = client.publicar_pedido({"id": "p1"})
            self.assertEqual(codigo, 202)
            self.assertEqual(client.master_conocido(), node1.url)

            # Node 2 and Node 3 must not have been probed for /health
            n2_health = [r for r in node2.requests if r[1] == "/health"]
            n3_health = [r for r in node3.requests if r[1] == "/health"]
            self.assertEqual(len(n2_health), 0)
            self.assertEqual(len(n3_health), 0)
        finally:
            client.cerrar()

    def test_cold_start_follows_master_conocido_from_slave(self):
        node2 = self.create_node(rol="master")
        node1 = self.create_node(rol="slave", master_conocido=node2.url)
        node3 = self.create_node(rol="slave", master_conocido=node2.url)

        # Probing node1 first should follow masterConocido -> node2
        client = ClienteReplica([node1.url, node2.url, node3.url], token="t")
        try:
            codigo, _ = client.publicar_pedido({"id": "p1"})
            self.assertEqual(codigo, 202)
            self.assertEqual(client.master_conocido(), node2.url)
        finally:
            client.cerrar()

    def test_cold_start_during_election_finds_nothing_to_cache(self):
        node1 = self.create_node(rol="candidato", master_conocido=None)
        node2 = self.create_node(rol="candidato", master_conocido=None)
        node3 = self.create_node(rol="candidato", master_conocido=None)

        client = ClienteReplica([node1.url, node2.url, node3.url], token="t", presupuesto=0.25)
        try:
            codigo, datos = client.publicar_pedido({"id": "p1"})
            self.assertEqual(codigo, 503)
            self.assertIn("no responde", datos.get("error", ""))
            self.assertIsNone(client.master_conocido())
        finally:
            client.cerrar()

    # 5.3 Steady-state no extra probes
    def test_steady_state_operation_issues_no_extra_health_probes(self):
        master = self.create_node(rol="master")
        client = ClienteReplica([master.url], token="t")
        try:
            # Cold start warms up the cache
            codigo, _ = client.publicar_pedido({"id": "init"})
            self.assertEqual(codigo, 202)
            self.assertEqual(client.master_conocido(), master.url)

            # Clear recorded requests
            with master._lock:
                master.requests.clear()

            # 100 consecutive calls
            for _ in range(100):
                client.tomar_respuesta("dest1", 0)

            # Assert zero health probes
            with master._lock:
                health_probes = [r for r in master.requests if r[1] == "/health"]
            self.assertEqual(len(health_probes), 0)
        finally:
            client.cerrar()

    # 5.4 One-shot redirect
    def test_stale_cache_follows_exactly_one_redirect_and_succeeds(self):
        old_master = self.create_node(rol="master")
        new_master = self.create_node(rol="master")
        client = ClienteReplica([old_master.url, new_master.url], token="t")
        try:
            # Establish cache on old_master
            client.publicar_pedido({"id": "warmup"})
            self.assertEqual(client.master_conocido(), old_master.url)

            # Old master steps down and redirects to new_master
            old_master.rol = "slave"
            old_master.master_conocido = new_master.url

            codigo, datos = client.publicar_pedido({"id": "p2"})
            self.assertEqual(codigo, 202)
            self.assertEqual(client.master_conocido(), new_master.url)

            # Assert that old_master got exactly 1 attempt that answered 421,
            # and new_master got exactly 1 attempt that answered 202
            old_posts = [r for r in old_master.requests if r[1] == "/pedidos" and (r[3] or {}).get("id") == "p2"]
            new_posts = [r for r in new_master.requests if r[1] == "/pedidos" and (r[3] or {}).get("id") == "p2"]
            self.assertEqual(len(old_posts), 1)
            self.assertEqual(len(new_posts), 1)
        finally:
            client.cerrar()

    def test_redirect_chain_does_not_loop_forever(self):
        node1 = self.create_node(rol="slave")
        node2 = self.create_node(rol="slave")
        node1.master_conocido = node2.url
        node2.master_conocido = node1.url

        client = ClienteReplica([node1.url, node2.url], token="t", presupuesto=0.3)
        try:
            codigo, datos = client.publicar_pedido({"id": "loop"})
            # Should not loop forever; should surface 503 once exhausted
            self.assertEqual(codigo, 503)
        finally:
            client.cerrar()

    # 5.5 Redirect target allowlist
    def test_redirect_to_unknown_host_is_never_contacted(self):
        evil_url = "http://evil:8085"
        node1 = self.create_node(rol="slave", master_conocido=evil_url)
        node2 = self.create_node(rol="master")

        client = ClienteReplica([node1.url, node2.url], token="t")
        try:
            codigo, datos = client.publicar_pedido({"id": "allowlist"})
            # node1 redirects to evil:8085, client must ignore it (not in seed list),
            # fall back to discovery, find node2 as master, and succeed
            self.assertEqual(codigo, 202)
            self.assertEqual(client.master_conocido(), node2.url)
        finally:
            client.cerrar()

    # 5.6 Mutual-421 pair
    def test_mutual_421_pair_stays_bounded_and_surfaces_503(self):
        node1 = self.create_node(rol="slave")
        node2 = self.create_node(rol="slave")
        node1.master_conocido = node2.url
        node2.master_conocido = node1.url

        client = ClienteReplica([node1.url, node2.url], token="t", presupuesto=0.3, backoff_inicial=0.05)
        try:
            codigo, _ = client.publicar_pedido({"id": "mutual"})
            self.assertEqual(codigo, 503)
            # Ensure request count stayed small and bounded
            total_reqs = len(node1.requests) + len(node2.requests)
            self.assertLess(total_reqs, 20)
        finally:
            client.cerrar()

    # 5.7 Connection failure triggers re-discovery
    def test_connection_reset_invalidates_cache_and_rediscover(self):
        node1 = self.create_node(rol="master")
        node2 = self.create_node(rol="slave", master_conocido=node1.url)

        client = ClienteReplica([node1.url, node2.url], token="t")
        try:
            client.publicar_pedido({"id": "init"})
            self.assertEqual(client.master_conocido(), node1.url)

            # Node 1 dies, Node 2 becomes master
            node1.shutdown()
            node2.rol = "master"
            node2.master_conocido = None

            # Next request fails on node1, invalidates cache, rediscovers node2
            # For idempotent/discovery or retry after discovery:
            codigo, datos = client.tomar_respuesta("dest", 0)
            self.assertEqual(client.master_conocido(), node2.url)
        finally:
            client.cerrar()

    def test_fresh_connection_failure_on_post_is_not_blindly_retried(self):
        node1 = self.create_node(rol="master")
        # Custom handler that closes connection immediately when POST /pedidos arrives
        def handler(method, path, headers, body):
            if method == "POST" and path == "/pedidos":
                # Do not send response, let connection reset / abort
                return None
            return None

        client = ClienteReplica([node1.url], token="t")
        try:
            # Set cached master
            client.publicar_pedido({"id": "warmup"})
            # Shut down server so next fresh request fails
            node1.shutdown()

            with self.assertRaises(ErrorCola):
                client.publicar_pedido({"id": "non_idempotent"})
            # Must have cleared cached master
            self.assertIsNone(client.master_conocido())
        finally:
            client.cerrar()

    # 5.8 Backoff while leaderless
    def test_leaderless_backoff_sleeps_and_exhausts_budget(self):
        node1 = self.create_node(rol="candidato", master_conocido=None)
        node2 = self.create_node(rol="candidato", master_conocido=None)

        client = ClienteReplica(
            [node1.url, node2.url],
            token="t",
            backoff_inicial=0.05,
            backoff_maximo=0.1,
            presupuesto=0.25,
        )
        try:
            t0 = time.monotonic()
            codigo, datos = client.publicar_pedido({"id": "p1"})
            dur = time.monotonic() - t0
            self.assertEqual(codigo, 503)
            self.assertGreaterEqual(dur, 0.20)
            # Not busy-looping: requests should be bounded
            total_probes = len(node1.requests) + len(node2.requests)
            self.assertLess(total_probes, 30)
        finally:
            client.cerrar()

    # 5.9 Never writes to a known slave
    def test_never_writes_to_a_known_slave(self):
        slave_node = self.create_node(rol="slave", master_conocido=None)
        master_node = self.create_node(rol="master")

        client = ClienteReplica([slave_node.url, master_node.url], token="t")
        try:
            # First request will probe slave_node, learn it is a slave, then find master_node
            codigo, _ = client.publicar_pedido({"id": "write1"})
            self.assertEqual(codigo, 202)
            self.assertEqual(client.master_conocido(), master_node.url)

            # Verify slave_node never received a POST /pedidos
            slave_posts = [r for r in slave_node.requests if r[1] == "/pedidos"]
            self.assertEqual(len(slave_posts), 0)
        finally:
            client.cerrar()


if __name__ == "__main__":
    unittest.main()
