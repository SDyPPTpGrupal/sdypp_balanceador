import unittest
from unittest.mock import MagicMock, patch

import balanceador
from clientereplica import ClienteReplica, ErrorCola


class TestBalanceadorCutover(unittest.TestCase):

    def test_cliente_replica_instanciado(self):
        """Verifica que el cliente del balanceador sea una instancia de ClienteReplica."""
        self.assertIsInstance(balanceador.CLIENTE, ClienteReplica)

    def test_ba_cola_url_lista_semilla(self):
        """Verifica que BA_COLA_URL acepte múltiples URLs separadas por coma."""
        urls_raw = "http://127.0.0.1:8085, http://127.0.0.1:8086 ,http://127.0.0.1:8087"
        urls_parseadas = [u.strip() for u in urls_raw.split(",") if u.strip()]
        cli = ClienteReplica(urls_parseadas)
        self.assertEqual(len(cli._urls), 3)
        self.assertIn("http://127.0.0.1:8085", cli._urls)
        self.assertIn("http://127.0.0.1:8086", cli._urls)
        self.assertIn("http://127.0.0.1:8087", cli._urls)

    @patch.object(balanceador.CLIENTE, "publicar_pedido")
    def test_derivar_sin_master(self, mock_publicar):
        """derivar() debe responder 503 si el clúster está sin líder."""
        mock_publicar.return_value = (503, {"error": "el sistema de colas no responde"})
        codigo, contenido, atendido, detalle = balanceador.derivar("GET /personas")
        self.assertEqual(codigo, 503)
        self.assertEqual(contenido, {"error": "el sistema de colas no responde"})

    @patch.object(balanceador.CLIENTE, "tomar_respuesta")
    @patch("time.sleep")
    def test_recolectar_status_handling(self, mock_sleep, mock_tomar):
        """recolectar() no duerme en 204, pero duerme en 503/421 o ErrorCola."""
        mock_tomar.side_effect = [
            (204, {}),                                    # 204 sin sleep
            (503, {"error": "sin master"}),               # 503 con sleep
            ErrorCola("desconexion"),                     # ErrorCola con sleep
        ]
        iter_count = 0

        def sigo_vivo():
            nonlocal iter_count
            iter_count += 1
            return iter_count <= 3

        balanceador.recolectar(sigo_vivo=sigo_vivo)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch.object(balanceador.CLIENTE, "estado")
    @patch.object(balanceador.CLIENTE, "instancias")
    @patch.object(balanceador.CLIENTE, "master_conocido")
    def test_salud_campos_aditivos_cluster(self, mock_master, mock_instancias, mock_estado):
        """salud() dice quién consume y cómo está cada nodo, sin repetir nada."""
        mock_master.return_value = "http://127.0.0.1:8085"
        mock_instancias.return_value = [
            {"url": "http://127.0.0.1:8085", "instancia": "cola-8085", "rol": "master", "termino": 2},
            {"url": "http://127.0.0.1:8086", "instancia": "cola-8086", "rol": "slave", "termino": 2},
            {"url": "http://127.0.0.1:8087", "instancia": "", "rol": "caido", "termino": 0},
        ]
        mock_estado.return_value = {
            "pedidos": {"esperando": 0, "enVuelo": 0, "cota": 100},
            "consumidores": {"10.0.0.1:8080": {"ultimoPedidoHaceMs": 1000},
                             "10.0.0.2:8080": {"ultimoPedidoHaceMs": 999_000}},
            "respuestas": {"pendientes": 0},
        }

        # Simular llamada a salud
        class FakeHandler:
            sobre = False
            def responder(self_h, codigo, cuerpo):
                handler.resp_codigo = codigo
                handler.resp_cuerpo = cuerpo

        handler = FakeHandler()
        balanceador.ManejadorPublico.salud(handler)

        cuerpo = handler.resp_cuerpo
        self.assertEqual(handler.resp_codigo, 200)
        # Sólo la que pidió trabajo hace poco: la de hace 999 s ya no cuenta.
        self.assertEqual(cuerpo["replicas"], ["10.0.0.1:8080"])
        cola_info = cuerpo["cola"]
        self.assertEqual(cola_info.get("estado"), "sana")
        self.assertEqual(cola_info.get("cota"), 100)
        nodos = cola_info.get("nodos")
        self.assertEqual([n["rol"] for n in nodos], ["master", "slave", "caido"])
        self.assertEqual(nodos[0]["termino"], 2)
        # De un nodo caído no se inventan nombre ni término.
        self.assertEqual(nodos[2], {"url": "http://127.0.0.1:8087", "rol": "caido"})
        # Nada repetido ni del registro del CD en lo que ve el cliente.
        self.assertEqual(set(cuerpo), {"balanceador", "casa", "replicas", "cola"})
        self.assertEqual(set(cola_info), {"estado", "encolados", "enVuelo", "cota", "nodos"})


if __name__ == "__main__":
    unittest.main()
