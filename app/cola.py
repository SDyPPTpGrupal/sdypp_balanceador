"""La cola compartida del balanceador y los workers que la vacían.

El hilo que atiende una request HTTP no elige réplica: arma un Pedido, lo
encola y espera. Del otro lado hay un hilo worker por réplica que saca de la
cola y le hace el RPC a la suya. Un pedido nunca pertenece a un servidor: si la
réplica no lo atendió, el worker lo devuelve al frente y otro worker lo saca.

Vive en un módulo aparte para poder probarlo sin gRPC de verdad ni servidor
HTTP: un stub falso y un pool falso alcanzan.
"""

import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Callable

import grpc


@dataclass
class Pedido:
    """Una request HTTP esperando a que alguien le haga el RPC.

    `llamar(stub, timeout, metadata)` ya lleva el mensaje armado: el worker no
    sabe qué operación es, sólo a quién se la manda. `vence_en` es un solo
    presupuesto para todo el viaje, espera en cola incluida: un pedido que
    esperó 4 s en la cola tiene 1 s de RPC, no 5.
    """

    operacion: str              # "POST /personas", "GET /personas", ... como en la bitácora
    llamar: Callable            # llamar(stub, timeout, metadata) -> respuesta del RPC
    idempotente: bool           # False sólo para CrearPersona: repetirla duplica la persona
    vence_en: float             # time.monotonic() + presupuesto
    cliente: str | None = None  # IP de quien hizo la request; viaja como x-forwarded-for
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    listo: threading.Event = field(default_factory=threading.Event)
    resultado: tuple | None = None   # (grpc.StatusCode, respuesta o detalle del error)
    intentos: list = field(default_factory=list)  # destinos probados, en orden

    def queda(self):
        """Segundos que le quedan. Negativo si ya venció."""
        return self.vence_en - time.monotonic()


class Cola:
    """Cola en memoria, acotada, con devolución al frente.

    deque + Condition y no queue.Queue: Queue no tiene push-front, y devolver al
    frente es justamente lo que hace que un pedido reasignado no pierda su lugar
    detrás de los que llegaron después.

    Un solo Condition hace de candado y de campana: quien encola avisa, quien
    saca espera. Sin candado, dos workers podrían sacar el mismo pedido.
    """

    ESPERA = 0.5  # cada cuánto un worker bloqueado vuelve a preguntar si sigue vivo

    def __init__(self, cota):
        self.cota = cota
        self._pedidos = deque()
        self._hay = threading.Condition()

    def put(self, pedido):
        """Encola al final. Devuelve False si está llena.

        Nunca bloquea: el handler prefiere contestar 503 en el acto a tener al
        cliente esperando por un lugar en la cola además de por la respuesta.
        """
        with self._hay:
            if len(self._pedidos) >= self.cota:
                return False
            self._pedidos.append(pedido)
            self._hay.notify()
            return True

    def devolver_al_frente(self, pedido):
        """Lo pone primero. Ignora la cota: el pedido ya estaba adentro."""
        with self._hay:
            self._pedidos.appendleft(pedido)
            self._hay.notify()

    def get(self, sigo_vivo):
        """Saca el primero. Bloquea hasta que haya uno.

        Cada ESPERA segundos (o cuando lo despiertan) evalúa `sigo_vivo()`; si
        da False devuelve None sin sacar nada, para que el worker de una réplica
        que se cayó no se lleve un pedido que no va a poder atender.
        """
        with self._hay:
            while True:
                if not sigo_vivo():
                    # Si nos despertaron por un pedido que no vamos a sacar, le
                    # pasamos el aviso a otro en vez de dejarlo esperar ESPERA s.
                    if self._pedidos:
                        self._hay.notify()
                    return None
                if self._pedidos:
                    return self._pedidos.popleft()
                self._hay.wait(timeout=self.ESPERA)

    def largo(self):
        with self._hay:
            return len(self._pedidos)


class Worker(threading.Thread):
    """Un hilo atado a una réplica: saca de la cola y le habla sólo a ella.

    Es el modelo de consumidores que compiten: nadie reparte, cada worker saca
    cuando puede. Una réplica lenta saca menos; una caída no saca nada, porque
    su worker duerme mientras `backend.sano` sea False. Así el reparto sale
    solo de la velocidad de cada una, sin round-robin ni contador compartido.

    Necesita del backend: `destino`, `sano`, `stub`, `soltar(worker)`. Del pool:
    `tiene(backend)`, `fallo(backend)`. Nada más, así se prueba con falsos.
    """

    # Prioridad al resumir los N workers de una réplica en un solo estado para
    # /health: si uno está ocupado, la réplica está ocupada.
    ESTADOS = ("ocupado", "libre", "durmiendo", "terminando")

    def __init__(self, backend, cola, pool, numero=1):
        super().__init__(name=f"worker-{backend.destino}-{numero}", daemon=True)
        self.backend = backend
        self.cola = cola
        self.pool = pool
        self.estado = "durmiendo"
        self.atendidos = 0

    def run(self):
        b = self.backend
        try:
            while self.pool.tiene(b):
                if not b.sano:
                    self.estado = "durmiendo"
                    time.sleep(Cola.ESPERA)
                    continue
                self.estado = "libre"
                pedido = self.cola.get(lambda: b.sano and self.pool.tiene(b))
                if pedido is None:
                    continue
                self.estado = "ocupado"
                self.atender(pedido)
        finally:
            # Sacado del pool: terminamos lo que teníamos en vuelo y nos vamos.
            # El canal lo cierra el backend cuando se va el último de sus workers.
            self.estado = "terminando"
            b.soltar(self)

    def atender(self, pedido):
        """Un intento contra nuestra réplica. Termina el pedido o lo devuelve."""
        b = self.backend
        if pedido.queda() <= 0:
            # Venció esperando en la cola. No gastamos un RPC en algo que el
            # handler ya no va a leer.
            self.terminar(pedido, grpc.StatusCode.DEADLINE_EXCEEDED, "venció esperando en la cola")
            return

        pedido.intentos.append(b.destino)
        # El mismo id en cada intento: es lo que permite seguir un pedido
        # reasignado por las bitácoras de dos casas.
        metadata = [("x-request-id", pedido.request_id)]
        if pedido.cliente:
            metadata.append(("x-forwarded-for", pedido.cliente))
        try:
            respuesta = pedido.llamar(b.stub, timeout=pedido.queda(), metadata=metadata)
        except grpc.RpcError as e:
            codigo, detalle = e.code(), e.details()
        except Exception as e:  # un bug en `llamar` no puede dejar al handler esperando
            self.terminar(pedido, grpc.StatusCode.INTERNAL, f"{type(e).__name__}: {e}")
            return
        else:
            self.atendidos += 1
            self.terminar(pedido, grpc.StatusCode.OK, respuesta)
            return

        # La regla de reintento. Es contrato, no detalle: ver el README.
        if codigo == grpc.StatusCode.UNAVAILABLE:
            # La réplica ni miró el pedido: no hay nada hecho a medias.
            # Reintentar es gratis, y la marcamos caída sin esperar al vigilante.
            self.pool.fallo(b)
            self.cola.devolver_al_frente(pedido)
        elif codigo == grpc.StatusCode.DEADLINE_EXCEEDED and pedido.idempotente and pedido.queda() > 0:
            # Una lectura que tardó demasiado se puede repetir en otra. Una
            # escritura NO: "tardó demasiado" no dice si se ejecutó o no, y
            # repetirla puede crear la persona dos veces. Preferimos un 504
            # honesto a un duplicado silencioso.
            self.cola.devolver_al_frente(pedido)
        else:
            # INVALID_ARGUMENT, ALREADY_EXISTS, ... van a dar igual en cualquier
            # réplica: reintentar sería repetir el mismo error N veces.
            self.terminar(pedido, codigo, detalle)

    @staticmethod
    def terminar(pedido, codigo, respuesta=None):
        pedido.resultado = (codigo, respuesta)
        pedido.listo.set()
