# Imagen del balanceador. Multi-etapa: pip y sus wheels se quedan en la etapa
# builder y a la final sólo pasa lo instalado.
#
# Ya no se generan los stubs de contrato.proto: desde que los workers viven en
# las réplicas, el balanceador no le hace ningún RPC de negocio a nadie. Lo
# único que le queda de gRPC es `grpc.health.v1.Health` para preguntar si una
# réplica sigue viva, y eso viene en grpcio-health-checking. `contrato.proto`
# queda en el repo porque sigue siendo el contrato que implementan las réplicas,
# pero este contenedor no lo necesita.

FROM python:3.13-slim AS builder
WORKDIR /build
COPY requirements.txt ./
RUN pip install --no-cache-dir --prefix=/instalado -r requirements.txt


FROM python:3.13-slim
WORKDIR /app

# Usuario sin privilegios: el balanceador ve todo el tráfico del servicio, es el
# último proceso al que le querés dar root.
#
# El uid es 1000 y no uno alto: la bitácora se escribe en un bind mount del disco
# del host, y el uid de adentro tiene que coincidir con el del dueño de ese
# directorio afuera o el proceso no puede escribir. Con un uid distinto el
# servicio arranca igual y sólo se pierde el archivo —la línea sigue saliendo por
# stdout—, así que el problema no se nota hasta que hace falta la bitácora para
# la auditoría cruzada. Es el mismo criterio que el Dockerfile de la app.
RUN useradd --create-home --uid 1000 balanceador

COPY --from=builder /instalado /usr/local
# Los tres módulos del balanceador. `clientereplica.py` es el que rutea contra
# el clúster de colas y `clientecola.py` el transporte que usa por debajo:
# sin el primero la imagen ni siquiera importa.
COPY app/balanceador.py app/clientecola.py app/clientereplica.py ./

RUN mkdir -p /app/logs && chown -R balanceador:balanceador /app
USER balanceador

ENV BA_PUERTO=8080 \
    BA_PUERTO_ADMIN=8081 \
    BA_ADMIN_BIND=127.0.0.1 \
    BA_COLA_URL=http://127.0.0.1:8085 \
    BA_LOGS=/app/logs \
    PYTHONUNBUFFERED=1

# Sólo se publica el puerto público. El de administración escucha en loopback:
# el CD corre en la misma máquina, también con --network host, y llega por
# 127.0.0.1:8081. Abrirlo (BA_ADMIN_BIND + BA_ADMIN_IPS) es sólo para un
# balanceador que corra en otra casa.
EXPOSE 8080

# Se chequea a sí mismo: 200 mientras alcance la cola y tenga al menos una
# réplica sana. Las dos cosas hacen falta — sin cola no se atiende nada, aunque
# las réplicas estén perfectas.
# El puerto sale de BA_PUERTO y no va fijo: con el puerto hardcodeado, levantar el
# balanceador en otro puerto lo deja marcado unhealthy aunque esté sirviendo bien,
# y eso en una demo se confunde con una caída de verdad.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import os,urllib.request,sys; p=os.environ.get('BA_PUERTO','8080'); sys.exit(0 if urllib.request.urlopen(f'http://localhost:{p}/health',timeout=2).status==200 else 1)"

STOPSIGNAL SIGTERM

CMD ["python", "balanceador.py"]
