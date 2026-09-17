# Imagen del balanceador. Multi-etapa: grpcio-tools pesa 8 MB y sólo hace falta
# para generar los stubs, así que se queda en la etapa builder.

FROM python:3.13-slim AS builder
WORKDIR /build
COPY requirements.txt requirements-build.txt ./
RUN pip install --no-cache-dir --prefix=/instalado -r requirements.txt && \
    pip install --no-cache-dir -r requirements-build.txt
COPY contrato.proto ./
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. contrato.proto


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
COPY --from=builder /build/contrato_pb2.py /build/contrato_pb2_grpc.py ./
COPY app/balanceador.py ./

RUN mkdir -p /app/logs && chown -R balanceador:balanceador /app
USER balanceador

ENV BA_PUERTO=8080 \
    BA_PUERTO_ADMIN=8081 \
    BA_ADMIN_BIND=127.0.0.1 \
    BA_LOGS=/app/logs \
    PYTHONUNBUFFERED=1

# Sólo se publica el puerto público. El de administración escucha en loopback por
# defecto; para que las casas conmuten desde afuera hay que abrirlo con
# BA_ADMIN_BIND, restringirlo con BA_ADMIN_IPS y correr con --network host.
EXPOSE 8080

# Se chequea a sí mismo: 200 mientras tenga al menos una réplica en rotación.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/health',timeout=2).status==200 else 1)"

STOPSIGNAL SIGTERM

CMD ["python", "balanceador.py"]
