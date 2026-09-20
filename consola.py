#!/usr/bin/env python3
"""Consola del balanceador y su cola: configura, levanta y observa.

Mismo criterio que la consola del CD: la configuración se arma una vez, se guarda
en `balanceador.env` —que es el `--env-file` de los contenedores— y después se
opera por menú. Veinte variables a mano es una receta para levantar el plano de
control en la interfaz equivocada o para que el token del balanceador y el de la
cola no coincidan.

Un solo archivo para los dos contenedores a propósito: cada uno ignora las
variables del otro, y así los tres valores que tienen que coincidir —la URL, el
token y el puerto de la cola— se escriben una sola vez.

    python3 consola.py

Sólo biblioteca estándar. No pide sudo.
"""
import json
import os
import re
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.request

RAIZ = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(RAIZ, "balanceador.env")
IMAGEN = "sdypp-balanceador:local"
CONTENEDOR = "sdypp-ba"
IMAGEN_COLA = "sdypp-cola:local"
CONTENEDOR_COLA = "sdypp-cola"

COLOR = sys.stdout.isatty() and os.environ.get("TERM") not in (None, "dumb")


def pintar(t, c):
    return f"\033[{c}m{t}\033[0m" if COLOR else t


def titulo(t):
    print("\n" + pintar(f"=== {t} ===", "1"))


def ok(t):
    print(pintar(f"  ✓ {t}", "32"))


def mal(t):
    print(pintar(f"  ✗ {t}", "31"))


def aviso(t):
    print(pintar(f"  ! {t}", "33"))


def nota(t):
    print(pintar(f"  {t}", "90"))


def consola_vecina():
    """La consola del CD, si el repo está al lado.

    Plataforma corre el balanceador y el CD en la misma máquina, en repos
    distintos: sin esto hay que acordarse de en qué carpeta vive cada consola.
    """
    ruta = os.path.join(os.path.dirname(RAIZ), "cd", "consola.py")
    return ruta if os.path.isfile(ruta) else None


def abrir_vecina():
    ruta = consola_vecina()
    if not ruta:
        mal("no encuentro el repo `cd` al lado de este")
        return
    print()
    nota(f"abriendo {ruta} — al salir volvés acá")
    try:
        subprocess.run([sys.executable, ruta], check=False)
    except (OSError, KeyboardInterrupt):
        pass


def pausa():
    try:
        input(pintar("\n  ⏎ para volver al menú ", "90"))
    except (EOFError, KeyboardInterrupt):
        pass


DEFAULTS = {
    "BA_CASA": "casa-tomas",
    "BA_NOMBRE": "balanceador",
    "BA_PUERTO": "8080",
    "BA_PUERTO_ADMIN": "8081",
    "BA_ADMIN_BIND": "127.0.0.1",
    "BA_ADMIN_IPS": "",
    "BA_BACKENDS": "",
    "BA_COLA_URL": "http://127.0.0.1:8085,http://127.0.0.1:8086,http://127.0.0.1:8087",
    "BA_COLA_TOKEN": "",
    "BA_RECOLECTORES": "4",
    "BA_ESPERA_RECOLECTOR": "20",
    "BA_PRESUPUESTO": "5",
    "BA_GRACIA_COLA": "1",
    "BA_INTERVALO_SALUD": "3",
    "BA_FALLOS_PARA_SACAR": "2",
    "BA_EXITOS_PARA_VOLVER": "1",
    "BA_TIMEOUT_SALUD": "2",
    # La cola es otro contenedor, pero comparte este archivo: COLA_TOKEN y
    # BA_COLA_TOKEN tienen que ser el mismo valor y acá se escribe una vez.
    "COLA_PUERTO": "8085",
    "COLA_BIND": "0.0.0.0",
    "COLA_CASA": "casa-tomas",
    "COLA_TOKEN": "",
    "COLA_COTA_PEDIDOS": "100",
    "COLA_COTA_RESPUESTAS": "1000",
    "COLA_RESERVA": "2",
    "COLA_TTL_RESPUESTAS": "60",
    "COLA_ESPERA_MAXIMA": "30",
}


def leer_config():
    cfg = dict(DEFAULTS)
    try:
        with open(CONFIG, encoding="utf-8") as f:
            for linea in f:
                linea = linea.rstrip("\n")
                if linea and not linea.startswith("#") and "=" in linea:
                    clave, _, valor = linea.partition("=")
                    cfg[clave.strip()] = valor
    except OSError:
        pass
    return cfg


def guardar_config(cfg):
    with open(CONFIG, "w", encoding="utf-8") as f:
        f.write("# Configuración del balanceador.\n"
                "# Es el --env-file del contenedor. Generado por consola.py; no se versiona.\n")
        for clave in DEFAULTS:
            f.write(f"{clave}={cfg.get(clave, '')}\n")
    os.chmod(CONFIG, 0o600)


def hay_config():
    return os.path.exists(CONFIG)


# ------------------------------------------------------------------ utilidades
def correr(*orden, timeout=20):
    try:
        r = subprocess.run(orden, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip() if r.returncode == 0 else ""
    except (OSError, subprocess.TimeoutExpired):
        return ""


def ip_tailscale():
    salida = correr("tailscale", "ip", "-4")
    if salida:
        return salida.splitlines()[0].strip()
    for linea in correr("ip", "-4", "-o", "addr", "show", "tailscale0").split("\n"):
        hallado = re.search(r"inet (\d+\.\d+\.\d+\.\d+)", linea)
        if hallado:
            return hallado.group(1)
    return ""


def pedir(etiqueta, ayuda=None, default="", obligatorio=True, validar=None):
    while True:
        print()
        print(pintar(etiqueta, "1"))
        if ayuda:
            nota(ayuda)
        if default:
            nota(f"[{default}]  ⏎ para aceptar")
        try:
            leido = input("  > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\ncancelado")
            sys.exit(1)
        valor = leido or default
        if not valor and obligatorio:
            mal("hace falta un valor")
            continue
        if validar:
            problema = validar(valor)
            if problema:
                mal(problema)
                continue
        return valor


def pedir_si(pregunta, por_defecto=True):
    sufijo = "[S/n]" if por_defecto else "[s/N]"
    try:
        r = input(pintar(f"  {pregunta} {sufijo} ", "1")).strip().lower()
    except (EOFError, KeyboardInterrupt):
        return False
    return por_defecto if not r else r in ("s", "si", "sí", "y", "yes")


def es_puerto(v):
    return None if v.isdigit() and 1 <= int(v) <= 65535 else "un puerto entre 1 y 65535"


def es_numero(v):
    try:
        float(v)
        return None
    except ValueError:
        return "un número"


def url_admin(cfg):
    return f"http://127.0.0.1:{cfg['BA_PUERTO_ADMIN']}"


def url_publica(cfg):
    return f"http://127.0.0.1:{cfg['BA_PUERTO']}"


def url_cola(cfg):
    return f"http://127.0.0.1:{cfg['COLA_PUERTO']}"


def pedir_json(url, metodo="GET", cuerpo=None, timeout=10):
    pedido = urllib.request.Request(url, data=cuerpo, method=metodo,
                                    headers={"Content-Type": "application/json"} if cuerpo else {})
    try:
        with urllib.request.urlopen(pedido, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except (ValueError, OSError):
            return e.code, None


# ------------------------------------------------------------------ estado
def corriendo(contenedor=CONTENEDOR):
    return correr("docker", "inspect", "-f", "{{.State.Running}}", contenedor) == "true"


def estado_cola(cfg):
    """El /estado de la cola. None si no responde: es el modo de falla nuevo."""
    try:
        codigo, datos = pedir_json(f"{url_cola(cfg)}/estado", timeout=3)
    except (urllib.error.URLError, OSError, ValueError):
        return None
    return datos if codigo == 200 else None


def backends(cfg):
    try:
        return pedir_json(f"{url_admin(cfg)}/admin/backends")[1]
    except (urllib.error.URLError, OSError, ValueError):
        return None


def desenvolver(datos):
    """El plano público contesta {"Code": …, "contenido": {…}}; el de control no.

    Se usa sólo con las rutas públicas. Si no viene envuelto se devuelve igual,
    así la consola sigue sirviendo contra un balanceador sin actualizar.
    """
    if isinstance(datos, dict) and "Code" in datos and "contenido" in datos:
        return datos["contenido"]
    return datos


def salud(cfg):
    try:
        codigo, datos = pedir_json(f"{url_publica(cfg)}/health")
        return codigo, desenvolver(datos)
    except (urllib.error.URLError, OSError, ValueError):
        return None, None


def encabezado(cfg):
    codigo, datos = salud(cfg)
    if datos:
        color = "32" if codigo == 200 else "33"
        cola = datos.get("cola") or {}
        estado = pintar(f"{datos.get('replicasSanas', 0)}/{datos.get('replicasTotales', 0)} sanas"
                        f" · cola {cola.get('estado', '?')}"
                        f" · {datos.get('encolados', 0)}/{datos.get('cota', '?')} en cola"
                        f" · {cola.get('enVuelo', 0)} en vuelo", color)
    elif corriendo():
        estado = pintar("arrancando o sin responder", "33")
    else:
        estado = pintar("abajo", "31")
    print()
    print(pintar(f"  Balanceador · {cfg['BA_CASA']} · público :{cfg['BA_PUERTO']}"
                 f" · control {cfg['BA_ADMIN_BIND']}:{cfg['BA_PUERTO_ADMIN']}"
                 f" · cola :{cfg['COLA_PUERTO']}", "1"))
    print(f"  {estado}")


# ------------------------------------------------------------------ el contenedor
def construir(imagen=IMAGEN, contexto=RAIZ):
    print(f"  construyendo {imagen}...")
    r = subprocess.run(["docker", "build", "-t", imagen, contexto], capture_output=True, text=True)
    if r.returncode != 0:
        mal("falló el build")
        print(r.stderr[-1500:])
        return False
    ok(f"imagen {imagen}")
    return True


def orden_docker():
    return ["docker", "run", "-d", "--name", CONTENEDOR, "--restart", "unless-stopped",
            "--network", "host", "--env-file", CONFIG,
            "-v", f"{RAIZ}/logs:/app/logs", IMAGEN]


def orden_docker_cola():
    # --network host igual que el balanceador: así el balanceador la alcanza por
    # 127.0.0.1 (no sale a la red para encolar) y las réplicas de las otras casas
    # por la IP de Tailscale de esta máquina, con un solo puerto publicado.
    return ["docker", "run", "-d", "--name", CONTENEDOR_COLA, "--restart", "unless-stopped",
            "--network", "host", "--env-file", CONFIG,
            "-v", f"{RAIZ}/logs:/app/logs", IMAGEN_COLA]


def levantar_cola(cfg):
    titulo("Levantando el sistema de colas")
    if not cfg.get("COLA_TOKEN"):
        aviso("sin COLA_TOKEN: cualquiera que alcance el puerto puede tomar pedidos reales")
        if not pedir_si("¿Seguir igual?", por_defecto=False):
            return False
    if not construir(IMAGEN_COLA, os.path.join(RAIZ, "cola")):
        return False
    os.makedirs(os.path.join(RAIZ, "logs"), exist_ok=True)
    subprocess.run(["docker", "rm", "-f", CONTENEDOR_COLA], capture_output=True)
    r = subprocess.run(orden_docker_cola(), capture_output=True, text=True)
    if r.returncode != 0:
        mal("no arrancó")
        print(r.stderr[-1500:])
        return False
    for _ in range(40):
        if estado_cola(cfg) is not None:
            ok(f"arriba · :{cfg['COLA_PUERTO']}")
            return True
        time.sleep(0.5)
    mal("arrancó pero no responde. Últimas líneas:")
    print(correr("docker", "logs", "--tail", "15", CONTENEDOR_COLA))
    return False


def levantar(cfg):
    titulo("Levantando el balanceador")
    if not corriendo(CONTENEDOR_COLA):
        # Sin cola el balanceador arranca igual y contesta 503 a todo. Vale la
        # pena decirlo acá y no dejar que se descubra por un 503 en la demo.
        aviso("la cola no está corriendo: el balanceador va a contestar 503 a todo")
        if pedir_si("¿La levanto primero?") and not levantar_cola(cfg):
            return False
    if cfg["BA_ADMIN_BIND"] not in ("127.0.0.1", "::1") and not cfg["BA_ADMIN_IPS"]:
        aviso(f"el plano de control va a escuchar en {cfg['BA_ADMIN_BIND']} sin lista blanca")
        nota("quien lo alcance decide a dónde va TODO el tráfico")
        if not pedir_si("¿Seguir igual?", por_defecto=False):
            return False
    if not construir():
        return False
    os.makedirs(os.path.join(RAIZ, "logs"), exist_ok=True)
    subprocess.run(["docker", "rm", "-f", CONTENEDOR], capture_output=True)
    r = subprocess.run(orden_docker(), capture_output=True, text=True)
    if r.returncode != 0:
        mal("no arrancó")
        print(r.stderr[-1500:])
        return False
    for _ in range(40):
        if backends(cfg) is not None:
            ok(f"arriba · público :{cfg['BA_PUERTO']} · control {cfg['BA_ADMIN_BIND']}:{cfg['BA_PUERTO_ADMIN']}")
            if not cfg["BA_BACKENDS"]:
                nota("el registro arranca vacío: /health contesta 503 hasta el primer deploy del CD")
            nota("las réplicas atienden cuando su worker consume de la cola, no cuando "
                 "entran acá")
            return True
        time.sleep(0.5)
    mal("arrancó pero no responde. Últimas líneas:")
    print(correr("docker", "logs", "--tail", "15", CONTENEDOR))
    return False


# ------------------------------------------------------------------ opciones
def ver_pool(cfg):
    titulo("Réplicas")
    datos = backends(cfg)
    if datos is None:
        mal(f"el plano de control no responde en {url_admin(cfg)}")
        nota("escucha en loopback: esta consola tiene que correr en la misma máquina")
        return
    if not datos["backends"]:
        aviso("vacío. /health contesta 503 hasta que el CD conmute por primera vez.")
        return
    print(pintar(f"\n    {'destino':<24}{'app':<9}{'sano':<7}{'consume':<10}{'vuelo':<7}"
                 f"{'fallos':<8}atendidos", "90"))
    for b in datos["backends"]:
        consume = b.get("consumiendo")
        print(f"    {b['destino']:<24}{b.get('app', '?'):<9}"
              + pintar(f"{str(b.get('sano')):<7}", "32" if b.get("sano") else "31")
              + pintar(f"{('sí' if consume else 'no'):<10}", "32" if consume else "31")
              + f"{str(b.get('enVuelo', '—')):<7}"
                f"{str(b.get('fallos', 0)):<8}{b.get('atendidos', '—')}")
    print()
    nota("sano = contesta el health gRPC · consume = fue a buscar trabajo a la cola hace poco")
    nota("sana pero sin consumir = el contenedor vive y su worker no; ésa no atiende nada")


def ver_health(cfg):
    titulo("Health público")
    codigo, datos = salud(cfg)
    if datos is None:
        mal(f"no responde en {url_publica(cfg)}")
        return
    print(f"\n  HTTP {codigo}"
          + ("" if codigo == 200 else pintar("  (503 = sin cola o sin réplicas sanas)", "33")))
    for clave in ("balanceador", "casa", "replicasSanas", "replicasTotales"):
        if clave in datos:
            print(f"    {clave:<16} {datos[clave]}")
    cola = datos.get("cola") or {}
    print(pintar("\n  la cola", "1"))
    for clave in ("url", "estado", "encolados", "enVuelo", "cota", "reasignados",
                  "respuestasPendientes"):
        print(f"    {clave:<22} {cola.get(clave, '—')}")
    print()
    nota("reasignados = pedidos que una réplica tomó y no contestó, y otra terminó "
         "atendiendo")
    nota(f'el cuerpo viaja envuelto: {{"Code": {codigo}, "contenido": {{…}}}}')
    nota("misma forma para el éxito y para el error; lo de arriba es el contenido")


def tocar_pool(cfg):
    titulo("Agregar o quitar un backend a mano")
    nota("Esto lo hace el CD al conmutar. Acá es para una emergencia: sacar de rotación")
    nota("una réplica que anda mal, o volver a meter una a mano si un deploy quedó a medias.")
    nota("El CD no se entera: su estado va a quedar desfasado hasta el próximo deploy.")
    print("""
  1  Agregar un backend
  2  Quitar un backend
  0  Volver""")
    try:
        opcion = input("\n  > ").strip()
    except (EOFError, KeyboardInterrupt):
        return
    if opcion not in ("1", "2"):
        return

    if opcion == "2":
        datos = backends(cfg)
        if not datos or not datos["backends"]:
            aviso("el pool está vacío")
            return
        for i, b in enumerate(datos["backends"], 1):
            print(f"  {i}  {b['destino']}  ({b.get('app', '?')})")
        try:
            elegido = int(input("\n  ¿Cuál? [0 cancela] > ").strip() or "0")
        except (ValueError, EOFError, KeyboardInterrupt):
            return
        if not 1 <= elegido <= len(datos["backends"]):
            return
        destino = datos["backends"][elegido - 1]["destino"]
        cuerpo = {"agregar": [], "quitar": [destino]}
    else:
        destino = pedir("Destino", "host:puerto de la réplica, con la IP de Tailscale",
                        validar=lambda v: None if re.fullmatch(r"[\w.:-]+:\d+", v)
                        else "host:puerto")
        app = pedir("Equipo", "python o java", default="python",
                    validar=lambda v: None if v in ("python", "java") else "python o java")
        cuerpo = {"agregar": [{"destino": destino, "app": app}], "quitar": []}

    codigo, datos = pedir_json(f"{url_admin(cfg)}/admin/backends", "POST",
                               json.dumps(cuerpo).encode())
    if codigo == 200:
        ok(f"agregados={datos.get('agregados') or '-'} quitados={datos.get('quitados') or '-'}")
    else:
        mal(f"HTTP {codigo}")


def verificar(cfg):
    titulo("Mandar tráfico de prueba")
    nota("Corre app/verificador.py contra el puerto público: sirve para ver que no se")
    nota("pierde nada durante un deploy. En la entrega lo corre el otro equipo, no nosotros.")
    n = pedir("¿Cuántas requests?", default="100",
              validar=lambda v: None if v.isdigit() and int(v) > 0 else "un entero positivo")
    hilos = pedir("¿Cuántas en paralelo?", default="4",
                  validar=lambda v: None if v.isdigit() and int(v) > 0 else "un entero positivo")
    alta = pedir_si("¿Probar también un alta y su lectura?", por_defecto=False)

    orden = [sys.executable, os.path.join(RAIZ, "app", "verificador.py"),
             url_publica(cfg), "--n", n, "--hilos", hilos]
    if alta:
        orden.append("--alta")
    print()
    try:
        subprocess.run(orden, check=False)
    except (OSError, KeyboardInterrupt) as e:
        mal(str(e))


def ver_bitacora(cfg):
    titulo("Bitácora")
    ruta = os.path.join(RAIZ, "logs")
    archivos = []
    try:
        archivos = sorted((os.path.join(ruta, f) for f in os.listdir(ruta) if f.endswith(".log")),
                          key=os.path.getmtime, reverse=True)
    except OSError:
        pass
    if not archivos:
        aviso("todavía no hay bitácora")
        return
    try:
        with open(archivos[0], encoding="utf-8") as f:
            lineas = f.readlines()[-25:]
    except OSError as e:
        mal(str(e))
        return
    nota(f"{archivos[0]}")
    for linea in lineas:
        partes = linea.strip().split(" | ")
        if len(partes) >= 4 and partes[3].isdigit():
            color = "32" if partes[3].startswith("2") else "31" if partes[3][0] in "45" else "0"
            print(pintar(f"  {' | '.join([partes[0][11:19]] + partes[2:])}", color))
        else:
            print(f"  {linea.rstrip()}")


def menu_contenedor(cfg):
    titulo("Los contenedores")
    print(f"  {CONTENEDOR:<14} {'corriendo' if corriendo() else 'parado o inexistente'}")
    print(f"  {CONTENEDOR_COLA:<14} "
          f"{'corriendo' if corriendo(CONTENEDOR_COLA) else 'parado o inexistente'}")
    print("""
  1  Levantar / reiniciar los dos
  2  Levantar / reiniciar sólo el balanceador
  3  Levantar / reiniciar sólo la cola
  4  Bajar los dos
  5  Logs del balanceador
  6  Logs de la cola
  0  Volver""")
    try:
        opcion = input("\n  > ").strip()
    except (EOFError, KeyboardInterrupt):
        return
    if opcion == "1":
        # La cola primero: al revés, el balanceador arranca contestando 503 y
        # quien mire /health en esos segundos cree que está roto.
        levantar_cola(cfg) and levantar(cfg)
        pausa()
    elif opcion == "2":
        levantar(cfg)
        pausa()
    elif opcion == "3":
        aviso("reiniciar la cola tira los pedidos que estaban esperando: la cola no persiste")
        if pedir_si("¿Sigo?", False):
            levantar_cola(cfg)
        pausa()
    elif opcion == "4":
        aviso("mientras estén abajo, nadie atiende el puerto público")
        if pedir_si("¿Bajo los dos?", False):
            for c in (CONTENEDOR, CONTENEDOR_COLA):
                subprocess.run(["docker", "rm", "-f", c], capture_output=True)
            ok("bajados")
            pausa()
    elif opcion == "5":
        print()
        print(correr("docker", "logs", "--tail", "40", CONTENEDOR))
        pausa()
    elif opcion == "6":
        print()
        print(correr("docker", "logs", "--tail", "40", CONTENEDOR_COLA))
        pausa()


def ver_config(cfg):
    titulo("Configuración")
    for clave in DEFAULTS:
        print(f"  {clave:<24} {cfg.get(clave) or '—'}")
    print()
    nota(f"archivo: {CONFIG}")
    nota("es el --env-file de los dos contenedores; los docker run equivalentes:")
    print("  " + " ".join(orden_docker_cola()))
    print("  " + " ".join(orden_docker()))
    print()
    if pedir_si("¿Reconfigurar?", por_defecto=False):
        return configurar(cfg)
    return cfg


# ------------------------------------------------------------------ asistente
def configurar(cfg=None):
    cfg = cfg or leer_config()
    titulo("Configuración del balanceador")
    print("  Sólo lo que suele cambiar. El resto queda con sus defaults y se")
    print("  puede tocar después desde la opción 7.")

    cfg["BA_CASA"] = pedir("Nombre de esta máquina", "Sale en la bitácora y en /health.",
                           default=cfg.get("BA_CASA") or "casa-tomas")
    cfg["BA_PUERTO"] = pedir("Puerto público", "El que ven los clientes y el verificador.",
                             default=cfg.get("BA_PUERTO") or "8080", validar=es_puerto)
    cfg["BA_PUERTO_ADMIN"] = pedir("Puerto del plano de control", "Por acá conmuta el CD.",
                                   default=cfg.get("BA_PUERTO_ADMIN") or "8081", validar=es_puerto)

    mia = ip_tailscale()
    print()
    print(pintar("¿Quién puede tocar /admin/backends?", "1"))
    nota("Quien lo alcance decide a dónde va TODO el tráfico del servicio.")
    print("""
  1  Sólo esta máquina (127.0.0.1) — el CD corre acá al lado. Recomendado.
  2  Otra máquina del tailnet — para un CD remoto (Etapa 3). Pide lista blanca.""")
    try:
        eleccion = input("\n  > ").strip() or "1"
    except (EOFError, KeyboardInterrupt):
        eleccion = "1"
    if eleccion == "2":
        cfg["BA_ADMIN_BIND"] = pedir("IP en la que escuchar el control",
                                     "La de Tailscale de esta máquina, nunca 0.0.0.0.",
                                     default=mia or "", validar=lambda v: None
                                     if re.fullmatch(r"[\d.:a-fA-F]+", v) else "una IP")
        cfg["BA_ADMIN_IPS"] = pedir("IP del CD autorizada",
                                    "Lista blanca, separada por comas. Sin esto, cualquiera del "
                                    "tailnet\n  puede redirigir el tráfico.",
                                    validar=lambda v: None if v else "hace falta al menos una")
    else:
        cfg["BA_ADMIN_BIND"] = "127.0.0.1"
        cfg["BA_ADMIN_IPS"] = ""

    print()
    print(pintar("La cola", "1"))
    nota("Corre en su propio contenedor. El balanceador no atiende nada sin ella.")
    cfg["COLA_PUERTO"] = pedir("Puerto base de la cola",
                               "Por acá entran los workers de las réplicas.",
                               default=cfg.get("COLA_PUERTO") or "8085", validar=es_puerto)
    cfg["COLA_CASA"] = cfg["BA_CASA"]
    nodos = int(pedir("Cantidad de nodos del clúster de colas",
                      "Mínimo 3 nodos recomendados para alta disponibilidad (impar).",
                      default=cfg.get("COLA_NODOS") or "3", validar=es_numero))
    if nodos < 1:
        nodos = 1
    cfg["COLA_NODOS"] = str(nodos)
    puerto_base = int(cfg["COLA_PUERTO"])
    urls_semilla = [f"http://127.0.0.1:{puerto_base + i}" for i in range(nodos)]
    cfg["BA_COLA_URL"] = ",".join(urls_semilla)

    # El token se genera en vez de pedirse: uno elegido a mano termina siendo
    # "cola123" y esto lo alcanza cualquiera del tailnet.
    if not cfg.get("COLA_TOKEN") or pedir_si("Ya hay un token, ¿genero uno nuevo?", False):
        cfg["COLA_TOKEN"] = secrets.token_urlsafe(32)
        ok("token generado")
    cfg["BA_COLA_TOKEN"] = cfg["COLA_TOKEN"]
    nota("el mismo token va en los workers de las réplicas; se lee con la opción 7")

    if pedir_si("¿Ajustar cotas y tiempos (cola 100, reserva 2 s, presupuesto 5 s)?", False):
        cfg["COLA_COTA_PEDIDOS"] = pedir(
            "Cota de la cola de pedidos", "Cuántos esperan antes de contestar 503.",
            default=cfg.get("COLA_COTA_PEDIDOS") or "100", validar=es_numero)
        cfg["COLA_RESERVA"] = pedir(
            "Reserva en segundos",
            "Cuánto tiene un worker para contestar antes de que el pedido se le\n"
            "  ofrezca a otro. Tiene que ser bastante menor que el presupuesto.",
            default=cfg.get("COLA_RESERVA") or "2", validar=es_numero)
        cfg["BA_PRESUPUESTO"] = pedir(
            "Presupuesto por pedido en segundos", "Todo el viaje, espera en la cola incluida.",
            default=cfg.get("BA_PRESUPUESTO") or "5", validar=es_numero)

    cfg["BA_BACKENDS"] = ""  # el registro lo arma el CD al conmutar
    guardar_config(cfg)
    ok(f"guardado en {CONFIG}  (600)")
    nota("el registro de réplicas arranca vacío: lo llena el CD en el primer deploy")
    return cfg


MENU = """
  1  Réplicas                 cuáles hay, sanas, consumiendo, en vuelo y atendidas
  2  Health público           lo que ve el verificador, con el estado de la cola
  3  Tráfico de prueba        correr el verificador contra este balanceador
  4  Agregar / quitar réplica  a mano, para una emergencia
  5  Bitácora                 últimas 25 líneas
  6  Contenedores             balanceador y cola: levantar, bajar, logs
  7  Configuración            ver y editar
  8  Consola del CD           el otro componente de Plataforma
  0  Salir"""


def main():
    if not hay_config():
        titulo("Primera vez")
        print("  No hay configuración todavía. Vamos a armarla.")
        cfg = configurar()
        print()
        if pedir_si("¿Levanto la cola y el balanceador ahora?"):
            levantar_cola(cfg) and levantar(cfg)
    while True:
        cfg = leer_config()
        encabezado(cfg)
        print(MENU)
        try:
            opcion = input("\n  > ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if opcion == "1":
            ver_pool(cfg)
            pausa()
        elif opcion == "2":
            ver_health(cfg)
            pausa()
        elif opcion == "3":
            verificar(cfg)
            pausa()
        elif opcion == "4":
            tocar_pool(cfg)
            pausa()
        elif opcion == "5":
            ver_bitacora(cfg)
            pausa()
        elif opcion == "6":
            menu_contenedor(cfg)
        elif opcion == "7":
            ver_config(cfg)
        elif opcion == "8":
            abrir_vecina()
        elif opcion == "0":
            return 0
        elif opcion:
            mal("no conozco esa opción")


if __name__ == "__main__":
    sys.exit(main())
