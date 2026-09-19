#!/usr/bin/env python3
"""Verificador: mide el servicio desde afuera y muestra el resultado.

Lo corre OTRO equipo, desde OTRA casa. Nadie corrige su propio examen.

    python3 app/verificador.py http://100.101.15.93:8080
    python3 app/verificador.py http://100.101.15.93:8080 --n 200 --hilos 8
    python3 app/verificador.py http://100.101.15.93:8080 --alta

Sólo usa la biblioteca estándar: el equipo que verifica no debería tener que
instalar nada nuestro para probarnos.
"""

import argparse
import json
import random
import string
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter


def desenvolver(datos):
    """Saca el contenido del sobre {"Code": …, "contenido": {…}}.

    El código HTTP ya viaja aparte en la tupla de `pedir`, así que acá abajo
    sólo interesa el contenido. Si la respuesta no viene envuelta se devuelve
    tal cual: sirve para apuntar el verificador a un balanceador viejo, o a una
    réplica directo, sin que el script explote.
    """
    if isinstance(datos, dict) and "Code" in datos and "contenido" in datos:
        return datos["contenido"]
    return datos


def pedir(url, metodo="GET", cuerpo=None, timeout=10):
    """Devuelve (codigo, contenido_o_None, segundos). Un error HTTP no es
    excepción: un 503 es un dato del experimento, no un accidente del script."""
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    pedido = urllib.request.Request(
        url, data=datos, method=metodo,
        headers={"Content-Type": "application/json"} if datos else {})
    arranque = time.perf_counter()
    try:
        with urllib.request.urlopen(pedido, timeout=timeout) as r:
            return (r.status, desenvolver(json.loads(r.read() or b"null")),
                    time.perf_counter() - arranque)
    except urllib.error.HTTPError as e:
        try:
            return (e.code, desenvolver(json.loads(e.read() or b"null")),
                    time.perf_counter() - arranque)
        except ValueError:
            return e.code, None, time.perf_counter() - arranque
    except Exception as e:
        # Esto no vino del servidor: no hay sobre que abrir.
        return 0, {"error": str(e)}, time.perf_counter() - arranque


def barra(n, total, ancho=28):
    llenos = int(ancho * n / total) if total else 0
    return "█" * llenos + "·" * (ancho - llenos)


def main():
    p = argparse.ArgumentParser(description="Verificador del servicio replicado")
    p.add_argument("url", help="la URL pública del balanceador")
    p.add_argument("--n", type=int, default=100, help="cuántas requests (default 100)")
    p.add_argument("--hilos", type=int, default=4, help="cuántas en paralelo (default 4)")
    p.add_argument("--alta", action="store_true",
                   help="además, dar un alta y leerla en la request siguiente")
    args = p.parse_args()
    base = args.url.rstrip("/")

    print(f"\n  Verificando {base}")
    print(f"  {args.n} requests, {args.hilos} en paralelo\n")

    codigos, servidas, latencias = Counter(), Counter(), []
    candado = threading.Lock()
    pendientes = list(range(args.n))

    def trabajar():
        while True:
            with candado:
                if not pendientes:
                    return
                pendientes.pop()
            codigo, cuerpo, t = pedir(f"{base}/")
            quien = (cuerpo or {}).get("host", "—") if codigo == 200 else "—"
            with candado:
                codigos[codigo] += 1
                servidas[quien] += 1
                latencias.append(t)

    arranque = time.perf_counter()
    hilos = [threading.Thread(target=trabajar) for _ in range(args.hilos)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    total = time.perf_counter() - arranque

    ok = codigos.get(200, 0)
    print("  ── Códigos ──")
    for codigo, n in sorted(codigos.items()):
        etiqueta = {0: "sin conexión"}.get(codigo, str(codigo))
        print(f"    {etiqueta:>13}  {n:4d}  {barra(n, args.n)}")

    print("\n  ── Reparto entre instancias ──")
    for quien, n in servidas.most_common():
        print(f"    {quien:>13}  {n:4d}  {barra(n, args.n)}  {100*n/args.n:5.1f} %")

    latencias.sort()
    def pct(q):
        return latencias[min(int(len(latencias) * q), len(latencias) - 1)] * 1000
    print("\n  ── Latencia ──")
    print(f"    p50 {pct(.50):7.1f} ms    p95 {pct(.95):7.1f} ms    p99 {pct(.99):7.1f} ms")
    print(f"    {args.n} requests en {total:.2f} s  ·  {args.n/total:.1f} req/s")

    if args.alta:
        legajo = random.randint(500000, 999999)
        nombre = "Verificador " + "".join(random.choices(string.ascii_uppercase, k=4))
        print("\n  ── Estado compartido ──")
        codigo, alta, _ = pedir(f"{base}/personas", "POST",
                                {"nombre": nombre, "legajo": legajo})
        if codigo != 201:
            print(f"    el alta falló con {codigo}: {alta}")
        else:
            print(f"    alta   {codigo}  id={alta['persona']['id']}  legajo={legajo}"
                  f"  la atendió {alta.get('servidoPor')}")
            codigo, lista, _ = pedir(f"{base}/personas")
            encontrada = any(p["legajo"] == legajo for p in (lista or {}).get("personas", []))
            print(f"    lectura {codigo}  {len((lista or {}).get('personas', []))} personas"
                  f"  ·  la recién dada de alta {'ESTÁ' if encontrada else 'NO ESTÁ'}")

    print(f"\n  {ok}/{args.n} OK  ({100*ok/args.n:.1f} %)\n")
    return 0 if ok == args.n else 1


if __name__ == "__main__":
    sys.exit(main())
