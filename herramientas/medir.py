"""
Herramienta de medición - Taller de Laboratorio N.º 3 (punto 8)
Autores: David Felipe Garcia Ortiz - Juan David Claros

Corre dentro de Docker (no requiere instalar nada en el equipo):

    docker compose run --rm herramientas python medir.py --n 20 --etiqueta medicion_5s

1. Envía N pagos seguidos al POST /pagos y mide el tiempo de respuesta de cada uno.
2. (Opcional) Consulta GET /pagos/{id} hasta que todos estén PROCESADO.
3. Calcula las medidas del punto 8 con los tiempos guardados en la base de datos.
Sólo usa la librería estándar de Python.
"""
import argparse
import json
import statistics
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

API = "http://api:8000"


def peticion(metodo, ruta, cuerpo=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(API + ruta, data=datos, method=metodo,
                                 headers={"Content-Type": "application/json"})
    inicio = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            codigo, texto = resp.status, resp.read()
    except urllib.error.HTTPError as err:
        codigo, texto = err.code, err.read()
    ms = (time.perf_counter() - inicio) * 1000
    return codigo, json.loads(texto), ms


def fecha(texto):
    return datetime.fromisoformat(texto)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--prefijo", default="PAG")
    ap.add_argument("--medio", default="transferencia")
    ap.add_argument("--etiqueta", default="medicion")
    ap.add_argument("--no-esperar", action="store_true", help="no espera a que queden PROCESADO")
    ap.add_argument("--timeout", type=int, default=1800, help="segundos máximos de espera")
    args = ap.parse_args()

    marca = datetime.now().strftime("%H%M%S")
    registros = []
    print(f"Enviando {args.n} pagos seguidos a {API}/pagos ...")
    t_inicio = time.perf_counter()
    for i in range(1, args.n + 1):
        cuerpo = {"referencia": f"{args.prefijo}-{marca}-{i:04d}", "valor": 100000 + i * 1000, "medio": args.medio}
        codigo, resp, ms = peticion("POST", "/pagos", cuerpo)
        registros.append({"n": i, "http": codigo, "respuesta": resp, "ms": round(ms, 2),
                          "id": resp.get("data", {}).get("id")})
        print(f"  #{i:02d} HTTP {codigo} {ms:8.2f} ms  {json.dumps(resp, ensure_ascii=False)}")
    t_envio = time.perf_counter() - t_inicio

    tiempos = [r["ms"] for r in registros]
    resultado = {
        "etiqueta": args.etiqueta,
        "n": args.n,
        "ids": [r["id"] for r in registros],
        "respuestas_ms": tiempos,
        "api_media_ms": round(statistics.mean(tiempos), 2),
        "api_mediana_ms": round(statistics.median(tiempos), 2),
        "api_max_ms": round(max(tiempos), 2),
        "api_min_ms": round(min(tiempos), 2),
        "envio_total_s": round(t_envio, 3),
    }
    print(f"\nTiempo de respuesta de la API: media {resultado['api_media_ms']} ms | "
          f"máximo {resultado['api_max_ms']} ms | mínimo {resultado['api_min_ms']} ms")
    print(f"Los {args.n} POST se enviaron en {t_envio:.3f} s")

    # Estado inmediatamente después de enviar
    estados = [peticion("GET", f"/pagos/{r['id']}")[1]["data"]["estado"] for r in registros]
    resultado["estados_al_terminar_envio"] = {e: estados.count(e) for e in set(estados)}
    print(f"Estados justo después del envío: {resultado['estados_al_terminar_envio']}")

    if not args.no_esperar:
        print("\nEsperando a que el consumidor procese todos los pagos ...")
        limite = time.time() + args.timeout
        pendientes = {r["id"] for r in registros}
        pagos = {}
        while pendientes and time.time() < limite:
            for pid in sorted(pendientes):
                data = peticion("GET", f"/pagos/{pid}")[1]["data"]
                if data["estado"] == "PROCESADO":
                    pagos[pid] = data
            pendientes -= set(pagos)
            print(f"  {time.strftime('%H:%M:%S')} procesados {len(pagos)}/{args.n}")
            if pendientes:
                time.sleep(2)
        resultado["t_cliente_hasta_todos_procesados_s"] = round(time.perf_counter() - t_inicio, 3)

        filas = []
        for r in registros:
            p = pagos.get(r["id"])
            if not p:
                continue
            reg, fin = fecha(p["fecha_registro"]), fecha(p["procesamiento"]["fecha_procesamiento"])
            toma = fecha(p["procesamiento"]["fecha_toma"])
            filas.append({"id": r["id"], "registro": p["fecha_registro"],
                          "toma": p["procesamiento"]["fecha_toma"],
                          "procesado": p["procesamiento"]["fecha_procesamiento"],
                          "espera_en_cola_s": round((toma - reg).total_seconds(), 3),
                          "registro_a_procesado_s": round((fin - reg).total_seconds(), 3)})
        resultado["detalle"] = filas
        if len(filas) == args.n:
            primero, ultimo = filas[0], filas[-1]
            total = fecha(ultimo["procesado"]) - fecha(primero["registro"])
            resultado["primer_pago_s"] = primero["registro_a_procesado_s"]
            resultado["ultimo_pago_s"] = ultimo["registro_a_procesado_s"]
            resultado["total_20_procesados_s"] = round(total.total_seconds(), 3)

        print("\n  id  | registro                | tomado de la cola       | procesado               | en cola (s) | registro->procesado (s)")
        for f in filas:
            print(f"  {f['id']:>3} | {f['registro']} | {f['toma']} | {f['procesado']} | {f['espera_en_cola_s']:>11.3f} | {f['registro_a_procesado_s']:>10.3f}")

    print("\n================ TABLA DE MEDICIÓN ================")
    print(f"Tiempo de respuesta de la API (media de los {args.n} registros): {resultado['api_media_ms']} ms")
    print(f"Tiempo de respuesta de la API (máximo observado)            : {resultado['api_max_ms']} ms")
    if "primer_pago_s" in resultado:
        print(f"Tiempo entre registro y procesamiento del primer pago       : {resultado['primer_pago_s']} s")
        print(f"Tiempo entre registro y procesamiento del último pago       : {resultado['ultimo_pago_s']} s")
        print(f"Tiempo total hasta que los {args.n} pagos quedaron PROCESADO   : {resultado['total_20_procesados_s']} s")
    print("===================================================")

    salida = Path("/resultados") / f"{args.etiqueta}.json"
    salida.parent.mkdir(parents=True, exist_ok=True)
    salida.write_text(json.dumps(resultado, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Resultados guardados en resultados/{salida.name}")


if __name__ == "__main__":
    main()
