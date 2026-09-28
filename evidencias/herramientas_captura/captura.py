"""
Generador de capturas de evidencia (se ejecuta dentro de un contenedor Playwright,
unido a la red de docker compose). No requiere instalar nada en el equipo.

  python captura.py rabbit   <salida.png> [titulo]
  python captura.py swagger  <prefijo_salida>
  python captura.py terminales <carpeta_txt>

- rabbit: abre la consola web de RabbitMQ (http://rabbitmq:15672), inicia sesión
  y fotografía la lista de colas.
- swagger: en Swagger UI registra un pago válido y uno inválido y fotografía GET /pagos/{id}
  inmediatamente y pasados 10 segundos.
- terminales: convierte cada .txt (salida real de un comando) en una imagen con
  aspecto de terminal. La primera línea del .txt es el comando ejecutado.
"""
import html
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from playwright.sync_api import sync_playwright

RABBIT = "http://rabbitmq:15672"
API = "http://api:8000"
# Desde el equipo las mismas URL se ven con los puertos publicados:
URL_VISIBLE = {RABBIT: "http://localhost:15672", API: "http://localhost:8090"}

MARCO = """<!doctype html><html><head><meta charset="utf-8"><style>
body{{margin:0;background:#dfe3e8;font-family:Segoe UI,Arial,sans-serif}}
.win{{margin:14px;border-radius:8px;overflow:hidden;box-shadow:0 4px 18px rgba(0,0,0,.35);background:#fff;display:inline-block;min-width:760px;max-width:1470px}}
.bar{{background:#2b2b2b;color:#ddd;font-size:13px;padding:7px 12px;display:flex;justify-content:space-between}}
.url{{background:#f1f3f4;border-bottom:1px solid #ccc;padding:6px 12px;font:13px Consolas,monospace;color:#333}}
.tag{{color:#8ab4f8}}
pre{{margin:0;padding:14px 16px;background:#0c0c0c;color:#e6e6e6;font:13px/1.35 Consolas,'DejaVu Sans Mono',monospace;white-space:pre-wrap;word-break:break-all}}
.cmd{{color:#f9f1a5}} .err{{color:#ff6b6b}} .ok{{color:#7ee787}}
iframe,.content{{border:0;width:100%}}
.json{{padding:16px;font:14px/1.5 Consolas,'DejaVu Sans Mono',monospace;white-space:pre;background:#fff;color:#111}}
</style></head><body><div class="win">{cuerpo}</div></body></html>"""


def barra(titulo, ahora=None):
    ahora = ahora or datetime.now(ZoneInfo("America/Bogota")).strftime("%Y-%m-%d %H:%M:%S")
    return f'<div class="bar"><span>{html.escape(titulo)}</span><span class="tag">{ahora}</span></div>'


def colorear(linea):
    texto = html.escape(linea)
    if any(p in linea for p in ("ERROR", "Error", "Exited", "error")):
        return f'<span class="err">{texto}</span>'
    if any(p in linea for p in ("TERMINADO", "PROCESADO |", "✓")):
        return f'<span class="ok">{texto}</span>'
    return texto


def terminales(pagina, carpeta):
    for txt in sorted(Path(carpeta).glob("*.txt")):
        lineas = txt.read_text(encoding="utf-8", errors="replace").splitlines()
        # línea 1: hora en que se ejecutó el comando; línea 2: comando; resto: salida real
        fecha, comando, salida = lineas[0], lineas[1], lineas[2:]
        cuerpo = (barra("Windows PowerShell - Laboratorio03_Pagos_Asincronos", fecha)
                  + "<pre>" + f'<span class="cmd">PS C:\\Laboratorio03_Pagos_Asincronos&gt; {html.escape(comando)}</span>\n'
                  + "\n".join(colorear(l) for l in salida) + "</pre>")
        pagina.set_viewport_size({"width": 1500, "height": 400})
        pagina.set_content(MARCO.format(cuerpo=cuerpo))
        png = txt.with_suffix(".png")
        pagina.locator(".win").screenshot(path=str(png))
        print("captura", png.name)


def rabbit(pagina, salida, titulo):
    pagina.set_viewport_size({"width": 1500, "height": 760})
    pagina.goto(RABBIT + "/")
    if pagina.locator("input[name=username]").count():
        pagina.fill("input[name=username]", "guest")
        pagina.fill("input[name=password]", "guest")
        pagina.click("input[type=submit], button[type=submit]")
    pagina.wait_for_selector("#tabs")
    pagina.goto(RABBIT + "/#/queues")
    pagina.wait_for_selector("table.list")
    time.sleep(1.5)
    pagina.screenshot(path=salida, clip={"x": 0, "y": 0, "width": 1500, "height": 480})
    print("captura", salida, titulo)


def recorte(pagina, bloque, ruta):
    """Fotografía un bloque de Swagger UI desde su encabezado hasta la respuesta real
    (omite la tabla de respuestas documentadas que viene debajo)."""
    desplazamiento = pagina.evaluate("window.scrollY")  # bounding_box es relativo al viewport
    arriba = bloque.bounding_box()
    vivo = bloque.locator(".live-responses-table").bounding_box()
    abajo = vivo["y"] + vivo["height"] + 12 + desplazamiento
    pagina.screenshot(path=ruta, full_page=True,
                      clip={"x": arriba["x"], "y": arriba["y"] + desplazamiento, "width": arriba["width"],
                            "height": abajo - arriba["y"] - desplazamiento})


def ejecutar(bloque):
    """Pulsa Execute y devuelve el cuerpo de la respuesta real que muestra Swagger UI."""
    bloque.locator("button.execute").click()
    respuesta = bloque.locator(".live-responses-table .response-col_description pre").first
    respuesta.wait_for()
    bloque.locator(".loading-container").wait_for(state="detached")
    time.sleep(0.3)
    return respuesta.inner_text()


def swagger(pagina, prefijo):
    """Punto 11 con Swagger UI (http://localhost:8090/docs): pago válido, pago inválido
    y consulta del mismo pago inmediatamente y pasados 10 segundos."""
    pagina.set_viewport_size({"width": 1400, "height": 1000})
    pagina.goto(API + "/docs")
    pagina.wait_for_selector(".opblock")
    time.sleep(1)
    pagina.screenshot(path=f"{prefijo}_documentacion.png", full_page=True)
    print("captura", f"{prefijo}_documentacion.png")

    post = pagina.locator("#operations-Pagos-registrar_pago_pagos_post")
    get = pagina.locator("#operations-Pagos-obtener_pago_pagos__pago_id__get")
    # El GET se deja listo antes de registrar, para poder consultar de inmediato.
    for bloque in (post, get):
        bloque.locator(".opblock-summary").first.click()
        bloque.locator("button.try-out__btn").click()

    pid = json.loads(ejecutar(post))["data"]["id"]
    get.locator("input[placeholder='pago_id']").fill(str(pid))
    t_get = time.time()
    antes = json.loads(ejecutar(get))
    recorte(pagina, post, f"{prefijo}_post_valido.png")
    recorte(pagina, get, f"{prefijo}_get_antes.png")
    print("captura GET inmediato:", antes["data"]["estado"])

    # Pago inválido: mismo endpoint con el ejemplo "Pago inválido"
    post.locator("select", has=pagina.locator("option", has_text="Pago inválido")).select_option(label="Pago inválido")
    time.sleep(0.5)
    ejecutar(post)
    recorte(pagina, post, f"{prefijo}_post_invalido.png")

    time.sleep(max(0.0, 10 - (time.time() - t_get)))
    despues = json.loads(ejecutar(get))
    recorte(pagina, get, f"{prefijo}_get_despues.png")
    print("captura GET a los 10 s:", despues["data"]["estado"])
    Path(f"{prefijo}_resultado.json").write_text(
        json.dumps({"pago_id": pid, "antes": antes, "despues": despues}, indent=2, ensure_ascii=False),
        encoding="utf-8")


def main():
    modo = sys.argv[1]
    with sync_playwright() as p:
        navegador = p.chromium.launch()
        pagina = navegador.new_page(timezone_id="America/Bogota", locale="es-CO")
        if modo == "rabbit":
            rabbit(pagina, sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "")
        elif modo == "swagger":
            swagger(pagina, sys.argv[2])
        elif modo == "terminales":
            terminales(pagina, sys.argv[2])
        navegador.close()


if __name__ == "__main__":
    main()
