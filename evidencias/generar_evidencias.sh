#!/usr/bin/env bash
# =====================================================================
# Ejecuta de principio a fin la medición (punto 8), los tres casos
# (punto 9) y la verificación con Swagger UI (punto 11), guardando la salida
# real de cada comando y las capturas en evidencias/capturas.
# Sólo usa Docker. Ejecutar desde la raíz del proyecto:
#     bash evidencias/generar_evidencias.sh
# ADVERTENCIA: reinicia el entorno (docker compose down -v) para partir de cero.
# =====================================================================
set -u
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
RAIZ=$(pwd -W 2>/dev/null || pwd)
OUT=evidencias/capturas
PW_IMG=lab03-captura
RED=lab03-pagos_default
rm -rf "$OUT" resultados comprobantes
mkdir -p "$OUT" resultados

paso() { echo; echo "===== $(date +%H:%M:%S) $* ====="; }

# guarda "comando mostrado" + salida real en un .txt (luego se convierte en imagen)
cap() {
  local nombre=$1 mostrado=$2; shift 2
  { date "+%Y-%m-%d %H:%M:%S"; echo "$mostrado"; "$@" 2>&1; } > "$OUT/$nombre.txt"
  tail -n +2 "$OUT/$nombre.txt"
}

sql_pagos() { # $1 = filtro de referencia (LIKE)
  local q="SELECT p.id, p.referencia, p.valor, p.medio_pago, p.fecha_registro, p.estado, pr.id AS proc_id, pr.fecha_toma, pr.fecha_procesamiento FROM pagos p LEFT JOIN procesamientos pr ON pr.pago_id = p.id WHERE p.referencia LIKE '$1' ORDER BY p.id;"
  cap "$2" "docker compose exec mysql mysql -t -u pagos_user -p pagos_db -e \"$q\"" \
    docker compose exec -T -e MYSQL_PWD=pagos_pass mysql mysql -t -upagos_user pagos_db -e "$q"
}

colas_cli() {
  cap "$1" "docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers" \
    docker compose exec -T rabbitmq rabbitmqctl list_queues -q name messages_ready messages_unacknowledged consumers
}

colas_ui() { # captura de la consola web de RabbitMQ (espera a que refresque sus estadísticas)
  sleep "${2:-6}"
  docker run --rm --network "$RED" -v "$RAIZ/evidencias:/ev" "$PW_IMG" \
    python captura.py rabbit "/ev/capturas/$1.png"
}

log_consumidor() {
  cap "$1" "docker compose logs consumer --tail ${2:-40}" docker compose logs consumer --no-color --tail "${2:-40}"
}

ps_servicios() {
  cap "$1" "docker compose ps -a" docker compose ps -a --format "table {{.Name}}\t{{.Service}}\t{{.Status}}\t{{.Ports}}"
}

herr() { docker compose run --rm -T herramientas "$@"; }

esperar_api() {
  for _ in $(seq 1 60); do
    docker compose exec -T api python -c "import urllib.request;urllib.request.urlopen('http://localhost:8000/health')" >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo "La API no respondió"; exit 1
}

# ---------------------------------------------------------------------
paso "Entorno desde cero con un único comando"
docker compose --profile herramientas down -v --remove-orphans >/dev/null 2>&1
cap 00_docker_compose_up "docker compose up -d --build" docker compose up -d --build
esperar_api
docker compose --profile herramientas build herramientas >/dev/null 2>&1
docker build -q -t "$PW_IMG" evidencias/herramientas_captura >/dev/null
ps_servicios 01_servicios_arriba

# ---------------------------------------------------------------------
paso "Punto 11 - Documentación y verificación con Swagger UI (http://localhost:8090/docs)"
docker compose exec -T api python -c "import json,urllib.request;print(json.dumps(json.load(urllib.request.urlopen('http://localhost:8000/openapi.json')),indent=2,ensure_ascii=False))" > swagger/openapi.json
docker run --rm --network "$RED" -v "$RAIZ/evidencias:/ev" "$PW_IMG"   python captura.py swagger /ev/capturas/03_swagger
sleep 3

# ---------------------------------------------------------------------
paso "Punto 8 - Medición con 20 pagos (acción de 5 s)"
( cap 10_medicion_5s "docker compose run --rm herramientas python medir.py --n 20 --prefijo MED5 --etiqueta medicion_5s" \
    herr python medir.py --n 20 --prefijo MED5 --etiqueta medicion_5s > /dev/null ) &
MED=$!
sleep 4
colas_ui 11_medicion_5s_cola_durante 1
sql_pagos 'MED5%' 12_medicion_5s_tabla_durante
wait $MED; cat "$OUT/10_medicion_5s.txt" | tail -12
sql_pagos 'MED5%' 13_medicion_5s_tabla_final
log_consumidor 14_medicion_5s_log_consumidor 42

# ---------------------------------------------------------------------
paso "Caso 1 - Consumidor detenido"
cap 20_caso1_detener_consumidor "docker compose stop consumer" docker compose stop consumer
ps_servicios 21_caso1_servicios
cap 22_caso1_registro_5_pagos "docker compose run --rm herramientas python medir.py --n 5 --prefijo DET --no-esperar --etiqueta caso1_consumidor_detenido" \
  herr python medir.py --n 5 --prefijo DET --no-esperar --etiqueta caso1_consumidor_detenido
colas_cli 23_caso1_cola_cli_detenido
colas_ui 24_caso1_cola_detenido
sql_pagos 'DET%' 25_caso1_tabla_registrado
cap 26_caso1_levantar_consumidor "docker compose start consumer" docker compose start consumer
sleep 34
log_consumidor 27_caso1_log_consumidor 14
sql_pagos 'DET%' 28_caso1_tabla_procesado
colas_cli 29_caso1_cola_cli_final
colas_ui 29_caso1_cola_final 1

# ---------------------------------------------------------------------
paso "Caso 2 - Acción más lenta (15 s)"
PROCESS_SECONDS=15 docker compose up -d consumer
sleep 4
log_consumidor 30_caso2_consumidor_15s 3
( cap 31_medicion_15s "docker compose run --rm herramientas python medir.py --n 20 --prefijo LENTO --etiqueta medicion_15s" \
    herr python medir.py --n 20 --prefijo LENTO --etiqueta medicion_15s > /dev/null ) &
MED=$!
sleep 8
colas_ui 32_caso2_cola_durante 1
colas_cli 33_caso2_cola_cli_durante
sql_pagos 'LENTO%' 34_caso2_tabla_durante
wait $MED; tail -12 "$OUT/31_medicion_15s.txt"
sql_pagos 'LENTO%' 35_caso2_tabla_final
log_consumidor 36_caso2_log_consumidor 42

# ---------------------------------------------------------------------
paso "Caso 3 - Fallo al procesar"
SIMULAR_FALLO=true docker compose up -d consumer
sleep 4
log_consumidor 40_caso3_consumidor_con_fallo 3
cap 41_caso3_registro_pago_falla "docker compose run --rm herramientas python registrar.py FALLA-0001" \
  herr python registrar.py FALLA-0001
sleep 5
log_consumidor 42_caso3_log_error 4
colas_cli 43_caso3_cola_cli
colas_ui 44_caso3_cola_fallidos 2
sql_pagos 'FALLA%' 45_caso3_tabla_registrado

paso "Caso 3b - El consumidor muere a mitad del procesamiento"
cap 46_caso3b_registro_pago "docker compose run --rm herramientas python registrar.py CAIDA-0001" \
  herr python registrar.py CAIDA-0001
# sin pausas: hay que matar al consumidor antes de que termine sus 5 s
colas_cli 47_caso3b_cola_cli_procesando
cap 48_caso3b_matar_consumidor "docker kill lab03-consumer" docker kill lab03-consumer
log_consumidor 49_caso3b_log_consumidor_muerto 4
colas_cli 50_caso3b_cola_cli_tras_caida
colas_ui 51_caso3b_cola_tras_caida
sql_pagos 'CAIDA%' 52_caso3b_tabla_registrado

paso "Caso 3 - Recuperación: se corrige el fallo y se reprocesan los mensajes"
docker compose up -d consumer
sleep 9
cap 53_caso3_reprocesar_fallidos "docker compose exec consumer python reprocesar_fallidos.py" \
  docker compose exec -T consumer python reprocesar_fallidos.py
sleep 9
log_consumidor 54_caso3_log_recuperacion 8
sql_pagos 'FALLA%' 55_caso3_tabla_falla_final
sql_pagos 'CAIDA%' 56_caso3_tabla_caida_final
colas_cli 57_caso3_cola_cli_final
colas_ui 58_caso3_cola_final 1

# ---------------------------------------------------------------------
paso "Convirtiendo las salidas de terminal en imágenes"
docker run --rm -v "$RAIZ/evidencias:/ev" "$PW_IMG" \
  python captura.py terminales /ev/capturas
paso "Listo. Evidencias en $OUT y resultados en resultados/"
