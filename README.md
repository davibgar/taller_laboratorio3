# Laboratorio 03: procesamiento asíncrono de pagos

**Fundación Escuela Tecnológica de Neiva “Jesús Oviedo Pérez”**
Optativa IV: Sistemas de Tiempo Real Distribuidos (2026-2)
Docente: Juan Carlos Polania Cortes
Estudiantes: **David Felipe Garcia Ortiz** y **Juan David Claros**

La API registra un pago en MySQL con estado `REGISTRADO`, publica su identificador en RabbitMQ y responde de inmediato. Un consumidor independiente toma el mensaje, genera un comprobante (tarda 5 s a propósito), deja el pago en `PROCESADO` y registra la acción en la tabla `procesamientos`.

```
Cliente ──POST /pagos──▶ API ──INSERT (REGISTRADO)──▶ MySQL
                          │ └──publish {pago_id}──▶ RabbitMQ [pagos.registrados]
   ◀── 201 inmediato ─────┘                                  │
                                                             ▼
                          Consumidor ◀── toma el mensaje ────┘
                           ├─ genera el comprobante (5 s)
                           ├─ UPDATE pagos → PROCESADO + INSERT procesamientos
                           └─ ack (sólo al final)   ─ si falla: nack → [pagos.fallidos]
```

## 1. Requisitos

Sólo **Docker** (Docker Desktop en Windows o Mac, o Docker Engine con el plugin Compose en Linux). En el equipo no se instala Python, MySQL, RabbitMQ ni ninguna librería: todo corre en contenedores.

Puertos que se publican en el equipo (se pueden cambiar con variables de entorno):

| Servicio | URL / puerto | Variable |
|---|---|---|
| API + Swagger UI | http://localhost:8090/docs | `API_HOST_PORT` |
| Consola de RabbitMQ | http://localhost:15672 (guest / guest) | `RABBIT_UI_PORT` |
| RabbitMQ AMQP | localhost:5672 | `RABBIT_AMQP_PORT` |
| MySQL | localhost:3307 (pagos_user / pagos_pass) | `MYSQL_HOST_PORT` |

## 2. Levantar todo el entorno (un único comando)

Desde la carpeta del proyecto:

```bash
docker compose up -d --build
```

Ese comando descarga las imágenes, construye la API y el consumidor, crea las tablas (`db/init.sql` se ejecuta la primera vez) y arranca los cuatro servicios. MySQL y RabbitMQ tardan unos segundos en quedar listos. El compose espera sus *healthchecks* y, además, la API y el consumidor reintentan la conexión cada 3 s en vez de terminar con error.

Comprobar el estado:

```bash
docker compose ps          # los cuatro servicios en "Up" (mysql y rabbitmq "healthy")
docker compose logs -f consumer
```

Si el puerto 8090, 3307, 5672 o 15672 ya está ocupado en su equipo, cámbielo al levantar el entorno. Por ejemplo, en PowerShell: `$env:API_HOST_PORT=8095; docker compose up -d --build`.

## 3. Probar la API con Swagger

Abra **http://localhost:8090/docs** en el navegador. Swagger UI documenta los endpoints, los esquemas y los ejemplos, y permite ejecutarlos con *Try it out*. La especificación exportada está en [`swagger/openapi.json`](swagger/openapi.json) (también en http://localhost:8090/openapi.json).

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/pagos` | Registra el pago y responde sin esperar el procesamiento |
| GET | `/pagos/{id}` | Consulta un pago y su procesamiento |
| GET | `/pagos` | Lista los últimos 100 pagos |
| GET | `/health` | Estado de la API |

Cuerpo de la petición: `{"referencia": "PAG-0001", "valor": 125000, "medio": "transferencia"}`. Los medios válidos son `transferencia`, `tarjeta`, `efectivo`, `pse` y `nequi`.

* Si el pago es válido responde **201** con `{"status": true, "message": "Pago registrado", "data": {"id": 1, "estado": "REGISTRADO"}}`.
* Si no es válido responde **400** con `{"status": false, "message": "Datos del pago inválidos"}`. En ese caso no inserta nada ni publica en la cola.

Verificación del punto 11:

1. En `POST /pagos` pulse *Try it out* y luego *Execute* con el ejemplo **Pago válido**. Anote el `id`.
2. En el mismo endpoint elija el ejemplo **Pago inválido** y pulse *Execute*. La respuesta es `status: false`.
3. En `GET /pagos/{pago_id}` escriba el `id` y pulse *Execute* de inmediato. El estado es **REGISTRADO**.
4. Espere 10 segundos y pulse *Execute* otra vez. El estado es **PROCESADO** y ya aparece el bloque `procesamiento`.

## 4. Ver la cola, la base de datos y el registro del consumidor

```bash
# Colas: mensajes listos, sin confirmar y consumidores
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers

# Tabla de pagos
docker compose exec mysql mysql -t -u pagos_user -ppagos_pass pagos_db -e "SELECT * FROM pagos; SELECT * FROM procesamientos;"

# Registro del consumidor (id del pago, hora en que lo tomó y hora en que terminó)
docker compose logs consumer
```

La consola web de RabbitMQ está en http://localhost:15672, pestaña *Queues and Streams*. Los comprobantes generados se guardan en `./comprobantes`.

## 5. Medición (punto 8)

La herramienta de medición también corre en un contenedor:

```bash
docker compose run --rm herramientas python medir.py --n 20 --prefijo MED --etiqueta medicion_5s
```

Envía 20 pagos seguidos, mide el tiempo de respuesta de cada POST, espera a que todos queden `PROCESADO` e imprime la tabla del punto 8. Los resultados se guardan en `resultados/medicion_5s.json`.

## 6. Casos del punto 9

**Caso 1: consumidor detenido**
```bash
docker compose stop consumer
docker compose run --rm herramientas python medir.py --n 5 --prefijo DET --no-esperar --etiqueta caso1
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready consumers   # 5 mensajes, 0 consumidores
docker compose start consumer                                                        # los 5 se procesan
```

**Caso 2: acción más lenta (15 s)**
```bash
# PowerShell:  $env:PROCESS_SECONDS=15; docker compose up -d consumer
PROCESS_SECONDS=15 docker compose up -d consumer
docker compose run --rm herramientas python medir.py --n 20 --prefijo LENTO --etiqueta medicion_15s
docker compose up -d consumer          # volver a 5 s (quite la variable antes en PowerShell: Remove-Item Env:PROCESS_SECONDS)
```

**Caso 3: fallo al procesar**
```bash
# PowerShell:  $env:SIMULAR_FALLO="true"; docker compose up -d consumer
SIMULAR_FALLO=true docker compose up -d consumer
docker compose run --rm herramientas python registrar.py FALLA-0001   # falla a mitad del procesamiento
docker compose logs consumer --tail 5                                  # ERROR: mensaje no confirmado
docker compose exec rabbitmq rabbitmqctl list_queues                   # el mensaje queda en pagos.fallidos
docker compose up -d consumer                                          # consumidor sin fallo simulado
docker compose exec consumer python reprocesar_fallidos.py             # se devuelve a la cola y se procesa
```
Otra forma de comprobarlo es matar al consumidor con `docker kill lab03-consumer` mientras procesa un pago. Como el *ack* no se envió, RabbitMQ devuelve el mensaje a `pagos.registrados` y se procesa cuando el consumidor vuelve a arrancar.

**Todo automático:** `bash evidencias/generar_evidencias.sh` reinicia el entorno desde cero, ejecuta la verificación con Swagger, la medición y los tres casos, y guarda la salida real de cada comando junto con las capturas en `evidencias/capturas/`. Requiere un intérprete bash, como Git Bash en Windows, y usa sólo Docker.

## 7. Apagar y limpiar

```bash
docker compose down        # detiene y elimina los contenedores (conserva los datos)
docker compose down -v     # además borra los volúmenes de MySQL y RabbitMQ (reinicio desde cero)
```

## 8. Estructura

```
docker-compose.yml          # los 4 servicios (+ herramientas de medición bajo el perfil "herramientas")
db/init.sql                 # script de creación de las tablas pagos y procesamientos
api/                        # API FastAPI (app.py, Dockerfile, requirements.txt), Swagger en /docs
consumer/                   # consumidor pika (consumer.py, reprocesar_fallidos.py, Dockerfile)
herramientas/               # medir.py y registrar.py (medición del punto 8)
swagger/openapi.json        # especificación OpenAPI exportada
evidencias/                 # script de evidencias y capturas
resultados/                 # JSON con las mediciones
Informe_Laboratorio03.docx  # informe: medición, casos y preguntas de cierre (también en PDF)
Informe_Laboratorio03.pdf
```
