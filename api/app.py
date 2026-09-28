"""
API de pagos - Taller de Laboratorio N.º 3
Autores: David Felipe Garcia Ortiz - Juan David Claros

Flujo del POST /pagos:
  1. Valida los datos (si no son válidos responde status=false y termina).
  2. Inserta el pago con estado REGISTRADO y hace COMMIT.
  3. Publica en la cola un mensaje con el id del pago.
  4. Responde de inmediato. NO espera al consumidor.

Documentación Swagger (OpenAPI) generada por FastAPI:
  Swagger UI : http://localhost:8090/docs
  OpenAPI    : http://localhost:8090/openapi.json
"""
import asyncio
import json
import logging
import math
import os
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional

import aio_pika
import aiomysql
from fastapi import FastAPI, Path, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [API] %(message)s")
log = logging.getLogger("api")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "mysql"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "pagos_user"),
    "password": os.getenv("DB_PASSWORD", "pagos_pass"),
    "db": os.getenv("DB_NAME", "pagos_db"),
}
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE = os.getenv("QUEUE_NAME", "pagos.registrados")
DLX = os.getenv("DLX_NAME", "pagos.dlx")
DLQ = os.getenv("DLQ_NAME", "pagos.fallidos")
MEDIOS_VALIDOS = ["transferencia", "tarjeta", "efectivo", "pse", "nequi"]
REINTENTOS = int(os.getenv("CONNECT_RETRIES", "40"))
ESPERA_REINTENTO = float(os.getenv("CONNECT_RETRY_SECONDS", "3"))

estado = {"pool": None, "conexion": None, "canal": None}


# ---------------------------------------------------------------------------
# Modelos: sólo documentan el contrato en Swagger. La validación del cuerpo se
# hace a mano en validar() para responder SIEMPRE con el formato del taller
# ({"status": false, "message": "Datos del pago inválidos"}) y no con el 422
# genérico de FastAPI.
# ---------------------------------------------------------------------------
class PagoEntrada(BaseModel):
    referencia: str = Field(..., min_length=1, max_length=50, description="Referencia del pago",
                            examples=["PAG-0001"])
    valor: float = Field(..., gt=0, le=999_999_999_999, description="Valor del pago (número mayor que 0)",
                         examples=[125000])
    medio: Literal["transferencia", "tarjeta", "efectivo", "pse", "nequi"] = Field(
        ..., description="Medio de pago", examples=["transferencia"])


class PagoRegistrado(BaseModel):
    id: int = Field(examples=[1])
    estado: Literal["REGISTRADO"] = "REGISTRADO"


class RespuestaRegistro(BaseModel):
    status: bool = Field(examples=[True], description="Booleano, no cadena de texto")
    message: str = Field(examples=["Pago registrado"])
    data: PagoRegistrado


class RespuestaError(BaseModel):
    status: bool = Field(examples=[False], description="Booleano, no cadena de texto")
    message: str = Field(examples=["Datos del pago inválidos"])


class Procesamiento(BaseModel):
    id: int
    fecha_toma: Optional[str] = Field(None, description="Hora en que el consumidor tomó el mensaje de la cola")
    fecha_procesamiento: str = Field(description="Hora en que el consumidor terminó")
    resultado: str


class Pago(BaseModel):
    id: int
    referencia: str
    valor: float
    medio: str
    fecha_registro: str
    estado: Literal["REGISTRADO", "PROCESADO"]
    procesamiento: Optional[Procesamiento] = Field(None, description="null mientras el pago no se ha procesado")


class RespuestaPago(BaseModel):
    status: bool
    message: str
    data: Pago


class RespuestaPagos(BaseModel):
    status: bool
    message: str
    data: list[Pago]


EJEMPLO_REGISTRADO = {
    "status": True, "message": "Pago encontrado",
    "data": {"id": 1, "referencia": "PAG-0001", "valor": 125000, "medio": "transferencia",
             "fecha_registro": "2026-09-28T11:32:11.519", "estado": "REGISTRADO", "procesamiento": None},
}
EJEMPLO_PROCESADO = {
    "status": True, "message": "Pago encontrado",
    "data": {"id": 1, "referencia": "PAG-0001", "valor": 125000, "medio": "transferencia",
             "fecha_registro": "2026-09-28T11:32:11.519", "estado": "PROCESADO",
             "procesamiento": {"id": 1, "fecha_toma": "2026-09-28T11:32:11.569",
                               "fecha_procesamiento": "2026-09-28T11:32:16.596",
                               "resultado": "Comprobante generado: comprobante_000001.txt"}},
}


def ejemplo_error(message):
    return {"model": RespuestaError, "content": {"application/json": {"example": {"status": False, "message": message}}}}


# ---------------------------------------------------------------------------
# Conexiones
# ---------------------------------------------------------------------------
async def con_reintentos(nombre, fabrica):
    """MySQL y RabbitMQ tardan en arrancar: reintenta en vez de morir."""
    for intento in range(1, REINTENTOS + 1):
        try:
            recurso = await fabrica()
            log.info("Conectado a %s (intento %d)", nombre, intento)
            return recurso
        except Exception as exc:  # noqa: BLE001
            log.warning("%s no disponible (intento %d/%d): %s", nombre, intento, REINTENTOS, exc)
            await asyncio.sleep(ESPERA_REINTENTO)
    raise RuntimeError(f"No fue posible conectar a {nombre}")


async def declarar_topologia(canal):
    """Cola principal durable con dead-letter hacia pagos.fallidos.
    La misma declaración existe en el consumidor (debe coincidir)."""
    dlx = await canal.declare_exchange(DLX, aio_pika.ExchangeType.DIRECT, durable=True)
    dlq = await canal.declare_queue(DLQ, durable=True)
    await dlq.bind(dlx, routing_key=DLQ)
    await canal.declare_queue(
        QUEUE,
        durable=True,
        arguments={"x-dead-letter-exchange": DLX, "x-dead-letter-routing-key": DLQ},
    )


@asynccontextmanager
async def ciclo_de_vida(_app):
    estado["pool"] = await con_reintentos(
        "MySQL",
        lambda: aiomysql.create_pool(minsize=1, maxsize=10, autocommit=False,
                                     init_command="SET time_zone = '-05:00'", **DB_CONFIG),
    )
    estado["conexion"] = await con_reintentos("RabbitMQ", lambda: aio_pika.connect_robust(RABBIT_URL))
    # publisher_confirms=True: el publish espera la confirmación del broker,
    # así la API sólo responde cuando el mensaje quedó realmente encolado.
    estado["canal"] = await estado["conexion"].channel(publisher_confirms=True)
    await declarar_topologia(estado["canal"])
    yield
    await estado["conexion"].close()
    estado["pool"].close()
    await estado["pool"].wait_closed()


app = FastAPI(
    title="API de pagos - Laboratorio 03",
    version="1.0.0",
    description=(
        "Taller de Laboratorio N.º 3 - **Procesamiento asíncrono de eventos** (Optativa IV, FET).\n\n"
        "Autores: **David Felipe Garcia Ortiz** y **Juan David Claros**.\n\n"
        "El `POST /pagos` guarda el pago en MySQL con estado `REGISTRADO`, publica su id en la cola "
        "`pagos.registrados` de RabbitMQ y **responde de inmediato**, sin esperar al consumidor. "
        "El consumidor procesa el mensaje por su cuenta (≈5 s), genera un comprobante y deja el pago en `PROCESADO`.\n\n"
        "Para verlo: registre un pago, consúltelo con `GET /pagos/{id}` enseguida (REGISTRADO) "
        "y otra vez pasados 10 segundos (PROCESADO)."
    ),
    lifespan=ciclo_de_vida,
    openapi_tags=[{"name": "Pagos", "description": "Registro y consulta de pagos"},
                  {"name": "Salud", "description": "Estado de la API"}],
)


def openapi_sin_422():
    """La API nunca responde el 422 genérico de FastAPI (valida a mano), así que
    se quita de la documentación Swagger para que refleje el contrato real."""
    if app.openapi_schema is None:
        esquema = get_openapi(title=app.title, version=app.version, description=app.description,
                              routes=app.routes, tags=app.openapi_tags)
        for operaciones in esquema["paths"].values():
            for operacion in operaciones.values():
                operacion.get("responses", {}).pop("422", None)
        for nombre in ("HTTPValidationError", "ValidationError"):
            esquema.get("components", {}).get("schemas", {}).pop(nombre, None)
        app.openapi_schema = esquema
    return app.openapi_schema


app.openapi = openapi_sin_422


def respuesta(http_status, status, message, data=None):
    cuerpo = {"status": status, "message": message}
    if data is not None:
        cuerpo["data"] = data
    return JSONResponse(status_code=http_status, content=cuerpo)


def validar(cuerpo):
    """Devuelve (referencia, valor, medio) o None si los datos no son válidos."""
    if not isinstance(cuerpo, dict):
        return None
    referencia, valor, medio = cuerpo.get("referencia"), cuerpo.get("valor"), cuerpo.get("medio")
    if not isinstance(referencia, str) or not (1 <= len(referencia.strip()) <= 50):
        return None
    # bool es subclase de int en Python: se excluye explícitamente.
    if isinstance(valor, bool) or not isinstance(valor, (int, float)):
        return None
    if not math.isfinite(valor) or valor <= 0 or valor > 999_999_999_999:
        return None
    if not isinstance(medio, str) or medio.strip().lower() not in MEDIOS_VALIDOS:
        return None
    return referencia.strip(), valor, medio.strip().lower()


def a_json(valor):
    if isinstance(valor, datetime):
        return valor.isoformat(timespec="milliseconds")
    if isinstance(valor, (Decimal, float)):
        numero = float(valor)
        return int(numero) if numero.is_integer() else numero
    return valor


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post(
    "/pagos",
    tags=["Pagos"],
    summary="Registrar un pago",
    description=(
        "Valida el pago, lo inserta con estado `REGISTRADO`, publica `{\"pago_id\": id}` en la cola "
        "y responde **sin esperar el procesamiento**. Si los datos no son válidos responde "
        "`status: false` y no inserta ni publica nada.\n\n"
        f"Medios válidos: {', '.join(MEDIOS_VALIDOS)}."
    ),
    status_code=201,
    response_model=RespuestaRegistro,
    responses={
        201: {"description": "Pago registrado y encolado",
              "content": {"application/json": {"example": {"status": True, "message": "Pago registrado",
                                                           "data": {"id": 1, "estado": "REGISTRADO"}}}}},
        400: {"description": "Datos del pago inválidos", **ejemplo_error("Datos del pago inválidos")},
        503: {"description": "Base de datos o cola no disponibles", **ejemplo_error("No fue posible encolar el pago")},
    },
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {
                "schema": PagoEntrada.model_json_schema(),
                "examples": {
                    "valido": {"summary": "Pago válido",
                               "value": {"referencia": "PAG-0001", "valor": 125000, "medio": "transferencia"}},
                    "invalido": {"summary": "Pago inválido",
                                 "value": {"referencia": "", "valor": -125000, "medio": "bitcoin"}},
                },
            }},
        }
    },
)
async def registrar_pago(request: Request):
    try:
        cuerpo = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        cuerpo = None
    datos = validar(cuerpo)
    if datos is None:
        # Datos inválidos: no se inserta ni se publica nada.
        return respuesta(400, False, "Datos del pago inválidos")
    referencia, valor, medio = datos

    # 1) Guardar primero (el consumidor nunca debe buscar un pago que no existe).
    try:
        async with estado["pool"].acquire() as con:
            async with con.cursor() as cur:
                await cur.execute(
                    "INSERT INTO pagos (referencia, valor, medio_pago, estado) VALUES (%s, %s, %s, 'REGISTRADO')",
                    (referencia, valor, medio),
                )
                pago_id = cur.lastrowid
            await con.commit()
    except Exception as exc:  # noqa: BLE001
        log.error("Error guardando el pago: %s", exc)
        return respuesta(503, False, "No fue posible registrar el pago")

    # 2) Publicar después del COMMIT, mensaje persistente con el id del pago.
    try:
        mensaje = aio_pika.Message(
            body=json.dumps({"pago_id": pago_id}).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await estado["canal"].default_exchange.publish(mensaje, routing_key=QUEUE)
    except Exception as exc:  # noqa: BLE001
        # Compensación: si no se pudo encolar, se elimina el registro para no
        # dejar un pago REGISTRADO que nunca se procesaría.
        log.error("Error publicando el pago %s: %s", pago_id, exc)
        async with estado["pool"].acquire() as con:
            async with con.cursor() as cur:
                await cur.execute("DELETE FROM pagos WHERE id = %s", (pago_id,))
            await con.commit()
        return respuesta(503, False, "No fue posible encolar el pago")

    log.info("Pago %s registrado y encolado (%s, %s, %s)", pago_id, referencia, valor, medio)
    # 3) Responder de inmediato: no se espera al consumidor.
    return respuesta(201, True, "Pago registrado", {"id": pago_id, "estado": "REGISTRADO"})


async def consultar(sql, params=()):
    async with estado["pool"].acquire() as con:
        async with con.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            filas = await cur.fetchall()
        await con.commit()  # cierra la transacción de lectura (lecturas siempre frescas)
    return [{k: a_json(v) for k, v in fila.items()} for fila in filas]


SQL_PAGO = """
SELECT p.id, p.referencia, p.valor, p.medio_pago AS medio, p.fecha_registro, p.estado,
       pr.id AS procesamiento_id, pr.fecha_toma, pr.fecha_procesamiento, pr.resultado
FROM pagos p
LEFT JOIN procesamientos pr ON pr.pago_id = p.id
"""


def armar_pago(fila):
    pago = {k: fila[k] for k in ("id", "referencia", "valor", "medio", "fecha_registro", "estado")}
    pago["procesamiento"] = None
    if fila["procesamiento_id"] is not None:
        pago["procesamiento"] = {
            "id": fila["procesamiento_id"],
            "fecha_toma": fila["fecha_toma"],
            "fecha_procesamiento": fila["fecha_procesamiento"],
            "resultado": fila["resultado"],
        }
    return pago


@app.get(
    "/pagos/{pago_id}",
    tags=["Pagos"],
    summary="Consultar un pago por su identificador",
    description="Devuelve el pago con su estado actual. `procesamiento` es `null` mientras el consumidor no lo ha procesado.",
    response_model=RespuestaPago,
    responses={
        200: {"description": "Pago encontrado",
              "content": {"application/json": {"examples": {
                  "registrado": {"summary": "Recién registrado", "value": EJEMPLO_REGISTRADO},
                  "procesado": {"summary": "Ya procesado", "value": EJEMPLO_PROCESADO}}}}},
        400: {"description": "Identificador inválido", **ejemplo_error("Identificador de pago inválido")},
        404: {"description": "Pago no encontrado", **ejemplo_error("Pago no encontrado")},
    },
)
async def obtener_pago(pago_id: str = Path(..., description="Identificador del pago", examples=["1"])):
    if not pago_id.isdigit():
        return respuesta(400, False, "Identificador de pago inválido")
    filas = await consultar(SQL_PAGO + " WHERE p.id = %s", (int(pago_id),))
    if not filas:
        return respuesta(404, False, "Pago no encontrado")
    return respuesta(200, True, "Pago encontrado", armar_pago(filas[0]))


@app.get("/pagos", tags=["Pagos"], summary="Listar los últimos 100 pagos", response_model=RespuestaPagos)
async def listar_pagos():
    filas = await consultar(SQL_PAGO + " ORDER BY p.id DESC LIMIT 100")
    return respuesta(200, True, "Pagos encontrados", [armar_pago(f) for f in filas])


@app.get("/health", tags=["Salud"], summary="Estado de la API", response_model=RespuestaError)
async def salud():
    return respuesta(200, True, "API disponible")
