"""
Consumidor de pagos - Taller de Laboratorio N.º 3
Autores: David Felipe Garcia Ortiz - Juan David Claros

Por cada mensaje de la cola pagos.registrados:
  1. Registra en el log la hora en que lo tomó de la cola.
  2. Ejecuta la acción de procesamiento: genera un comprobante de pago
     (archivo de texto) tardando deliberadamente PROCESS_SECONDS segundos.
  3. En una sola transacción: pasa el pago a PROCESADO e inserta la fila
     en procesamientos.
  4. Registra en el log la hora en que terminó.
  5. Sólo entonces confirma (ack) el mensaje. Si algo falla antes, el mensaje
     NO se confirma: se rechaza hacia la cola pagos.fallidos (dead-letter) y
     el pago sigue en REGISTRADO. Si el proceso muere a mitad, RabbitMQ
     devuelve el mensaje a la cola porque nunca recibió el ack.
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pika
import pymysql

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CONSUMIDOR] %(message)s")
log = logging.getLogger("consumidor")
logging.getLogger("pika").setLevel(logging.WARNING)

ZONA = ZoneInfo(os.getenv("APP_TZ", "America/Bogota"))
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "mysql"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "pagos_user"),
    "password": os.getenv("DB_PASSWORD", "pagos_pass"),
    "database": os.getenv("DB_NAME", "pagos_db"),
    "autocommit": False,
    "init_command": "SET time_zone = '-05:00'",
}
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASSWORD = os.getenv("RABBIT_PASSWORD", "guest")
QUEUE = os.getenv("QUEUE_NAME", "pagos.registrados")
DLX = os.getenv("DLX_NAME", "pagos.dlx")
DLQ = os.getenv("DLQ_NAME", "pagos.fallidos")
PROCESS_SECONDS = float(os.getenv("PROCESS_SECONDS", "5"))
SIMULAR_FALLO = os.getenv("SIMULAR_FALLO", "false").lower() == "true"
PREFIJO_FALLO = os.getenv("PREFIJO_FALLO", "FALLA")
DIR_COMPROBANTES = Path(os.getenv("DIR_COMPROBANTES", "/app/comprobantes"))
REINTENTOS = int(os.getenv("CONNECT_RETRIES", "40"))
ESPERA_REINTENTO = float(os.getenv("CONNECT_RETRY_SECONDS", "3"))


def ahora():
    return datetime.now(ZONA)


def hora(dt):
    return dt.strftime("%H:%M:%S.%f")[:-3]


def con_reintentos(nombre, fabrica):
    """MySQL y RabbitMQ tardan en arrancar: reintenta en vez de terminar con error."""
    for intento in range(1, REINTENTOS + 1):
        try:
            recurso = fabrica()
            log.info("Conectado a %s (intento %d)", nombre, intento)
            return recurso
        except Exception as exc:  # noqa: BLE001
            log.warning("%s no disponible (intento %d/%d): %s", nombre, intento, REINTENTOS, exc)
            time.sleep(ESPERA_REINTENTO)
    raise RuntimeError(f"No fue posible conectar a {nombre}")


def declarar_topologia(canal):
    """Debe coincidir con la declaración de la API."""
    canal.exchange_declare(exchange=DLX, exchange_type="direct", durable=True)
    canal.queue_declare(queue=DLQ, durable=True)
    canal.queue_bind(queue=DLQ, exchange=DLX, routing_key=DLQ)
    canal.queue_declare(
        queue=QUEUE,
        durable=True,
        arguments={"x-dead-letter-exchange": DLX, "x-dead-letter-routing-key": DLQ},
    )


class Consumidor:
    def __init__(self):
        self.db = con_reintentos("MySQL", lambda: pymysql.connect(**DB_CONFIG))
        DIR_COMPROBANTES.mkdir(parents=True, exist_ok=True)

    def cursor(self):
        self.db.ping(reconnect=True)
        return self.db.cursor(pymysql.cursors.DictCursor)

    def accion_procesamiento(self, pago):
        """Acción elegida: generar el comprobante del pago en un archivo.
        Tarda deliberadamente PROCESS_SECONDS segundos."""
        mitad = PROCESS_SECONDS / 2
        time.sleep(mitad)
        if SIMULAR_FALLO and pago["referencia"].upper().startswith(PREFIJO_FALLO):
            # Error provocado a mitad del procesamiento (caso 3 del punto 9).
            raise RuntimeError(f"Fallo simulado procesando la referencia {pago['referencia']}")
        time.sleep(PROCESS_SECONDS - mitad)
        archivo = DIR_COMPROBANTES / f"comprobante_{pago['id']:06d}.txt"
        archivo.write_text(
            "COMPROBANTE DE PAGO\n"
            f"Pago N.º      : {pago['id']}\n"
            f"Referencia    : {pago['referencia']}\n"
            f"Valor         : $ {pago['valor']:,.2f}\n"
            f"Medio de pago : {pago['medio_pago']}\n"
            f"Registrado    : {pago['fecha_registro']}\n"
            f"Emitido       : {ahora().isoformat(timespec='milliseconds')}\n",
            encoding="utf-8",
        )
        return f"Comprobante generado: {archivo.name}"

    def procesar(self, canal, metodo, propiedades, cuerpo):
        tomado = ahora()
        try:
            pago_id = int(json.loads(cuerpo)["pago_id"])
        except Exception:  # noqa: BLE001
            log.error("Mensaje mal formado %r: se envía a %s", cuerpo, DLQ)
            canal.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
            return

        log.info("Pago %s | TOMADO de la cola a las %s (redelivered=%s)", pago_id, hora(tomado), metodo.redelivered)
        try:
            with self.cursor() as cur:
                cur.execute("SELECT * FROM pagos WHERE id = %s", (pago_id,))
                pago = cur.fetchone()
            self.db.commit()
            if pago is None:
                raise LookupError(f"El pago {pago_id} no existe")
            if pago["estado"] == "PROCESADO":
                # Idempotencia: un mensaje reenviado de un pago ya procesado se descarta.
                log.info("Pago %s | ya estaba PROCESADO, se confirma sin repetir la acción", pago_id)
                canal.basic_ack(delivery_tag=metodo.delivery_tag)
                return

            resultado = self.accion_procesamiento(pago)

            with self.cursor() as cur:
                cur.execute(
                    "UPDATE pagos SET estado = 'PROCESADO' WHERE id = %s AND estado = 'REGISTRADO'",
                    (pago_id,),
                )
                cur.execute(
                    "INSERT INTO procesamientos (pago_id, fecha_toma, fecha_procesamiento, resultado) "
                    "VALUES (%s, %s, NOW(3), %s)",
                    (pago_id, tomado.replace(tzinfo=None), resultado),
                )
            self.db.commit()
        except Exception as exc:  # noqa: BLE001
            self.db.rollback()
            log.error("Pago %s | ERROR a las %s: %s -> mensaje NO confirmado, enviado a %s; el pago sigue REGISTRADO",
                      pago_id, hora(ahora()), exc, DLQ)
            canal.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
            return

        terminado = ahora()
        # El ack va al FINAL: sólo ahora el mensaje sale de la cola.
        canal.basic_ack(delivery_tag=metodo.delivery_tag)
        log.info("Pago %s | TERMINADO a las %s (tomado %s, duración %.3f s) -> PROCESADO | %s",
                 pago_id, hora(terminado), hora(tomado), (terminado - tomado).total_seconds(), resultado)

    def iniciar(self):
        credenciales = pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
        parametros = pika.ConnectionParameters(host=RABBIT_HOST, credentials=credenciales, heartbeat=120)
        conexion = con_reintentos("RabbitMQ", lambda: pika.BlockingConnection(parametros))
        canal = conexion.channel()
        declarar_topologia(canal)
        canal.basic_qos(prefetch_count=1)  # un mensaje a la vez
        canal.basic_consume(queue=QUEUE, on_message_callback=self.procesar, auto_ack=False)
        log.info("Esperando mensajes en '%s' | PROCESS_SECONDS=%s | SIMULAR_FALLO=%s",
                 QUEUE, PROCESS_SECONDS, SIMULAR_FALLO)
        canal.start_consuming()


def detener(_senal, _marco):
    """docker compose stop envía SIGTERM. Un mensaje a medio procesar no alcanza
    a confirmarse, así que RabbitMQ lo devuelve a la cola."""
    log.info("SIGTERM recibido: el consumidor se detiene")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, detener)
    Consumidor().iniciar()
