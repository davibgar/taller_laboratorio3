"""
Devuelve a la cola principal (pagos.registrados) los mensajes que quedaron en
la cola de fallidos (pagos.fallidos). Uso, con el entorno levantado:

    docker compose exec consumer python reprocesar_fallidos.py

Cada mensaje se republica como persistente y sólo después se confirma en la
cola de fallidos, de modo que tampoco aquí se pierde ninguno.
"""
import pika

from consumer import DLQ, QUEUE, RABBIT_HOST, RABBIT_PASSWORD, RABBIT_USER, declarar_topologia


def main():
    parametros = pika.ConnectionParameters(
        host=RABBIT_HOST, credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
    )
    conexion = pika.BlockingConnection(parametros)
    canal = conexion.channel()
    canal.confirm_delivery()
    declarar_topologia(canal)
    movidos = 0
    while True:
        metodo, propiedades, cuerpo = canal.basic_get(queue=DLQ, auto_ack=False)
        if metodo is None:
            break
        canal.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=cuerpo,
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
        canal.basic_ack(delivery_tag=metodo.delivery_tag)
        movidos += 1
        print(f"Reencolado: {cuerpo.decode()}")
    print(f"Mensajes devueltos de {DLQ} a {QUEUE}: {movidos}")
    conexion.close()


if __name__ == "__main__":
    main()
