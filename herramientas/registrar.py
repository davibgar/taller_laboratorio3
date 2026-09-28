"""
Registra un único pago y muestra la respuesta de la API.

    docker compose run --rm herramientas python registrar.py FALLA-0001 [valor] [medio]
"""
import sys
import time

from medir import peticion


def main():
    referencia = sys.argv[1] if len(sys.argv) > 1 else "PAG-0001"
    valor = float(sys.argv[2]) if len(sys.argv) > 2 else 125000
    medio = sys.argv[3] if len(sys.argv) > 3 else "transferencia"
    cuerpo = {"referencia": referencia, "valor": valor, "medio": medio}
    codigo, resp, ms = peticion("POST", "/pagos", cuerpo)
    print(f"{time.strftime('%H:%M:%S')} POST /pagos {cuerpo}")
    print(f"HTTP {codigo} en {ms:.2f} ms -> {resp}")


if __name__ == "__main__":
    main()
