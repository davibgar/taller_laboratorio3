FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY medir.py registrar.py ./
CMD ["python", "medir.py", "--n", "20", "--etiqueta", "medicion"]
