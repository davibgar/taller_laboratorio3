-- =============================================================
-- Taller de Laboratorio N.º 3 - Procesamiento asíncrono de eventos
-- Script de creación de las tablas pagos y procesamientos
-- Autores: David Felipe Garcia Ortiz - Juan David Claros
-- MySQL 8.x lo ejecuta automáticamente la primera vez que se crea
-- el volumen de datos (/docker-entrypoint-initdb.d).
-- =============================================================

CREATE DATABASE IF NOT EXISTS pagos_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE pagos_db;

-- Pago registrado por la API. Nace en REGISTRADO y el consumidor
-- lo pasa a PROCESADO cuando termina la acción de procesamiento.
CREATE TABLE IF NOT EXISTS pagos (
  id              INT UNSIGNED  NOT NULL AUTO_INCREMENT,
  referencia      VARCHAR(50)   NOT NULL,
  valor           DECIMAL(14,2) NOT NULL,
  medio_pago      VARCHAR(30)   NOT NULL,
  fecha_registro  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  estado          ENUM('REGISTRADO', 'PROCESADO') NOT NULL DEFAULT 'REGISTRADO',
  PRIMARY KEY (id),
  INDEX idx_pagos_estado (estado),
  CONSTRAINT chk_pagos_valor CHECK (valor > 0)
) ENGINE = InnoDB;

-- Registro de cada acción de procesamiento ejecutada por el consumidor.
-- fecha_toma: hora en que el consumidor tomó el mensaje de la cola.
-- fecha_procesamiento: hora en que terminó el procesamiento.
CREATE TABLE IF NOT EXISTS procesamientos (
  id                   INT UNSIGNED NOT NULL AUTO_INCREMENT,
  pago_id              INT UNSIGNED NOT NULL,
  fecha_toma           DATETIME(3)  NULL,
  fecha_procesamiento  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  resultado            VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_procesamientos_pago (pago_id),
  CONSTRAINT fk_procesamientos_pago FOREIGN KEY (pago_id) REFERENCES pagos (id)
) ENGINE = InnoDB;
