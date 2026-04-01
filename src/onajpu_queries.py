# ============================================================
# ONAJPU – Queries y DDL
# ============================================================

SQL_CREATE_ONAJPU_STAGING = """
CREATE TABLE IF NOT EXISTS onajpu_staging (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    cedula          VARCHAR(20)  DEFAULT NULL,
    id_servicio     INT          DEFAULT NULL,
    estado          VARCHAR(20)  NOT NULL DEFAULT 'PENDIENTE',
    mensaje_error   TEXT         DEFAULT NULL,
    fecha_importacion DATETIME   DEFAULT CURRENT_TIMESTAMP,
    procesado_en    DATETIME     DEFAULT NULL,
    id_contacto     INT          DEFAULT NULL,
    id_campaign     INT          DEFAULT NULL,
    id_gestion      INT          DEFAULT NULL,
    origen_archivo  VARCHAR(255) DEFAULT NULL,
    INDEX idx_estado (estado)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# ── Lectura de pendientes ────────────────────────────────────
SQL_ONAJPU_PENDIENTES = """
SELECT id, cedula, id_servicio
FROM onajpu_staging
WHERE estado = 'PENDIENTE'
ORDER BY id
"""

# ── Resolución id_contacto por id_servicio ───────────────────
SQL_CONTACTO_POR_SERVICIO = """
SELECT s.id_contacto, c.id_tel_fijo1
FROM servicios s
JOIN contactos c ON c.id = s.id_contacto
WHERE s.id = %s
  AND s.id_estado NOT IN (7, 8, 9, 10, 15)
LIMIT 1
"""

# ── Resolución id_contacto por cédula ────────────────────────
SQL_CONTACTO_POR_CEDULA = """
SELECT s.id_contacto, s.id AS id_servicio_encontrado, c.id_tel_fijo1
FROM contactos c
JOIN servicios s ON s.id_contacto = c.id
WHERE c.ci = %s
  AND s.id_estado NOT IN (7, 8, 9, 10, 15)
ORDER BY s.id DESC
LIMIT 1
"""

# ── Marcar fila como procesada ───────────────────────────────
SQL_MARCAR_PROCESADO = """
UPDATE onajpu_staging
SET estado       = 'PROCESADO',
    id_contacto  = %s,
    id_campaign  = %s,
    id_gestion   = %s,
    procesado_en = NOW(),
    mensaje_error = NULL
WHERE id = %s
"""

# ── Marcar fila como error ───────────────────────────────────
SQL_MARCAR_ERROR = """
UPDATE onajpu_staging
SET estado        = 'ERROR',
    mensaje_error = %s,
    procesado_en  = NOW()
WHERE id = %s
"""
