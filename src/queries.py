# ============================================================
# QUERY BASE - NO MODIFICAR
# ============================================================
QUERY_RECHAZOS = """
SELECT
*
FROM facturas
JOIN pagos ON facturas.id_pago = pagos.id
JOIN servicios ON facturas.id_servicio = servicios.id
JOIN contactos ON servicios.id_contacto = contactos.id
WHERE servicios.id_estado NOT IN(7, 8, 9, 10, 15)
  AND pagos.id_tipo_fuente = %s
GROUP BY contactos.id_tel_fijo1
"""

# ============================================================
# Tabla auxiliar para control de duplicados
# ============================================================
SQL_CREATE_PROCESSED_TABLE = """
CREATE TABLE IF NOT EXISTS rechazos_preventivos_procesados (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_pago INT NOT NULL,
    id_factura INT NOT NULL,
    id_servicio INT NOT NULL,
    id_tipo_fuente INT NOT NULL,
    periodo CHAR(6) NOT NULL,
    campaign_name VARCHAR(100) NOT NULL,
    fecha_procesado DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_rechazo (id_pago, id_factura, id_servicio)
)
"""

# ============================================================
# Insertar procesados (ignora duplicados por UNIQUE KEY)
# ============================================================
SQL_INSERT_PROCESSED = """
INSERT IGNORE INTO rechazos_preventivos_procesados
    (id_pago, id_factura, id_servicio, id_tipo_fuente, periodo, campaign_name)
VALUES
    (%s, %s, %s, %s, %s, %s)
"""

# ============================================================
# Verificar si un rechazo ya fue procesado
# ============================================================
SQL_CHECK_PROCESSED = """
SELECT id FROM rechazos_preventivos_procesados
WHERE id_pago = %s AND id_factura = %s AND id_servicio = %s
"""
