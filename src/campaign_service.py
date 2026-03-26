import logging
from datetime import datetime
from collections import defaultdict
from db import execute_one, execute_query, execute_many, commit

logger = logging.getLogger(__name__)

# ============================================================
# Mapping id_tipo_fuente → nombre de medio de pago
# ============================================================
MEDIO_PAGO = {
    13: "visa",
    14: "master",
    15: "oca",
    16: "creditel",
    17: "cabal",
    18: "creditosdirectos",
    21: "passcard",
}

# Abreviatura para el campo "codigo" de campaigns
# Patrón observado: SCP10PC-2603  (PC=passcard, 2603=YYMM)
CODIGO_ABREV = {
    13: "VI",
    14: "MA",
    15: "OCA",
    16: "CR",
    17: "CAB",
    18: "CD",
    21: "PC",
}

# id_broker por medio de pago
# Ajustar si cada medio tiene un broker distinto
BROKER_ID = {
    13: 956,
    14: 956,
    15: 956,
    16: 956,
    17: 956,
    18: 956,
    21: 956,
}


def construir_nombre_campana(id_tipo_fuente, periodo):
    """
    Formato: sc-preventivo10[nombre_pago]-L[YYYYMM]
    Ejemplo: sc-preventivo10visa-L202603
    """
    nombre_pago = MEDIO_PAGO.get(id_tipo_fuente)
    if not nombre_pago:
        raise ValueError(f"id_tipo_fuente desconocido: {id_tipo_fuente}")
    return f"sc-preventivo10{nombre_pago}-L{periodo}"


def _construir_codigo(id_tipo_fuente, periodo):
    """
    Formato: SCP10[ABREV]-[YYMM]
    Ejemplo: SCP10PC-2603
    """
    abrev = CODIGO_ABREV.get(id_tipo_fuente, "XX")
    yymm = periodo[2:]  # "202603" → "2603"
    return f"SCP10{abrev}-{yymm}"


def agrupar_por_periodo(rows):
    """
    Agrupa los registros por periodo YYYYMM usando la fecha actual.
    Todos los rechazos del día se asignan al periodo del mes en curso.
    Retorna: dict { "YYYYMM": [row, ...] }
    """
    periodo_actual = datetime.now().strftime("%Y%m")
    grupos = defaultdict(list)
    for row in rows:
        grupos[periodo_actual].append(row)
    return dict(grupos)


def _ts():
    """Timestamp en formato YYYYMMDDHHMMSS que usa la tabla gestiones."""
    return datetime.now().strftime("%Y%m%d%H%M%S")


# ============================================================
# Integración con tablas reales: campaigns + gestiones
# ============================================================

def campaign_exists(conn, id_tipo_fuente, periodo):
    """
    Busca la campana por codigo exacto (nuestro formato).
    Si no encuentra, busca por nombre que contenga el medio de pago y el periodo YYMM.
    Esto cubre campanas creadas manualmente con formato distinto.
    Retorna: campaign_id (int) si existe, None si no existe.
    """
    codigo = _construir_codigo(id_tipo_fuente, periodo)
    yymm = periodo[2:]  # "202603" -> "2603"
    nombre_pago = MEDIO_PAGO.get(id_tipo_fuente, "")

    # Buscar por codigo exacto (nuestro formato)
    row = execute_one(conn, "SELECT id FROM campaigns WHERE codigo = %s", (codigo,))
    if row:
        logger.debug("Campana encontrada por codigo: id=%s codigo=%s", row["id"], codigo)
        return row["id"]

    # Buscar por nombre (formato viejo, ej: "SC-Preventivo VISA Febrero - L2603")
    row = execute_one(
        conn,
        "SELECT id FROM campaigns WHERE LOWER(nombre) LIKE %s AND codigo LIKE %s",
        (f"%{nombre_pago}%", f"%-{yymm}"),
    )
    if row:
        logger.info("Campana existente encontrada por nombre (formato previo): id=%s", row["id"])
        return row["id"]

    return None


def create_campaign(conn, nombre_campana, id_tipo_fuente, periodo):
    """
    Crea la campaña en la tabla campaigns y retorna su id.

    Columnas insertadas:
      id_estado   → 1 (activa)
      codigo      → SCP10[ABREV]-[YYMM]
      nombre      → sc-preventivo10[pago]-L[YYYYMM]
      descripcion → texto descriptivo
      brokers     → _[broker_id]_
      fc_inicio   → 0
    """
    codigo = _construir_codigo(id_tipo_fuente, periodo)
    broker_id = BROKER_ID.get(id_tipo_fuente, 956)
    brokers = f"_{broker_id}_"
    descripcion = (
        f"Camp. de Salud de Cartera Preventivo 10. "
        f"{MEDIO_PAGO.get(id_tipo_fuente, '').upper()} {periodo}. "
        f"Generado automáticamente."
    )

    execute_query(
        conn,
        """
        INSERT INTO campaigns (id_estado, codigo, nombre, descripcion, brokers, fc_inicio)
        VALUES (1, %s, %s, %s, %s, 0)
        """,
        (codigo, nombre_campana, descripcion, brokers),
    )
    commit(conn)

    row = execute_one(conn, "SELECT LAST_INSERT_ID() AS id")
    campaign_id = row["id"]
    logger.info("Campaña creada: id=%s codigo=%s nombre=%s", campaign_id, codigo, nombre_campana)
    return campaign_id


def insert_gestiones(conn, campaign_id, items, id_tipo_fuente):
    """
    Inserta los rechazos como gestiones en la tabla gestiones.

    Columnas insertadas:
      id_tipo       → 2
      id_campaign   → campaign_id
      id_broker     → BROKER_ID[id_tipo_fuente]
      id_contacto   → contactos.id  (campo "id" del resultado de la query)
      id_resultado  → 11
      notas         → ''
      timestamp     → YYYYMMDDHHMMSS
      id_tel_fijo1  → contactos.id_tel_fijo1
      lastupdate    → NOW()
    """
    if not items:
        return

    broker_id = BROKER_ID.get(id_tipo_fuente, 956)
    ts = _ts()

    data = [
        (
            2,              # id_tipo
            campaign_id,    # id_campaign
            broker_id,      # id_broker
            row["id"],      # id_contacto  ← contactos.id
            11,             # id_resultado (nuevo/pendiente)
            "",             # notas
            ts,             # timestamp
            row["id_tel_fijo1"],  # id_tel_fijo1 ← GROUP BY de la query
        )
        for row in items
    ]

    execute_many(
        conn,
        """
        INSERT INTO gestiones
            (id_tipo, id_campaign, id_broker, id_contacto, id_resultado, notas, timestamp, id_tel_fijo1, lastupdate)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """,
        data,
    )
    commit(conn)
    logger.info("Gestiones insertadas: %d para campaign_id=%s", len(data), campaign_id)


# ============================================================
# Función principal de procesamiento por grupo
# ============================================================

def procesar_grupo(conn, id_tipo_fuente, periodo, items):
    """
    Procesa un grupo de rechazos para un periodo y tipo de fuente dado.
    Retorna: dict con { campaign_name, campaign_id, action, nuevos_count }
    """
    campaign_name = construir_nombre_campana(id_tipo_fuente, periodo)
    logger.info(
        "Procesando campaña '%s' | tipo_fuente=%d | registros=%d",
        campaign_name,
        id_tipo_fuente,
        len(items),
    )

    campaign_id = campaign_exists(conn, id_tipo_fuente, periodo)

    if campaign_id is None:
        logger.info("Campaña NO existe → creando: %s", campaign_name)
        campaign_id = create_campaign(conn, campaign_name, id_tipo_fuente, periodo)
        insert_gestiones(conn, campaign_id, items, id_tipo_fuente)
        action = "CREADA"
    else:
        logger.info("Campaña YA existe (id=%s) → actualizando: %s", campaign_id, campaign_name)
        insert_gestiones(conn, campaign_id, items, id_tipo_fuente)
        action = "ACTUALIZADA"

    return {
        "campaign_name": campaign_name,
        "campaign_id": campaign_id,
        "action": action,
        "nuevos_count": len(items),
    }
