import logging
from datetime import datetime
from db import execute_one, execute_query, commit
from handy_queries import (
    SQL_CONTACTO_POR_SERVICIO,
    SQL_CONTACTO_POR_CEDULA,
    SQL_MARCAR_PROCESADO,
    SQL_MARCAR_ERROR,
)

logger = logging.getLogger(__name__)

HANDY_BROKER_ID = 956

# ============================================================
# Nomenclatura campaña HANDY
# ============================================================

def _periodo_actual():
    return datetime.now().strftime("%Y%m")


def _construir_nombre_campana(periodo):
    """SC-HANDY- L[YYMM]"""
    yymm = periodo[2:]
    return f"SC-HANDY- L{yymm}"


def _construir_codigo(periodo):
    """HANDY-[YYMM]"""
    yymm = periodo[2:]
    return f"HANDY-{yymm}"


def _ts():
    return datetime.now().strftime("%Y%m%d%H%M%S")


# ============================================================
# Resolución de id_contacto
# ============================================================

def resolver_contacto(conn, row):
    """
    Resuelve id_contacto + id_tel_fijo1 a partir de id_servicio o cedula.
    Prioridad: id_servicio > cedula.
    """
    id_servicio = row.get("id_servicio")
    cedula = row.get("cedula")

    if id_servicio:
        result = execute_one(conn, SQL_CONTACTO_POR_SERVICIO, (id_servicio,))
        if result:
            return {
                "id_contacto": result["id_contacto"],
                "id_tel_fijo1": result["id_tel_fijo1"],
            }
        return None

    if cedula:
        result = execute_one(conn, SQL_CONTACTO_POR_CEDULA, (cedula,))
        if result:
            return {
                "id_contacto": result["id_contacto"],
                "id_tel_fijo1": result["id_tel_fijo1"],
            }
        return None

    return None


# ============================================================
# Campaña mensual HANDY (crear o reutilizar)
# ============================================================

def obtener_o_crear_campana(conn, periodo):
    """
    Busca la campaña HANDY del mes. Si no existe, la crea.
    """
    codigo = _construir_codigo(periodo)
    nombre = _construir_nombre_campana(periodo)

    row = execute_one(conn, "SELECT id FROM campaigns WHERE codigo = %s", (codigo,))
    if row:
        logger.debug("Campaña HANDY existente: id=%s codigo=%s", row["id"], codigo)
        return row["id"]

    yymm = periodo[2:]
    row = execute_one(
        conn,
        "SELECT id FROM campaigns WHERE LOWER(nombre) LIKE %s AND codigo LIKE %s",
        ("%handy%", f"%-{yymm}"),
    )
    if row:
        logger.info("Campaña HANDY encontrada por nombre (formato previo): id=%s", row["id"])
        return row["id"]

    brokers = f"_{HANDY_BROKER_ID}_"
    descripcion = f"Camp. HANDY {periodo}. Generado automáticamente."

    execute_query(
        conn,
        """
        INSERT INTO campaigns (id_estado, codigo, nombre, descripcion, brokers, fc_inicio)
        VALUES (1, %s, %s, %s, %s, 0)
        """,
        (codigo, nombre, descripcion, brokers),
    )
    commit(conn)

    row = execute_one(conn, "SELECT LAST_INSERT_ID() AS id")
    campaign_id = row["id"]

    execute_query(
        conn,
        "INSERT INTO campaign_brokers (id_campaign, id_user) VALUES (%s, %s)",
        (campaign_id, HANDY_BROKER_ID),
    )
    commit(conn)

    logger.info("Campaña HANDY creada: id=%s codigo=%s nombre=%s", campaign_id, codigo, nombre)
    return campaign_id


# ============================================================
# Insertar gestión individual y obtener id
# ============================================================

def insertar_gestion(conn, campaign_id, id_contacto, id_tel_fijo1):
    ts = _ts()
    execute_query(
        conn,
        """
        INSERT INTO gestiones
            (id_tipo, id_campaign, id_broker, id_contacto, id_resultado,
             notas, timestamp, id_tel_fijo1, lastupdate)
        VALUES (2, %s, %s, %s, 0, '', %s, %s, NOW())
        """,
        (campaign_id, HANDY_BROKER_ID, id_contacto, ts, id_tel_fijo1),
    )
    commit(conn)
    row = execute_one(conn, "SELECT LAST_INSERT_ID() AS id")
    return row["id"]


# ============================================================
# Procesamiento fila a fila
# ============================================================

def procesar_fila(conn, staging_row, campaign_id):
    staging_id = staging_row["id"]

    contacto = resolver_contacto(conn, staging_row)
    if not contacto:
        motivo = "id_servicio" if staging_row.get("id_servicio") else "cedula"
        valor = staging_row.get("id_servicio") or staging_row.get("cedula") or "ninguno"
        msg = f"No se encontró contacto válido por {motivo}={valor}"
        execute_query(conn, SQL_MARCAR_ERROR, (msg, staging_id))
        commit(conn)
        logger.warning("Staging id=%s: %s", staging_id, msg)
        return False

    id_contacto = contacto["id_contacto"]
    id_tel_fijo1 = contacto["id_tel_fijo1"]

    id_gestion = insertar_gestion(conn, campaign_id, id_contacto, id_tel_fijo1)

    execute_query(
        conn,
        SQL_MARCAR_PROCESADO,
        (id_contacto, campaign_id, id_gestion, staging_id),
    )
    commit(conn)

    logger.debug(
        "Staging id=%s → contacto=%s campaign=%s gestion=%s",
        staging_id, id_contacto, campaign_id, id_gestion,
    )
    return True
