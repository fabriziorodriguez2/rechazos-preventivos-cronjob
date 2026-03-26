import logging
from db import execute_query, execute_many, commit, rollback
from queries import SQL_INSERT_PROCESSED

logger = logging.getLogger(__name__)


def obtener_procesados_keys(conn, id_tipo_fuente, periodo):
    """
    Retorna un set de (id_pago, id_servicio) ya procesados para este tipo_fuente y periodo.
    Se usa para filtrar antes de insertar gestiones.
    """
    rows = execute_query(
        conn,
        """
        SELECT id_pago, id_servicio
        FROM rechazos_preventivos_procesados
        WHERE id_tipo_fuente = %s AND periodo = %s
        """,
        (id_tipo_fuente, periodo),
    )
    return {(r["id_pago"], r["id_servicio"]) for r in rows}


def filtrar_nuevos(items, procesados_keys):
    """
    Devuelve solo los items que NO estan en procesados_keys.
    """
    return [
        row for row in items
        if (row["id_pago"], row["id_servicio"]) not in procesados_keys
    ]


def marcar_procesados(conn, items, id_tipo_fuente, periodo, campaign_name):
    """
    Inserta en lote los rechazos procesados.
    Usa INSERT IGNORE para evitar duplicados por UNIQUE KEY.
    """
    if not items:
        logger.debug("No hay items para marcar como procesados.")
        return 0

    data = [
        (
            row["id_pago"],
            row["id"],       # contactos.id = id_contacto (SELECT * pisa los demas "id")
            row["id_servicio"],
            id_tipo_fuente,
            periodo,
            campaign_name,
        )
        for row in items
    ]

    try:
        inserted = execute_many(conn, SQL_INSERT_PROCESSED, data)
        commit(conn)
        logger.info(
            "Procesados marcados: %d de %d (periodo=%s campana=%s)",
            inserted,
            len(data),
            periodo,
            campaign_name,
        )
        return inserted
    except Exception as e:
        rollback(conn)
        logger.error("Error al marcar procesados: %s", e)
        raise
