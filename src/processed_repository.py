import logging
from db import execute_many, commit, rollback
from queries import SQL_INSERT_PROCESSED

logger = logging.getLogger(__name__)


def marcar_procesados(conn, items, id_tipo_fuente, periodo, campaign_name):
    """
    Inserta en lote los rechazos procesados.
    Usa INSERT IGNORE para evitar duplicados por (id_pago, id_factura, id_servicio).

    items: lista de dicts con claves id_pago, id_factura, id_servicio
    """
    if not items:
        logger.debug("No hay items para marcar como procesados.")
        return 0

    data = [
        (
            row["id_pago"],
            row["id_factura"],
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
            "Procesados marcados: %d nuevos de %d intentados (periodo=%s, campaña=%s)",
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
