import logging
import sys

import config
from db import get_connection, execute_query, close
from queries import QUERY_RECHAZOS, SQL_CREATE_PROCESSED_TABLE
from campaign_service import agrupar_por_periodo, procesar_grupo, MEDIO_PAGO
from processed_repository import marcar_procesados
from discord_service import notify_created, notify_updated, notify_error

logger = logging.getLogger(__name__)

ID_TIPOS_FUENTE = [13, 14, 15, 16, 17, 18, 21]


def ensure_processed_table(conn):
    execute_query(conn, SQL_CREATE_PROCESSED_TABLE)


def run():
    logger.info("=" * 60)
    logger.info("Iniciando cronjob rechazos preventivos (log_level=%s)", config.LOG_LEVEL)
    logger.info("=" * 60)

    conn = None
    try:
        conn = get_connection()
        logger.info("Conexión a DB establecida.")
        ensure_processed_table(conn)
    except Exception as e:
        logger.critical("No se pudo conectar a la DB: %s", e)
        notify_error("Conexión DB", e)
        sys.exit(1)

    total_procesados = 0

    for id_tipo_fuente in ID_TIPOS_FUENTE:
        medio = MEDIO_PAGO.get(id_tipo_fuente, str(id_tipo_fuente))
        logger.info("-" * 40)
        logger.info("Consultando rechazos para tipo_fuente=%d (%s)", id_tipo_fuente, medio)

        try:
            rows = execute_query(conn, QUERY_RECHAZOS, (id_tipo_fuente,))
            logger.info("Registros obtenidos: %d", len(rows))

            if not rows:
                logger.info("Sin rechazos para %s, omitiendo.", medio)
                continue

            grupos = agrupar_por_periodo(rows)

            for periodo, items in grupos.items():
                logger.info(
                    "Procesando periodo=%s | tipo=%s | registros=%d",
                    periodo,
                    medio,
                    len(items),
                )
                try:
                    resultado = procesar_grupo(conn, id_tipo_fuente, periodo, items)
                    campaign_name = resultado["campaign_name"]
                    action = resultado["action"]
                    count = resultado["nuevos_count"]

                    nuevos = marcar_procesados(
                        conn, items, id_tipo_fuente, periodo, campaign_name
                    )
                    total_procesados += nuevos

                    if action == "CREADA":
                        notify_created(campaign_name, count, periodo, medio)
                    else:
                        notify_updated(campaign_name, nuevos, periodo, medio)

                    logger.info(
                        "Campaña %s (%s) → %d nuevos procesados",
                        campaign_name,
                        action,
                        nuevos,
                    )

                except Exception as e:
                    logger.error(
                        "Error procesando grupo tipo=%s periodo=%s: %s",
                        medio,
                        periodo,
                        e,
                    )
                    notify_error(f"tipo={medio} periodo={periodo}", e)

        except Exception as e:
            logger.error("Error consultando rechazos para tipo_fuente=%d: %s", id_tipo_fuente, e)
            notify_error(f"Query tipo_fuente={id_tipo_fuente} ({medio})", e)

    logger.info("=" * 60)
    logger.info("Cronjob finalizado. Total nuevos procesados: %d", total_procesados)
    logger.info("=" * 60)
    close(conn)


if __name__ == "__main__":
    run()
