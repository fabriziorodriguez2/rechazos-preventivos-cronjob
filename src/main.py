import logging
import sys

import config
from db import get_connection, execute_query, close
from queries import QUERY_RECHAZOS, SQL_CREATE_PROCESSED_TABLE
from campaign_service import agrupar_por_periodo, procesar_grupo, MEDIO_PAGO
from processed_repository import marcar_procesados, obtener_procesados_keys, filtrar_nuevos
from discord_service import notify_summary, notify_error

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
        logger.info("Conexion a DB establecida.")
        ensure_processed_table(conn)
    except Exception as e:
        logger.critical("No se pudo conectar a la DB: %s", e)
        notify_error("Conexion DB", e)
        sys.exit(1)

    stats = []
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
                stats.append({"medio": medio, "rechazos": 0, "nuevos": 0, "accion": "SIN DATOS"})
                continue

            grupos = agrupar_por_periodo(rows)

            for periodo, items in grupos.items():
                logger.info(
                    "Procesando periodo=%s | tipo=%s | registros=%d",
                    periodo, medio, len(items),
                )
                try:
                    procesados_keys = obtener_procesados_keys(conn, id_tipo_fuente, periodo)
                    items_nuevos = filtrar_nuevos(items, procesados_keys)

                    if not items_nuevos:
                        logger.info("Sin rechazos nuevos para %s periodo=%s, omitiendo.", medio, periodo)
                        stats.append({"medio": medio, "rechazos": len(items), "nuevos": 0, "accion": "SIN NUEVOS"})
                        continue

                    logger.info("Rechazos nuevos a procesar: %d (de %d totales)", len(items_nuevos), len(items))

                    resultado = procesar_grupo(conn, id_tipo_fuente, periodo, items_nuevos)
                    campaign_name = resultado["campaign_name"]
                    action = resultado["action"]

                    nuevos = marcar_procesados(
                        conn, items_nuevos, id_tipo_fuente, periodo, campaign_name
                    )
                    total_procesados += nuevos

                    stats.append({"medio": medio, "rechazos": len(items), "nuevos": nuevos, "accion": action})
                    logger.info("Campana %s (%s) → %d nuevos procesados", campaign_name, action, nuevos)

                except Exception as e:
                    logger.error("Error procesando grupo tipo=%s periodo=%s: %s", medio, periodo, e)
                    notify_error(f"tipo={medio} periodo={periodo}", e)
                    stats.append({"medio": medio, "rechazos": len(rows), "nuevos": 0, "accion": "ERROR"})

        except Exception as e:
            logger.error("Error consultando rechazos para tipo_fuente=%d: %s", id_tipo_fuente, e)
            notify_error(f"Query tipo_fuente={id_tipo_fuente} ({medio})", e)
            stats.append({"medio": medio, "rechazos": 0, "nuevos": 0, "accion": "ERROR"})

    logger.info("=" * 60)
    logger.info("Cronjob finalizado. Total nuevos procesados: %d", total_procesados)
    logger.info("=" * 60)

    notify_summary(stats)
    close(conn)


if __name__ == "__main__":
    run()
