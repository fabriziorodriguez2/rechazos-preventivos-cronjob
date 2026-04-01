import logging
import sys

import config
from db import get_connection, execute_query, close
from handy_queries import SQL_CREATE_HANDY_STAGING, SQL_HANDY_PENDIENTES
from handy_service import (
    _periodo_actual,
    obtener_o_crear_campana,
    procesar_fila,
)
from discord_service import notify_error

logger = logging.getLogger(__name__)


def _send_handy_summary(stats):
    from discord_service import _send
    from datetime import datetime, timedelta

    fecha = (datetime.now() + timedelta(hours=4)).strftime("%d/%m/%Y %H:%M")
    total = stats["total"]
    ok = stats["procesados"]
    err = stats["errores"]

    if total == 0 and stats.get("sin_datos"):
        logger.info("HANDY: sin filas pendientes, notificación omitida.")
        return

    lines = [
        f"Total pendientes:  {total}",
        f"Procesados OK:     {ok}",
        f"Errores:           {err}",
    ]
    body = "\n".join(lines)

    color = 3066993 if err == 0 else 15158332
    payload = {
        "embeds": [
            {
                "title": f"Resumen HANDY - {fecha}",
                "color": color,
                "description": f"```\n{body}\n```",
            }
        ]
    }
    _send(payload)


def run_handy():
    logger.info("=" * 60)
    logger.info("Iniciando cronjob HANDY (log_level=%s)", config.LOG_LEVEL)
    logger.info("=" * 60)

    conn = None
    try:
        conn = get_connection()
        logger.info("Conexión a DB establecida.")
        execute_query(conn, SQL_CREATE_HANDY_STAGING)
    except Exception as e:
        logger.critical("No se pudo conectar a la DB: %s", e)
        notify_error("HANDY - Conexion DB", e)
        sys.exit(1)

    stats = {"total": 0, "procesados": 0, "errores": 0, "sin_datos": False}

    try:
        pendientes = execute_query(conn, SQL_HANDY_PENDIENTES)
        stats["total"] = len(pendientes)
        logger.info("Filas PENDIENTE encontradas: %d", len(pendientes))

        if not pendientes:
            stats["sin_datos"] = True
            logger.info("Sin filas pendientes, finalizando.")
            _send_handy_summary(stats)
            close(conn)
            return

        periodo = _periodo_actual()
        campaign_id = obtener_o_crear_campana(conn, periodo)
        logger.info("Campaña HANDY: id=%s periodo=%s", campaign_id, periodo)

        for row in pendientes:
            try:
                ok = procesar_fila(conn, row, campaign_id)
                if ok:
                    stats["procesados"] += 1
                else:
                    stats["errores"] += 1
            except Exception as e:
                stats["errores"] += 1
                logger.error("Error inesperado staging id=%s: %s", row["id"], e)
                try:
                    from handy_queries import SQL_MARCAR_ERROR
                    from db import commit as db_commit
                    execute_query(conn, SQL_MARCAR_ERROR, (str(e)[:500], row["id"]))
                    db_commit(conn)
                except Exception:
                    logger.error("No se pudo marcar error para staging id=%s", row["id"])

    except Exception as e:
        logger.critical("Error general HANDY: %s", e)
        notify_error("HANDY - General", e)

    logger.info("=" * 60)
    logger.info(
        "HANDY finalizado. Procesados=%d Errores=%d de %d",
        stats["procesados"], stats["errores"], stats["total"],
    )
    logger.info("=" * 60)

    _send_handy_summary(stats)
    close(conn)


if __name__ == "__main__":
    run_handy()
