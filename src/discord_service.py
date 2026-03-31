import logging
import requests
from datetime import datetime, timedelta
from config import DISCORD_WEBHOOK_URL

logger = logging.getLogger(__name__)


def _send(payload):
    if not DISCORD_WEBHOOK_URL:
        logger.warning("DISCORD_WEBHOOK_URL no configurada, notificacion omitida.")
        return
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        logger.debug("Discord notificado OK (status=%s)", resp.status_code)
    except Exception as e:
        logger.error("Error al notificar Discord: %s", e)


def notify_summary(stats):
    """
    Envia un resumen consolidado al final de la ejecucion.

    stats: lista de dicts con claves:
        medio     - nombre del medio de pago
        rechazos  - total obtenidos de la query
        nuevos    - nuevos procesados en esta ejecucion
        accion    - CREADA / ACTUALIZADA / SIN NUEVOS / SIN DATOS / ERROR
    """
    fecha = (datetime.now() + timedelta(hours=4)).strftime("%d/%m/%Y %H:%M")

    col_medio = 20
    col_rechazos = 9
    col_nuevos = 7

    header = f"{'Medio':<{col_medio}} {'Rechazos':>{col_rechazos}} {'Nuevos':>{col_nuevos}}  Accion"
    sep = "-" * (col_medio + col_rechazos + col_nuevos + 12)

    filas = []
    total_rechazos = 0
    total_nuevos = 0

    for s in stats:
        rechazos = s["rechazos"]
        nuevos = s["nuevos"]
        total_rechazos += rechazos
        total_nuevos += nuevos
        filas.append(
            f"{s['medio']:<{col_medio}} {rechazos:>{col_rechazos}} {nuevos:>{col_nuevos}}  {s['accion']}"
        )

    total_line = f"{'TOTAL':<{col_medio}} {total_rechazos:>{col_rechazos}} {total_nuevos:>{col_nuevos}}"

    table = "\n".join([header, sep] + filas + [sep, total_line])

    if total_nuevos == 0:
        logger.info("Sin rechazos nuevos, notificacion Discord omitida.")
        return

    color = 3066993  # verde

    payload = {
        "embeds": [
            {
                "title": f"Resumen rechazos preventivos - {fecha}",
                "color": color,
                "description": f"```\n{table}\n```",
            }
        ]
    }
    _send(payload)


def notify_error(context, error):
    payload = {
        "embeds": [
            {
                "title": "ERROR cronjob rechazos preventivos",
                "color": 15158332,
                "fields": [
                    {"name": "Contexto", "value": str(context), "inline": False},
                    {"name": "Error", "value": str(error)[:1024], "inline": False},
                ],
            }
        ]
    }
    _send(payload)
