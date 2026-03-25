import logging
import requests
from config import DISCORD_WEBHOOK_URL

logger = logging.getLogger(__name__)


def _send(payload):
    if not DISCORD_WEBHOOK_URL:
        logger.warning("DISCORD_WEBHOOK_URL no configurada, notificación omitida.")
        return
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        logger.debug("Discord notificado OK (status=%s)", resp.status_code)
    except Exception as e:
        logger.error("Error al notificar Discord: %s", e)


def notify_created(campaign_name, count, periodo, medio_pago):
    payload = {
        "embeds": [
            {
                "title": "✅ Campaña CREADA",
                "color": 3066993,  # verde
                "fields": [
                    {"name": "Campaña", "value": campaign_name, "inline": False},
                    {"name": "Medio de pago", "value": medio_pago, "inline": True},
                    {"name": "Periodo", "value": periodo, "inline": True},
                    {"name": "Gestiones insertadas", "value": str(count), "inline": True},
                ],
            }
        ]
    }
    _send(payload)


def notify_updated(campaign_name, count, periodo, medio_pago):
    payload = {
        "embeds": [
            {
                "title": "🔄 Campaña ACTUALIZADA",
                "color": 3447003,  # azul
                "fields": [
                    {"name": "Campaña", "value": campaign_name, "inline": False},
                    {"name": "Medio de pago", "value": medio_pago, "inline": True},
                    {"name": "Periodo", "value": periodo, "inline": True},
                    {"name": "Nuevas gestiones", "value": str(count), "inline": True},
                ],
            }
        ]
    }
    _send(payload)


def notify_error(context, error):
    payload = {
        "embeds": [
            {
                "title": "❌ Error en cronjob rechazos preventivos",
                "color": 15158332,  # rojo
                "fields": [
                    {"name": "Contexto", "value": str(context), "inline": False},
                    {"name": "Error", "value": str(error)[:1024], "inline": False},
                ],
            }
        ]
    }
    _send(payload)
