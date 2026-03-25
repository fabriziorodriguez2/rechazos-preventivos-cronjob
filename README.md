# rechazos-preventivos-cronjob

Script Python que corre diariamente para automatizar campañas preventivas de rechazos en cobranzas.

## Qué hace

1. Consulta rechazos desde la DB por cada medio de pago (visa, master, oca, creditel, cabal, creditosdirectos, passcard)
2. Agrupa los resultados por periodo `YYYYMM`
3. Crea o reutiliza la campaña mensual correspondiente
4. Inserta las gestiones (rechazos) en la campaña
5. Marca los rechazos como procesados para evitar duplicados
6. Notifica a Discord (creada / actualizada / error)

## Estructura

```
src/
  config.py               # Carga .env e inicializa logging
  db.py                   # Conexión pymysql + helpers
  queries.py              # SQL: query base, tabla auxiliar, inserts
  processed_repository.py # Marcar rechazos procesados (batch)
  campaign_service.py     # Lógica de campañas + stubs de integración
  discord_service.py      # Notificaciones Discord via webhook
  main.py                 # Orquestador principal
.env                      # Variables de entorno (no commitear)
requirements.txt
```

## Cómo correr

```bash
# Instalar dependencias
pip install -r requirements.txt

# Correr el script
python src/main.py
```

## Cron (Linux/Mac)

```cron
0 8 * * * /usr/bin/python3 /ruta/absoluta/src/main.py >> /var/log/rechazos-preventivos.log 2>&1
```

## Variables de entorno (.env)

| Variable             | Descripción                        | Ejemplo                  |
|----------------------|------------------------------------|--------------------------|
| `DB_HOST`            | Host de la base de datos           | `localhost`              |
| `DB_PORT`            | Puerto MySQL                       | `3306`                   |
| `DB_NAME`            | Nombre de la base de datos         | `cobranzas`              |
| `DB_USER`            | Usuario MySQL                      | `root`                   |
| `DB_PASSWORD`        | Contraseña MySQL                   | `secret`                 |
| `DISCORD_WEBHOOK_URL`| URL del webhook de Discord         | `https://discord.com/...`|
| `LOG_LEVEL`          | Nivel de logging                   | `INFO` / `DEBUG`         |

## SQL tabla auxiliar

Crear manualmente si no existe (el script también la crea al iniciar):

```sql
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
);
```

## Medios de pago

| id_tipo_fuente | Nombre             |
|----------------|--------------------|
| 13             | visa               |
| 14             | master             |
| 15             | oca                |
| 16             | creditel           |
| 17             | cabal              |
| 18             | creditosdirectos   |
| 21             | passcard           |

## TODOs de integración real

Estos 3 stubs en `src/campaign_service.py` necesitan implementación con tus tablas reales:

### 1. `campaign_exists(conn, nombre_campana)`
Verificar si la campaña ya existe. Debe retornar el `id` de la campaña o `None`.

```python
def campaign_exists(conn, nombre_campana):
    from db import execute_one
    row = execute_one(conn, "SELECT id FROM campanas WHERE nombre = %s", (nombre_campana,))
    return row["id"] if row else None
```

### 2. `create_campaign(conn, nombre_campana)`
Insertar la campaña y retornar su `id`.

```python
def create_campaign(conn, nombre_campana):
    from db import execute_query, execute_one, commit
    execute_query(conn, "INSERT INTO campanas (nombre, fecha_creacion) VALUES (%s, NOW())", (nombre_campana,))
    commit(conn)
    row = execute_one(conn, "SELECT LAST_INSERT_ID() as id")
    return row["id"]
```

### 3. `insert_gestiones(conn, campaign_id, items)`
Insertar los rechazos como gestiones asociadas a la campaña.

```python
def insert_gestiones(conn, campaign_id, items):
    from db import execute_many, commit
    data = [(campaign_id, row["id_pago"], row["id_servicio"]) for row in items]
    execute_many(conn, "INSERT INTO gestiones (id_campana, id_pago, id_servicio) VALUES (%s, %s, %s)", data)
    commit(conn)
```

Adaptá los nombres de tabla y columnas a tu schema real.
