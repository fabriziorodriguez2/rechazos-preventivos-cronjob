# rechazos-preventivos-cronjob

Script Python que corre diariamente para automatizar campanas preventivas de rechazos en cobranzas.

## Que hace

Por cada medio de pago (visa, master, oca, creditel, cabal, creditosdirectos, passcard):

1. Consulta rechazos desde la DB (query base intocable)
2. Filtra los que ya fueron procesados (evita duplicados)
3. Si quedan rechazos nuevos:
   - Busca la campana del periodo actual
   - Si no existe: la crea e inserta gestiones
   - Si ya existe: inserta solo las gestiones nuevas
4. Marca los rechazos como procesados
5. Notifica a Discord (CREADA / ACTUALIZADA / ERROR)

## Estructura

```
src/
  config.py               - Carga .env e inicializa logging
  db.py                   - Conexion pymysql + helpers
  queries.py              - SQL: query base, tabla auxiliar, inserts
  processed_repository.py - Filtrar y marcar rechazos procesados
  campaign_service.py     - Logica de campanas (buscar, crear, insertar gestiones)
  discord_service.py      - Notificaciones Discord via webhook
  main.py                 - Orquestador principal
tests/
  test_unit.py            - Tests unitarios (sin dependencias externas)
.env                      - Variables de entorno (no commitear)
requirements.txt
```

## Como correr

```bash
# Instalar dependencias
pip install -r requirements.txt

# Correr el script
python src/main.py
```

## Tests

No requieren conexion a DB ni Discord. Solo stdlib de Python.

```bash
# Desde la raiz del proyecto
python tests/test_unit.py

# Con detalle de cada test
python tests/test_unit.py -v
```

Casos cubiertos:
- Formato exacto de nombre de campana para los 7 medios de pago
- Formato exacto de codigo (SCP10VI-2603, SCP10PC-2603, etc.)
- Agrupacion por periodo YYYYMM
- Filtrado de rechazos ya procesados (parcial / total / ninguno)
- Busqueda de campana por codigo exacto
- Busqueda de campana por nombre (formato viejo, fallback LIKE)
- Creacion de campana cuando no existe
- Actualizacion de campana existente (formato nuevo y viejo)
- No inserta gestiones si no hay items

## Cron (Linux/Mac)

```cron
0 8 * * * python3 /home/asiste8/rechazos-preventivos-cronjob/src/main.py >> /home/asiste8/rechazos-preventivos-cronjob/cronjob.log 2>&1
```

Ver logs:
```bash
tail -50 /home/asiste8/rechazos-preventivos-cronjob/cronjob.log
```

## Variables de entorno (.env)

| Variable              | Descripcion                        | Ejemplo                   |
|-----------------------|------------------------------------|---------------------------|
| `DB_HOST`             | Host de la base de datos           | `localhost`               |
| `DB_PORT`             | Puerto MySQL                       | `3306`                    |
| `DB_NAME`             | Nombre de la base de datos         | `asiste8_globalas_...`    |
| `DB_USER`             | Usuario MySQL                      | `asiste8_system`          |
| `DB_PASSWORD`         | Contrasena MySQL                   | `...`                     |
| `DISCORD_WEBHOOK_URL` | URL del webhook de Discord         | `https://discord.com/...` |
| `LOG_LEVEL`           | Nivel de logging                   | `INFO` / `DEBUG`          |

## SQL tabla auxiliar

Se crea automaticamente al iniciar. Para recrearla desde cero:

```sql
DROP TABLE IF EXISTS rechazos_preventivos_procesados;

CREATE TABLE rechazos_preventivos_procesados (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_pago INT NOT NULL,
    id_contacto INT NOT NULL,
    id_servicio INT NOT NULL,
    id_tipo_fuente INT NOT NULL,
    periodo CHAR(6) NOT NULL,
    campaign_name VARCHAR(100) NOT NULL,
    fecha_procesado DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_rechazo (id_pago, id_contacto, id_servicio)
);
```

## Medios de pago

| id_tipo_fuente | Nombre             | Codigo        |
|----------------|--------------------|---------------|
| 13             | visa               | SCP10VI-YYMM  |
| 14             | master             | SCP10MA-YYMM  |
| 15             | oca                | SCP10OCA-YYMM |
| 16             | creditel           | SCP10CR-YYMM  |
| 17             | cabal              | SCP10CAB-YYMM |
| 18             | creditosdirectos   | SCP10CD-YYMM  |
| 21             | passcard           | SCP10PC-YYMM  |

## Logica anti-duplicados

Antes de insertar gestiones, el script verifica en `rechazos_preventivos_procesados`
que combinacion de (id_pago, id_servicio) ya fue procesada para ese tipo_fuente y periodo.
Solo inserta los nuevos.

Busqueda de campana existente (en orden):
1. Por `codigo` exacto (SCP10VI-2603) - campanas creadas por este script
2. Por `nombre` LIKE + codigo LIKE - campanas creadas manualmente con formato distinto

## Ajustes pendientes en campaign_service.py

### BROKER_ID (linea 35)
Actualmente todos los medios usan broker 956.
Si cada medio tiene un broker distinto, actualizar el mapping:
```python
BROKER_ID = {
    13: 956,   # visa
    14: 956,   # master
    ...
}
```

### id_resultado en gestiones (linea 183)
Actualmente se inserta con `id_resultado = 11`.
Cambiar si corresponde otro valor para rechazos preventivos.
