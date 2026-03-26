"""
Tests unitarios para rechazos-preventivos-cronjob.
Usa unittest + unittest.mock (stdlib, sin dependencias externas).

Correr:
    python tests/test_unit.py
    python tests/test_unit.py -v   (verbose)
"""
import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Agregar src/ al path para poder importar los modulos
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from campaign_service import (
    construir_nombre_campana,
    _construir_codigo,
    agrupar_por_periodo,
    campaign_exists,
    procesar_grupo,
)
from processed_repository import filtrar_nuevos


# ============================================================
# Helpers
# ============================================================

def make_rows(n, id_pago_start=1, id_servicio_mult=10):
    """Genera n filas simuladas con los campos que devuelve la query base."""
    return [
        {
            "id":           i,           # contactos.id (SELECT * pisa los demas id)
            "id_pago":      id_pago_start + i - 1,
            "id_servicio":  (id_pago_start + i - 1) * id_servicio_mult,
            "id_tel_fijo1": (id_pago_start + i - 1) * 100,
        }
        for i in range(1, n + 1)
    ]


# ============================================================
# 1. Nombres y codigos
# ============================================================

class TestConstruirNombreCampana(unittest.TestCase):

    def test_formato_exacto_visa(self):
        self.assertEqual(
            construir_nombre_campana(13, "202603"),
            "sc-preventivo10visa-L202603",
        )

    def test_todos_los_medios(self):
        esperados = {
            13: "sc-preventivo10visa-L202603",
            14: "sc-preventivo10master-L202603",
            15: "sc-preventivo10oca-L202603",
            16: "sc-preventivo10creditel-L202603",
            17: "sc-preventivo10cabal-L202603",
            18: "sc-preventivo10creditosdirectos-L202603",
            21: "sc-preventivo10passcard-L202603",
        }
        for id_tipo, nombre in esperados.items():
            with self.subTest(id_tipo=id_tipo):
                self.assertEqual(construir_nombre_campana(id_tipo, "202603"), nombre)

    def test_tipo_desconocido_lanza_error(self):
        with self.assertRaises(ValueError):
            construir_nombre_campana(99, "202603")

    def test_periodo_en_nombre(self):
        nombre = construir_nombre_campana(13, "202604")
        self.assertIn("202604", nombre)


class TestConstruirCodigo(unittest.TestCase):

    def test_formato_exacto_passcard(self):
        self.assertEqual(_construir_codigo(21, "202603"), "SCP10PC-2603")

    def test_formato_exacto_visa(self):
        self.assertEqual(_construir_codigo(13, "202603"), "SCP10VI-2603")

    def test_todos_los_codigos(self):
        esperados = {
            13: "SCP10VI-2603",
            14: "SCP10MA-2603",
            15: "SCP10OCA-2603",
            16: "SCP10CR-2603",
            17: "SCP10CAB-2603",
            18: "SCP10CD-2603",
            21: "SCP10PC-2603",
        }
        for id_tipo, codigo in esperados.items():
            with self.subTest(id_tipo=id_tipo):
                self.assertEqual(_construir_codigo(id_tipo, "202603"), codigo)

    def test_yymm_correcto(self):
        # "202603" → yymm = "2603"
        self.assertTrue(_construir_codigo(13, "202603").endswith("-2603"))
        self.assertTrue(_construir_codigo(13, "202604").endswith("-2604"))


class TestAgruparPorPeriodo(unittest.TestCase):

    def test_todos_van_al_periodo_actual(self):
        rows = make_rows(5)
        grupos = agrupar_por_periodo(rows)
        self.assertEqual(len(grupos), 1)
        periodo = list(grupos.keys())[0]
        self.assertEqual(len(grupos[periodo]), 5)

    def test_periodo_tiene_formato_yyyymm(self):
        grupos = agrupar_por_periodo(make_rows(1))
        periodo = list(grupos.keys())[0]
        self.assertEqual(len(periodo), 6)
        self.assertTrue(periodo.isdigit())

    def test_lista_vacia(self):
        grupos = agrupar_por_periodo([])
        self.assertEqual(grupos, {})


# ============================================================
# 2. Filtro de duplicados
# ============================================================

class TestFiltrarNuevos(unittest.TestCase):

    def test_filtra_ya_procesados(self):
        items = make_rows(3)  # id_pago: 1, 2, 3 | id_servicio: 10, 20, 30
        procesados = {(1, 10), (2, 20)}
        result = filtrar_nuevos(items, procesados)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id_pago"], 3)

    def test_sin_procesados_pasan_todos(self):
        items = make_rows(3)
        result = filtrar_nuevos(items, set())
        self.assertEqual(len(result), 3)

    def test_todos_procesados_retorna_vacio(self):
        items = make_rows(2)
        procesados = {(1, 10), (2, 20)}
        result = filtrar_nuevos(items, procesados)
        self.assertEqual(result, [])

    def test_items_vacios_retorna_vacio(self):
        result = filtrar_nuevos([], {(1, 10)})
        self.assertEqual(result, [])

    def test_solo_nuevo_pasa(self):
        items = make_rows(3)
        procesados = {(1, 10), (2, 20)}
        result = filtrar_nuevos(items, procesados)
        self.assertEqual(result[0]["id_pago"], 3)
        self.assertEqual(result[0]["id_servicio"], 30)


# ============================================================
# 3. Busqueda de campana existente
# ============================================================

class TestCampaignExists(unittest.TestCase):

    def test_encuentra_por_codigo_exacto(self):
        """Caso principal: campana existe con nuestro formato de codigo."""
        with patch("campaign_service.execute_one") as mock:
            mock.return_value = {"id": 739}
            result = campaign_exists(MagicMock(), 13, "202603")
            self.assertEqual(result, 739)
            # Verificar que busco por codigo SCP10VI-2603
            primera_llamada = mock.call_args_list[0]
            self.assertIn("SCP10VI-2603", str(primera_llamada))

    def test_encuentra_por_nombre_formato_viejo(self):
        """Fallback: campana existe con nombre en formato anterior (ej: SC-Preventivo 10 VISA - L2603)."""
        with patch("campaign_service.execute_one") as mock:
            mock.side_effect = [None, {"id": 748}]  # codigo: no encuentra | nombre: encuentra
            result = campaign_exists(MagicMock(), 13, "202603")
            self.assertEqual(result, 748)
            self.assertEqual(mock.call_count, 2)

    def test_no_existe_retorna_none(self):
        """Campana no existe en ningun formato."""
        with patch("campaign_service.execute_one") as mock:
            mock.return_value = None
            result = campaign_exists(MagicMock(), 13, "202603")
            self.assertIsNone(result)
            self.assertEqual(mock.call_count, 2)  # busco por codigo Y por nombre

    def test_todos_los_medios_usan_codigo_correcto(self):
        """Cada medio de pago busca por su codigo especifico."""
        codigos = {
            13: "SCP10VI-2603",
            14: "SCP10MA-2603",
            15: "SCP10OCA-2603",
            16: "SCP10CR-2603",
            17: "SCP10CAB-2603",
            18: "SCP10CD-2603",
            21: "SCP10PC-2603",
        }
        for id_tipo, codigo_esperado in codigos.items():
            with self.subTest(id_tipo=id_tipo):
                with patch("campaign_service.execute_one") as mock:
                    mock.return_value = {"id": 100}
                    campaign_exists(MagicMock(), id_tipo, "202603")
                    primera = mock.call_args_list[0]
                    self.assertIn(codigo_esperado, str(primera))


# ============================================================
# 4. Logica principal: procesar_grupo
# ============================================================

class TestProcesarGrupo(unittest.TestCase):

    def test_crea_campana_cuando_no_existe(self):
        """Si no hay campana para el periodo, la crea e inserta gestiones."""
        with patch("campaign_service.execute_one") as mock_one, \
             patch("campaign_service.execute_query"), \
             patch("campaign_service.execute_many"), \
             patch("campaign_service.commit"):

            # campaign_exists: codigo=None, nombre=None → no existe
            # create_campaign: LAST_INSERT_ID → id 999
            mock_one.side_effect = [None, None, {"id": 999}]

            result = procesar_grupo(MagicMock(), 13, "202603", make_rows(5))

            self.assertEqual(result["action"], "CREADA")
            self.assertEqual(result["campaign_id"], 999)
            self.assertEqual(result["campaign_name"], "sc-preventivo10visa-L202603")
            self.assertEqual(result["nuevos_count"], 5)

    def test_actualiza_campana_existente_por_codigo(self):
        """Si la campana ya existe (codigo exacto), inserta en ella sin crear nueva."""
        with patch("campaign_service.execute_one") as mock_one, \
             patch("campaign_service.execute_many"), \
             patch("campaign_service.commit"):

            mock_one.return_value = {"id": 739}  # encontrada por codigo

            result = procesar_grupo(MagicMock(), 13, "202603", make_rows(3))

            self.assertEqual(result["action"], "ACTUALIZADA")
            self.assertEqual(result["campaign_id"], 739)

    def test_actualiza_campana_existente_formato_viejo(self):
        """Si la campana existe con formato viejo (LIKE), la reutiliza."""
        with patch("campaign_service.execute_one") as mock_one, \
             patch("campaign_service.execute_many"), \
             patch("campaign_service.commit"):

            # codigo: no encuentra | nombre LIKE: encuentra
            mock_one.side_effect = [None, {"id": 748}]

            result = procesar_grupo(MagicMock(), 13, "202603", make_rows(4))

            self.assertEqual(result["action"], "ACTUALIZADA")
            self.assertEqual(result["campaign_id"], 748)

    def test_no_inserta_gestiones_si_items_vacios(self):
        """Si no hay items nuevos, insert_gestiones no se llama."""
        with patch("campaign_service.execute_one") as mock_one, \
             patch("campaign_service.execute_many") as mock_many, \
             patch("campaign_service.commit"):

            mock_one.return_value = {"id": 739}

            procesar_grupo(MagicMock(), 13, "202603", [])

            mock_many.assert_not_called()

    def test_nombre_campana_correcto_para_cada_medio(self):
        """El nombre de campana tiene el formato exacto para cada medio de pago."""
        esperados = {
            13: "sc-preventivo10visa-L202603",
            14: "sc-preventivo10master-L202603",
            21: "sc-preventivo10passcard-L202603",
        }
        for id_tipo, nombre_esperado in esperados.items():
            with self.subTest(id_tipo=id_tipo):
                with patch("campaign_service.execute_one") as mock_one, \
                     patch("campaign_service.execute_many"), \
                     patch("campaign_service.commit"):
                    mock_one.return_value = {"id": 100}
                    result = procesar_grupo(MagicMock(), id_tipo, "202603", make_rows(1))
                    self.assertEqual(result["campaign_name"], nombre_esperado)


# ============================================================
# Punto de entrada
# ============================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)
