"""
tests/test_pipeline.py - Unit Tests

CONCEPTO FUNDAMENTAL: Testing in Data Pipelines
===============================================
Testing es crítico en data engineering:
1. Prevenir bugs que corrompen datos
2. Confidence para refactorizar
3. Documentación (tests muestran uso esperado)
4. Regression prevention

Tipos de tests:
- Unit: Funciones individuales
- Integration: Módulos trabajando juntos
- End-to-end: Pipeline completo
- Data quality: Validación de outputs

En Databricks:
- Databricks Asset Bundles CI/CD
- Delta Live Tables expectations
- Great Expectations para data quality
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from requests.exceptions import RequestException
import json

# Importamos módulos a testear
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.fetch import APIFetcher
from src.state import StateManager
from src.transform import EventTransformer
from src.metrics import MetricsCalculator


class TestAPIFetcher(unittest.TestCase):
    """
    CONCEPTO: Unit Testing
    ======================
    Testeamos APIFetcher en aislamiento.
    No queremos hacer requests reales a GitHub (lentos, rate limits).
    Usamos MOCKS.
    """
    
    def setUp(self):
        """
        CONCEPTO: Test Fixtures
        Setup que corre antes de cada test.
        """
        self.fetcher = APIFetcher(
            base_url="https://api.github.com",
            timeout=5,
            max_retries=2
        )
    
    @patch('src.fetch.requests.Session.get')
    def test_fetch_events_success(self, mock_get):
        """
        CONCEPTO: Mocking External Dependencies
        ========================================
        Mockeamos requests.get para no hacer HTTP calls reales.
        
        Esto nos permite:
        - Tests rápidos (no espera red)
        - Tests determinísticos (misma respuesta siempre)
        - Tests sin dependencias externas
        """
        # ARRANGE: Preparar mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'X-RateLimit-Remaining': '59'}
        mock_response.json.return_value = [
            {
                'id': '12345',
                'type': 'PushEvent',
                'created_at': '2024-01-15T10:00:00Z',
                'repo': {'id': 1, 'name': 'test/repo'},
                'actor': {'id': 1, 'login': 'user1'},
                'payload': {}
            }
        ]
        mock_get.return_value = mock_response
        
        # ACT: Ejecutar código a testear
        events = self.fetcher.fetch_events('test', 'repo')
        
        # ASSERT: Verificar resultados
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['id'], '12345')
        self.assertEqual(events[0]['type'], 'PushEvent')
        
        # Verificar que se agregó metadata de ingesta
        self.assertIn('_ingestion_timestamp', events[0])
        self.assertIn('_source', events[0])
    
    @patch('src.fetch.requests.Session.get')
    def test_fetch_events_304_not_modified(self, mock_get):
        """
        Test del caso incremental: no hay datos nuevos.
        """
        mock_response = Mock()
        mock_response.status_code = 304  # Not Modified
        mock_get.return_value = mock_response
        
        events = self.fetcher.fetch_events('test', 'repo', since='2024-01-15T00:00:00Z')
        
        # Debería devolver lista vacía (no hay datos nuevos)
        self.assertEqual(len(events), 0)
    
    @patch('src.fetch.requests.Session.get')
    def test_fetch_events_retry_on_failure(self, mock_get):
        """
        CONCEPTO: Testing Retry Logic
        ==============================
        Verificamos que el retry funciona correctamente.
        """
        # Primera llamada falla, segunda funciona
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = [
            {'id': '1', 'type': 'PushEvent', 'created_at': '2024-01-15T10:00:00Z',
            'repo': {'id': 1, 'name': 'test/repo'},
            'actor': {'id': 1, 'login': 'user1'},
            'payload': {}}
        ]
        
        # Primera llamada falla con RequestException, segunda funciona
        mock_get.side_effect = [
            RequestException("Network error"),
            mock_response
        ]

        events = self.fetcher.fetch_events('test', 'repo')

        assert len(events) == 1
        assert events[0]['id'] == '1'
        assert mock_get.call_count == 2


class TestStateManager(unittest.TestCase):
    """
    Testing del state management (checkpoint).
    """
    
    def setUp(self):
        """
        CONCEPTO: Temporary Test Data
        =============================
        Usamos directorio temporal para tests.
        No queremos contaminar data/ real.
        """
        import tempfile
        self.test_dir = tempfile.mkdtemp()
        self.state_manager = StateManager(state_dir=self.test_dir)
    
    def tearDown(self):
        """
        CONCEPTO: Test Cleanup
        Limpiamos después de cada test.
        """
        import shutil
        shutil.rmtree(self.test_dir)
    
    def test_bootstrap_mode_no_checkpoint(self):
        """
        Test: primera ejecución, no hay checkpoint.
        """
        timestamp = self.state_manager.get_last_processed_timestamp()
        
        # Primera vez: debe ser None
        self.assertIsNone(timestamp)
    
    def test_save_and_load_checkpoint(self):
        """
        CONCEPTO: Round-trip Testing
        ============================
        Guardamos y cargamos, verificamos que son iguales.
        """
        # Save checkpoint
        test_timestamp = "2024-01-15T10:00:00Z"
        self.state_manager.save_checkpoint(
            last_timestamp=test_timestamp,
            records_processed=100,
            status="success"
        )
        
        # Load checkpoint
        loaded_timestamp = self.state_manager.get_last_processed_timestamp()
        
        # Verificar
        self.assertEqual(loaded_timestamp, test_timestamp)
    
    def test_checkpoint_survives_multiple_saves(self):
        """
        Test: el checkpoint se actualiza correctamente.
        """
        # Primera ejecución
        self.state_manager.save_checkpoint("2024-01-15T10:00:00Z", 100, "success")
        ts1 = self.state_manager.get_last_processed_timestamp()
        
        # Segunda ejecución (incremental)
        self.state_manager.save_checkpoint("2024-01-15T11:00:00Z", 50, "success")
        ts2 = self.state_manager.get_last_processed_timestamp()
        
        # El timestamp debe haber avanzado
        self.assertGreater(ts2, ts1)


class TestEventTransformer(unittest.TestCase):
    """
    Testing de transformaciones de datos.
    """
    
    def setUp(self):
        self.transformer = EventTransformer(
            event_types_filter=['PushEvent', 'PullRequestEvent']
        )
    
    def test_clean_event_valid(self):
        """
        Test: evento válido se limpia correctamente.
        """
        raw_event = {
            'id': '12345',
            'type': 'PushEvent',
            'created_at': '2024-01-15T10:00:00Z',
            'repo': {'id': 1, 'name': 'apache/spark', 'url': 'https://...'},
            'actor': {'id': 1, 'login': 'contributor1'},
            'payload': {'size': 3, 'commits': []},
            '_ingestion_timestamp': '2024-01-15T10:01:00Z',
            '_source': 'github_api'
        }
        
        cleaned = self.transformer.clean_event(raw_event)
        
        # Verificar campos requeridos
        self.assertIsNotNone(cleaned)
        self.assertEqual(cleaned['event_id'], '12345')
        self.assertEqual(cleaned['event_type'], 'PushEvent')
        self.assertEqual(cleaned['repo_name'], 'apache/spark')
        
        # Verificar campos agregados
        self.assertIn('event_date', cleaned)
        self.assertIn('event_hour', cleaned)
        self.assertIn('_content_hash', cleaned)
    
    def test_clean_event_missing_required_field(self):
        """
        CONCEPTO: Data Quality Testing
        ===============================
        Verificamos que eventos inválidos se rechazan.
        """
        invalid_event = {
            'id': '12345',
            # FALTA 'type' (campo requerido)
            'created_at': '2024-01-15T10:00:00Z',
        }
        
        cleaned = self.transformer.clean_event(invalid_event)
        
        # Debe devolver None (evento rechazado)
        self.assertIsNone(cleaned)
    
    def test_clean_event_filters_by_type(self):
        """
        Test: filtro por tipo de evento funciona.
        """
        # Este tipo NO está en el filtro
        event = {
            'id': '12345',
            'type': 'WatchEvent',  # No en filter
            'created_at': '2024-01-15T10:00:00Z',
            'repo': {'id': 1, 'name': 'test/repo'},
            'actor': {'id': 1, 'login': 'user1'}
        }
        
        cleaned = self.transformer.clean_event(event)
        
        # Debe ser filtrado (None)
        self.assertIsNone(cleaned)
    
    def test_deduplication_in_batch(self):
        """
        CONCEPTO: Testing Deduplication
        ================================
        Verificamos que duplicados se eliminan.
        """
        # Mismo evento 3 veces
        event = {
            'id': '12345',
            'type': 'PushEvent',
            'created_at': '2024-01-15T10:00:00Z',
            'repo': {'id': 1, 'name': 'test/repo'},
            'actor': {'id': 1, 'login': 'user1'},
            'payload': {}
        }
        
        batch = [event.copy(), event.copy(), event.copy()]
        
        result = self.transformer.transform_batch(batch)
        
        # Solo debe quedar 1 (deduplicados los otros 2)
        self.assertEqual(len(result), 1)
    
    def test_enrich_event_adds_features(self):
        """
        Test: enriquecimiento agrega features calculadas.
        """
        event = {
            'event_type': 'PushEvent',
            'event_hour': 10,
            'payload': {'size': 3}
        }
        
        enriched = self.transformer.enrich_event(event)
        
        # Verificar features agregadas
        self.assertIn('activity_category', enriched)
        self.assertEqual(enriched['activity_category'], 'code_change')
        
        self.assertIn('time_of_day', enriched)
        self.assertEqual(enriched['time_of_day'], 'morning')
        
        self.assertIn('is_active_contribution', enriched)
        self.assertTrue(enriched['is_active_contribution'])


class TestMetricsCalculator(unittest.TestCase):
    """
    Testing de cálculo de métricas.
    """
    
    def setUp(self):
        self.calculator = MetricsCalculator()
        
        # CONCEPTO: Test Fixtures (datos de prueba reutilizables)
        self.sample_events = [
            {
                'event_type': 'PushEvent',
                'repo_name': 'apache/spark',
                'actor_login': 'user1',
                'created_at': '2024-01-15T10:00:00Z',
                'event_date': '2024-01-15',
                'event_hour': 10,
                'is_active_contribution': True,
                'activity_category': 'code_change',
                'time_of_day': 'morning'
            },
            {
                'event_type': 'PullRequestEvent',
                'repo_name': 'apache/spark',
                'actor_login': 'user2',
                'created_at': '2024-01-15T11:00:00Z',
                'event_date': '2024-01-15',
                'event_hour': 11,
                'is_active_contribution': True,
                'activity_category': 'code_review',
                'time_of_day': 'morning'
            },
            {
                'event_type': 'PushEvent',
                'repo_name': 'delta-io/delta',
                'actor_login': 'user1',
                'created_at': '2024-01-15T14:00:00Z',
                'event_date': '2024-01-15',
                'event_hour': 14,
                'is_active_contribution': True,
                'activity_category': 'code_change',
                'time_of_day': 'afternoon'
            }
        ]
    
    def test_calculate_event_type_metrics(self):
        """
        Test: agregación por tipo de evento.
        """
        result = self.calculator.calculate_event_type_metrics(self.sample_events)
        
        self.assertEqual(result['PushEvent'], 2)
        self.assertEqual(result['PullRequestEvent'], 1)
    
    def test_calculate_repo_metrics(self):
        """
        Test: métricas por repositorio.
        """
        result = self.calculator.calculate_repo_metrics(self.sample_events)
        
        # apache/spark tiene 2 eventos
        spark_stats = result['apache/spark']
        self.assertEqual(spark_stats['total_events'], 2)
        self.assertEqual(spark_stats['unique_contributors'], 2)
        
        # delta-io/delta tiene 1 evento
        delta_stats = result['delta-io/delta']
        self.assertEqual(delta_stats['total_events'], 1)
    
    def test_calculate_time_series_metrics(self):
        """
        Test: agregación temporal.
        """
        result = self.calculator.calculate_time_series_metrics(self.sample_events)
        
        # Todos los eventos son del mismo día
        daily = result['daily']
        self.assertIn('2024-01-15', daily)
        self.assertEqual(daily['2024-01-15']['event_count'], 3)
    
    def test_empty_events_handling(self):
        """
        CONCEPTO: Edge Case Testing
        ============================
        Testeamos casos límite: lista vacía.
        """
        result = self.calculator.calculate_all_metrics([])
        
        # No debe crashear, debe devolver dict vacío
        self.assertIsInstance(result, dict)


class TestPipelineIntegration(unittest.TestCase):
    """
    CONCEPTO: Integration Testing
    ==============================
    Testeamos múltiples componentes trabajando juntos.
    """
    
    def test_end_to_end_transformation(self):
        """
        Test: flujo completo desde raw event hasta metrics.
        """
        # 1. Raw event (como viene de API)
        raw_event = {
            'id': '12345',
            'type': 'PushEvent',
            'created_at': '2024-01-15T10:00:00Z',
            'repo': {'id': 1, 'name': 'apache/spark'},
            'actor': {'id': 1, 'login': 'contributor1'},
            'payload': {'size': 3, 'commits': []},
            '_ingestion_timestamp': '2024-01-15T10:01:00Z',
            '_source': 'github_api'
        }
        
        # 2. Transform
        transformer = EventTransformer()
        cleaned = transformer.clean_event(raw_event)
        enriched = transformer.enrich_event(cleaned)
        
        # 3. Calculate metrics
        calculator = MetricsCalculator()
        metrics = calculator.calculate_all_metrics([enriched])
        
        # 4. Verify end-to-end
        self.assertIsNotNone(enriched)
        self.assertIn('summary', metrics)
        self.assertEqual(metrics['summary']['total_events'], 1)


# CONCEPTO: Test Runner
if __name__ == '__main__':
    # Ejecuta todos los tests
    unittest.main(verbosity=2)