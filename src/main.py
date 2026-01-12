"""
main.py - Pipeline Orchestrator

CONCEPTO FUNDAMENTAL: Orchestration
===================================
Este es el "director de orquesta" del pipeline.
Coordina todos los módulos para ejecutar el flujo completo:

1. Cargar configuración
2. Cargar checkpoint (¿dónde nos quedamos?)
3. Fetch datos incrementales de API
4. Transform (limpiar y validar)
5. Calculate metrics
6. Persist resultados
7. Save checkpoint

En Databricks:
- Databricks Workflows orquesta notebooks
- Delta Live Tables orquesta pipelines declarativamente
- Jobs API para programar ejecuciones
"""

import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# Importamos nuestros módulos
from fetch import APIFetcher
from state import StateManager, RunTracker
from transform import EventTransformer
from metrics import MetricsCalculator

# BEST PRACTICE: Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Pipeline:
    """
    CONCEPTO: Pipeline Pattern
    ==========================
    Encapsula toda la lógica del pipeline en una clase.
    
    Ventajas:
    - Estado centralizado (config, managers)
    - Fácil de testear (mock dependencies)
    - Reutilizable (diferentes configs)
    - Orchestrable (puede ser llamado por scheduler)
    """
    
    def __init__(self, config: Dict):
        """
        Args:
            config: Configuración del pipeline (viene de config.jsonnet)
        """
        self.config = config
        
        # CONCEPTO: Dependency Injection
        # Inyectamos dependencias en lugar de crearlas aquí
        # Esto facilita testing (podemos inyectar mocks)
        self.fetcher = APIFetcher(
            base_url=config['api']['base_url'],
            timeout=config['api']['retry']['timeout'],
            max_retries=config['api']['retry']['max_retries']
        )
        
        self.state_manager = StateManager(
            state_dir=config['paths']['state']
        )
        
        self.run_tracker = RunTracker(
            state_dir=config['paths']['state']
        )
        
        self.transformer = EventTransformer(
            event_types_filter=config['transform']['event_types_filter']
        )
        
        self.metrics_calculator = MetricsCalculator()
        
        # Crear directorios si no existen
        self._setup_directories()
    
    def _setup_directories(self):
        """
        BEST PRACTICE: Idempotent Setup
        Crear directorios es una operación idempotente.
        Podemos ejecutarla múltiples veces sin problemas.
        """
        for path_key, path_value in self.config['paths'].items():
            Path(path_value).mkdir(parents=True, exist_ok=True)
        
        logger.info("Pipeline directories initialized")
    
    def _save_json(self, data: any, filepath: Path):
        """
        BEST PRACTICE: Atomic Write
        Guardamos a archivo temporal primero, luego rename.
        Esto previene archivos corruptos si el proceso muere.
        """
        temp_file = filepath.with_suffix('.tmp')
        
        try:
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Atomic rename
            temp_file.replace(filepath)
            
        except Exception as e:
            logger.error(f"Failed to save {filepath}: {e}")
            if temp_file.exists():
                temp_file.unlink()
            raise
    
    def run(self) -> Dict:
        """
        Ejecuta el pipeline completo.
        
        CONCEPTO: Transaction-like Execution
        ====================================
        El pipeline funciona como una transacción:
        1. Start run (tracking)
        2. Execute steps
        3. Si todo OK: commit (save checkpoint)
        4. Si falla: rollback (no avanzar checkpoint)
        
        Returns:
            Dict con estadísticas de la ejecución
        """
        run_id = self.run_tracker.start_run()
        logger.info(f"=== Pipeline Started (run_id: {run_id}) ===")
        
        stats = {
            'run_id': run_id,
            'status': 'failed',  # Pessimistic default
            'records_processed': 0,
            'error': None
        }
        
        try:
            # STEP 1: Determinar punto de inicio (incremental)
            last_timestamp = self.state_manager.get_last_processed_timestamp()
            
            if last_timestamp:
                logger.info(f"Incremental run: fetching events after {last_timestamp}")
            else:
                logger.info("Bootstrap run: fetching all available events")
            
            # STEP 2: Fetch raw data (Bronze)
            logger.info("Step 1/5: Fetching events from API...")
            raw_events = self._fetch_events(last_timestamp)
            
            if not raw_events:
                logger.info("No new events to process")
                stats['status'] = 'success'
                stats['records_processed'] = 0
                return stats
            
            logger.info(f"Fetched {len(raw_events)} events")
            
            # STEP 3: Transform data (Bronze → Silver)
            logger.info("Step 2/5: Transforming events...")
            transformed_events = self._transform_events(raw_events)
            logger.info(f"Transformed {len(transformed_events)} events")
            
            # STEP 4: Calculate metrics (Silver → Gold)
            logger.info("Step 3/5: Calculating metrics...")
            metrics = self._calculate_metrics(transformed_events)
            logger.info("Metrics calculated")
            
            # STEP 5: Persist results
            logger.info("Step 4/5: Persisting results...")
            self._persist_results(raw_events, transformed_events, metrics)
            logger.info("Results persisted")
            
            # STEP 6: Save checkpoint (commit)
            logger.info("Step 5/5: Saving checkpoint...")
            self._save_checkpoint(transformed_events)
            logger.info("Checkpoint saved")
            
            # Success!
            stats['status'] = 'success'
            stats['records_processed'] = len(transformed_events)
            
            logger.info(f"=== Pipeline Completed Successfully ===")
            logger.info(f"Processed {stats['records_processed']} events")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            stats['error'] = str(e)
            
            # CONCEPTO: Failure Handling
            # Marcamos el fallo pero NO avanzamos el checkpoint
            # Próxima ejecución reintentará desde el mismo punto
            self.state_manager.mark_failed(str(e))
        
        finally:
            # CONCEPTO: Cleanup & Finalization
            # Siempre guardamos el tracking, incluso si falla
            self.run_tracker.end_run(
                records_processed=stats['records_processed'],
                status=stats['status'],
                error=stats.get('error')
            )
        
        return stats
    
    def _fetch_events(self, since: str = None) -> List[Dict]:
        """
        Fetches events from configured repos.
        
        CONCEPTO: Batch Fetch
        =====================
        En producción real, esto podría:
        - Paralelizarse (multiprocessing/threading)
        - Usar connection pooling
        - Implementar circuit breaker pattern
        """
        repos = [(r['owner'], r['name']) for r in self.config['repos']]
        
        all_events_by_repo = self.fetcher.fetch_multiple_repos(repos, since=since)
        
        # Flatten: Dict[repo -> events] → List[events]
        all_events = []
        for repo, events in all_events_by_repo.items():
            all_events.extend(events)
        
        # CONCEPTO: Data Validation
        # Verificamos que recibimos datos válidos
        if all_events:
            logger.info(f"Fetched events date range: {min(e['created_at'] for e in all_events)} to {max(e['created_at'] for e in all_events)}")
        
        # Guardamos raw data (Bronze layer)
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        raw_file = Path(self.config['paths']['raw']) / f'events_{timestamp}.json'
        self._save_json(all_events, raw_file)
        logger.info(f"Raw data saved to {raw_file}")
        
        return all_events
    
    def _transform_events(self, events: List[Dict]) -> List[Dict]:
        """
        Transforms events (cleaning, validation, enrichment).
        """
        # Transform batch
        transformed = self.transformer.transform_batch(events)
        
        # Enrich each event
        enriched = [self.transformer.enrich_event(e) for e in transformed]
        
        # Guardamos curated data (Silver layer)
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        curated_file = Path(self.config['paths']['curated']) / f'events_curated_{timestamp}.json'
        self._save_json(enriched, curated_file)
        logger.info(f"Curated data saved to {curated_file}")
        
        return enriched
    
    def _calculate_metrics(self, events: List[Dict]) -> Dict:
        """
        Calculates aggregated metrics.
        """
        metrics = self.metrics_calculator.calculate_all_metrics(events)
        
        # CONCEPTO: Metrics Enrichment
        # Agregamos top rankings
        metrics['rankings'] = {
            'top_repos': self.metrics_calculator.get_top_repos(
                metrics, 
                top_n=self.config['metrics']['top_n']
            ),
            'top_contributors': self.metrics_calculator.get_top_contributors(
                metrics,
                top_n=self.config['metrics']['top_n']
            )
        }
        
        return metrics
    
    def _persist_results(self, raw: List[Dict], curated: List[Dict], metrics: Dict):
        """
        Persists final results.
        
        CONCEPTO: Multiple Output Formats
        ==================================
        Guardamos en diferentes formatos según uso:
        - JSON: Fácil de leer, debugging
        - CSV: Para análisis en Excel/pandas
        - Parquet: Formato columnar eficiente (en producción)
        
        En Databricks: Delta Lake maneja esto automáticamente
        """
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        # Metrics to JSON
        metrics_file = Path(self.config['paths']['metrics']) / f'metrics_{timestamp}.json'
        self._save_json(metrics, metrics_file)
        logger.info(f"Metrics saved to {metrics_file}")
        
        # OPCIONAL: Guardar también latest metrics (para dashboards)
        latest_metrics_file = Path(self.config['paths']['metrics']) / 'latest_metrics.json'
        self._save_json(metrics, latest_metrics_file)
    
    def _save_checkpoint(self, events: List[Dict]):
        """
        Saves checkpoint for incremental processing.
        
        CONCEPTO: Watermark
        ===================
        El checkpoint guarda la "marca de agua" (watermark):
        el último timestamp procesado exitosamente.
        
        En Structured Streaming, esto se llama "watermark" y
        se usa para windowed aggregations y late data handling.
        """
        if not events:
            return
        
        # IMPORTANTE: Usamos el timestamp MÁS RECIENTE procesado
        # Esto garantiza que próxima ejecución no reprocesa
        last_timestamp = max(e['created_at'] for e in events)
        
        self.state_manager.save_checkpoint(
            last_timestamp=last_timestamp,
            records_processed=len(events),
            status='success'
        )


def load_config(env: str = 'dev') -> Dict:
    """
    Loads configuration from jsonnet file.
    
    CONCEPTO: Configuration Loading
    ================================
    En producción, esto podría:
    - Leer de environment variables
    - Usar secret managers (AWS Secrets Manager, Azure Key Vault)
    - Merge múltiples fuentes de config
    """
    import subprocess
    
    try:
        # Compile jsonnet to JSON
        result = subprocess.run(
            ['jsonnet', 'config/config.jsonnet'],
            capture_output=True,
            text=True,
            check=True
        )
        
        configs = json.loads(result.stdout)
        return configs[env]
        
    except subprocess.CalledProcessError:
        logger.warning("jsonnet not installed, using default config")
        
        # Fallback: default config
        return {
            'api': {
                'base_url': 'https://api.github.com',
                'retry': {'max_retries': 3, 'timeout': 10}
            },
            'paths': {
                'raw': 'data/raw',
                'curated': 'data/curated',
                'metrics': 'data/metrics',
                'state': 'data/state'
            },
            'repos': [
                {'owner': 'apache', 'name': 'spark'},
                {'owner': 'delta-io', 'name': 'delta'}
            ],
            'transform': {
                'event_types_filter': ['PushEvent', 'PullRequestEvent', 'IssuesEvent']
            },
            'metrics': {
                'calculations': ['event_types', 'repos', 'contributors'],
                'top_n': 10
            }
        }


def main():
    """
    Entry point for the pipeline.
    
    CONCEPTO: CLI Interface
    =======================
    El pipeline se puede ejecutar desde command line:
    python main.py
    python main.py --env prod
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='GitHub Events Pipeline')
    parser.add_argument(
        '--env',
        choices=['dev', 'prod'],
        default='dev',
        help='Environment (dev or prod)'
    )
    
    args = parser.parse_args()
    
    # Load config
    logger.info(f"Loading configuration for environment: {args.env}")
    config = load_config(args.env)
    
    # Run pipeline
    pipeline = Pipeline(config)
    stats = pipeline.run()
    
    # Exit with appropriate code
    # BEST PRACTICE: Exit codes para automation
    # 0 = success, 1 = failure
    sys.exit(0 if stats['status'] == 'success' else 1)


if __name__ == '__main__':
    main()