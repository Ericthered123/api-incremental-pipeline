"""
main.py - Pipeline Orchestrator

FUNDAMENTAL CONCEPT: Orchestration
====================================
This is the "conductor" of the pipeline. It coordinates all modules to execute the complete flow:

1. Load configuration
2. Load checkpoint (where were we?)
3. Fetch incremental data from the API
4. Transform (clean and validate)
5. Calculate metrics
6. Persist results
7. Save checkpoint

In Databricks:
- Databricks Workflows orchestrate notebooks
- Delta Live Tables orchestrate pipelines declaratively
- Jobs API for scheduling executions
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
    CONCEPT: Pipeline Pattern
    ==========================
    Encapsulates all pipeline logic in a single class.

    Advantages:

    - Centralized state (config, managers)

    - Easy to test (mock dependencies)

    - Reusable (different configurations)

    - Orchestrable (can be called by a scheduler)
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
        Creating directories is an idempotent operation.
        We can run it multiple times without problems.
        """
        for path_key, path_value in self.config['paths'].items():
            Path(path_value).mkdir(parents=True, exist_ok=True)
        
        logger.info("Pipeline directories initialized")
    
    def _save_json(self, data: any, filepath: Path):
        """
        BEST PRACTICE: Atomic Write
        We save to a temporary file first, then rename it.

        This prevents corrupted files if the process terminates.
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
        Execute the entire pipeline.

            CONCEPT: Transaction-like Execution

            ===================================
            The pipeline functions like a transaction:

            1. Start run (tracking)

            2. Execute steps

            3. If everything is OK: commit (save checkpoint)

            4. If it fails: rollback (do not advance to the checkpoint)

            Returns:
            Dict with execution statistics
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
            
            # CONCEPT: Failure Handling

            # We mark the failure but do NOT advance the checkpoint

            # The next execution will retry from the same point
            self.state_manager.mark_failed(str(e))
        
        finally:
            # CONCEPT: Cleanup & Finalization

            # We always keep the tracking data, even if it fails
            self.run_tracker.end_run(
                records_processed=stats['records_processed'],
                status=stats['status'],
                error=stats.get('error')
            )
        
        return stats
    
    def _fetch_events(self, since: str = None) -> List[Dict]:
        """
        Fetches events from configured repositories.

            CONCEPT: Batch Fetch

            ====================
            In a real production environment, this could:

            - Be parallelized (multiprocessing/threading)

            - Use connection pooling

            - Implement a circuit breaker pattern
        """
        repos = [(r['owner'], r['name']) for r in self.config['repos']]
        
        all_events_by_repo = self.fetcher.fetch_multiple_repos(repos, since=since)
        
        # Flatten: Dict[repo -> events] → List[events]
        all_events = []
        for repo, events in all_events_by_repo.items():
            all_events.extend(events)
        
        # CONCEPTO: Data Validation
        # We verified that we received valid data
        if all_events:
            logger.info(f"Fetched events date range: {min(e['created_at'] for e in all_events)} to {max(e['created_at'] for e in all_events)}")
        
        # We save raw data (Bronze layer)
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
        
        # We save curated data (Silver layer)
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

        CONCEPT: Multiple Output Formats

        =================================
        We save in different formats depending on the use:

        - JSON: Easy to read, debugging

        - CSV: For analysis in Excel/Pandas

        - Parquet: Efficient columnar format (in production)

        In Databricks: Delta Lake handles this automatically
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

        CONCEPT: Watermark

        ==================
        The checkpoint saves the "watermark":

        the last timestamp successfully processed.

        In Structured Streaming, this is called a "watermark" and

        is used for windowed aggregations and late data handling.
        """
        if not events:
            return
        
        #IMPORTANT: We use the most recently processed timestamp.

        # This ensures that the next execution does not reprocess.
        last_timestamp = max(e['created_at'] for e in events)
        
        self.state_manager.save_checkpoint(
            last_timestamp=last_timestamp,
            records_processed=len(events),
            status='success'
        )


def load_config(env: str = 'dev') -> Dict:
    """

    Loads configuration from a JSONnet file.

    CONCEPT: Configuration Loading

    ===============================
    In production, this could:

    - Read from environment variables

    - Use secret managers (AWS Secrets Manager, Azure Key Vault)

    - Merge multiple configuration sources
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

        CONCEPT: CLI Interface

        ======================
        The pipeline can be executed from the command line:

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
    # BEST PRACTICE: Exit codes for automation
    # 0 = success, 1 = failure
    sys.exit(0 if stats['status'] == 'success' else 1)


if __name__ == '__main__':
    main()