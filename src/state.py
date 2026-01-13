"""
state.py - Checkpoint Management Module

FUNDAMENTAL CONCEPT: Stateful Processing
===========================================
A data pipeline needs to "remember" where it left off.

This is called "checkpoints" or "state management."

Analogy: It's like marking a page in a book.

- You close the book (pipeline ends)
- You come back later (next execution)
- You open to the marked page (checkpoint)
- You don't reread everything from the beginning

In Databricks:
- Delta Lake handles this automatically with ACID transactions
- Structured Streaming uses checkpoints in cloud storage
- We simulate it with JSON files (simple but effective)
"""

import json
import os
from datetime import datetime
from typing import Optional, Dict
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class StateManager:
    """
    CONCEPT: State Management

        ==========================
        Manages the pipeline state between executions.

        What do we store?

        - Timestamp of the last successfully processed data

        - Counters (records processed, errors)

        - Execution metadata (start_time, end_time, status)

        Why is it important?

        1. **Incremental processing**: We only process new data

        2. **Fault tolerance**: If a process fails, we know where to resume

        3. **Idempotence**: Multiple executions = same result

        4. **Auditing**: We know what was processed and when
    """
    
    def __init__(self, state_dir: str = "data/state"):
        """
        Args:

        state_dir: Directory where checkpoints are stored

        BEST PRACTICE: Separation of data by purpose

        - data/raw: Raw API data

        - data/curated: Cleaned data

        - data/state: Pipeline metadata
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / "checkpoint.json"
        
    def load_checkpoint(self) -> Optional[Dict]:
        """
        Load the last saved checkpoint.

        CONCEPT: Checkpoint Recovery

        ============================
        If the pipeline ran before, we have a checkpoint.

        If it's the first time, we return None (bootstrap mode).

        Returns:
        Dict with the state, or None if it doesn't exist
        """
        if not self.state_file.exists():
            logger.info("No previous checkpoint found (bootstrap mode)")
            return None
        
        try:
            with open(self.state_file, 'r') as f:
                checkpoint = json.load(f)
            
            logger.info(f"Loaded checkpoint: last_processed={checkpoint.get('last_processed_timestamp')}")
            return checkpoint
            
        except json.JSONDecodeError as e:
            logger.error(f"Corrupted checkpoint file: {e}")
            # CONCEPT: Fail-safe behavior

        # If the checkpoint is corrupted, it's safer to start over than to process duplicate data
            return None
    
    def save_checkpoint(
        self, 
        last_timestamp: str,
        records_processed: int,
        status: str = "success",
        metadata: Optional[Dict] = None
    ):
        """
       Save a new checkpoint.

        CONCEPT: ACID Properties (Atomicity)

        ====================================
        We want the saving to be atomic:

        - Save everything or nothing

        - We don't want corrupted or incomplete checkpoints

        Technique: Write-then-rename

        1. Write to a temporary file

        2. If everything is OK, rename to a final file

        3. Renaming is an atomic operation in filesystems

        In Databricks: Delta Lake does this automatically with the transaction log

        Args:

        last_timestamp: Last timestamp successfully processed

        records_processed: Number of records processed

        status: success | failed | partial

        metadata: Any additional data (e.g., errors, warnings)
        """
        checkpoint = {
            'last_processed_timestamp': last_timestamp,
            'records_processed': records_processed,
            'execution_timestamp': datetime.utcnow().isoformat(),
            'status': status,
            'metadata': metadata or {}
        }
        
        # BEST PRACTICE: Atomic write (write-then-rename)
        temp_file = self.state_file.with_suffix('.tmp')
        
        try:
            # Escribimos a archivo temporal
            with open(temp_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
            
            # Rename atómico
            temp_file.replace(self.state_file)
            
            logger.info(f"Checkpoint saved: {records_processed} records, status={status}")
            
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
            # Limpiamos archivo temporal si quedó
            if temp_file.exists():
                temp_file.unlink()
            raise
    
    def get_last_processed_timestamp(self) -> Optional[str]:
        """
        Gets the last processed timestamp.

        CONCEPT: Incremental Load Pattern

        ==================================
        This is the key function for incremental processing:

        1. Get the last processed timestamp

        2. Tell the API: "give me everything AFTER this"

        3. Process only the new data

        4. Save the new checkpoint

        Returns:
        ISO 8601 timestamp string, or None if it's the first execution
        """
        checkpoint = self.load_checkpoint()
        
        if checkpoint is None:
            return None
        
        return checkpoint.get('last_processed_timestamp')
    
    def mark_failed(self, error_message: str):
        """
        Mark an execution as failed without advancing the checkpoint.

        CONCEPT: Fault Tolerance

        ========================
        If the pipeline fails:

        - We do NOT advance the checkpoint

        - We save the error for debugging

        - The next execution will retry from the same point

        This ensures that we do not lose data if something fails.
        """
        checkpoint = self.load_checkpoint() or {}
        
        checkpoint.update({
            'last_execution_timestamp': datetime.utcnow().isoformat(),
            'last_execution_status': 'failed',
            'error_message': error_message
        })
        
        # Guardamos sin cambiar last_processed_timestamp
        with open(self.state_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)
        
        logger.error(f"Execution marked as failed: {error_message}")
    
    def get_stats(self) -> Dict:
        """
        Obtains pipeline statistics.

        CONCEPT: Observability

        ======================
        In production, we need to know:

        - When did it last run?

        - How many records did it process?

        - Were there any errors?

        This is used for:

        - Monitoring dashboards

        - Alerts (e.g., "hasn't run in 24 hours")

        - Debugging
        """
        checkpoint = self.load_checkpoint()
        
        if checkpoint is None:
            return {
                'status': 'never_run',
                'total_executions': 0
            }
        
        return {
            'status': checkpoint.get('status'),
            'last_processed': checkpoint.get('last_processed_timestamp'),
            'last_execution': checkpoint.get('execution_timestamp'),
            'records_processed': checkpoint.get('records_processed'),
            'metadata': checkpoint.get('metadata', {})
        }


class RunTracker:
    """
   CONCEPT: Execution Tracking

        =============================
        In addition to the checkpoint (pipeline status),

        we want to track each individual execution.

        This is useful for:

        - Viewing execution history

        - Detecting performance degradation

        - Complete auditing

        In Databricks: Databricks Workflows does this automatically.
    """
    
    def __init__(self, state_dir: str = "data/state"):
        self.state_dir = Path(state_dir)
        self.runs_file = self.state_dir / "run_history.jsonl"
    
    def start_run(self) -> str:
        """
       Start tracking an execution.

        Returns:

        run_id: Unique UUID for this execution
        """
        import uuid
        run_id = str(uuid.uuid4())
        
        self.run_id = run_id
        self.start_time = datetime.utcnow()
        
        logger.info(f"Started run {run_id}")
        return run_id
    
    def end_run(self, records_processed: int, status: str, error: Optional[str] = None):
        """
        Ends tracking of an execution.

        BEST PRACTICE: JSONL format

        ==========================
        We use JSONL (JSON Lines) instead of JSON arrays because:

        - Each line is an independent JSON object

        - Append-only (we don't need to rewrite the entire file)

        - Easy to process in streaming

        - Compatible with tools like jq, pandas, and Spark
        """
        end_time = datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()
        
        run_record = {
            'run_id': self.run_id,
            'start_time': self.start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'records_processed': records_processed,
            'status': status,
            'error': error
        }
        
        # Append al archivo JSONL
        with open(self.runs_file, 'a') as f:
            f.write(json.dumps(run_record) + '\n')
        
        logger.info(f"Run {self.run_id} completed: {status}, {records_processed} records, {duration:.2f}s")


# TESTING: Demuestra el ciclo completo
if __name__ == "__main__":
    print("=== STATE MANAGEMENT DEMO ===\n")
    
    state = StateManager(state_dir="data/state")
    tracker = RunTracker(state_dir="data/state")
    
    # Primera ejecución (bootstrap)
    print("1. BOOTSTRAP RUN (primera ejecución)")
    last_ts = state.get_last_processed_timestamp()
    print(f"   Last processed: {last_ts}")  # Debería ser None
    
    run_id = tracker.start_run()
    # Simulamos procesamiento
    state.save_checkpoint(
        last_timestamp="2024-01-15T10:00:00Z",
        records_processed=100,
        status="success"
    )
    tracker.end_run(records_processed=100, status="success")
    
    # Segunda ejecución (incremental)
    print("\n2. INCREMENTAL RUN (segunda ejecución)")
    last_ts = state.get_last_processed_timestamp()
    print(f"   Last processed: {last_ts}")  # Debería ser 2024-01-15T10:00:00Z
    
    run_id = tracker.start_run()
    state.save_checkpoint(
        last_timestamp="2024-01-15T11:00:00Z",
        records_processed=50,
        status="success"
    )
    tracker.end_run(records_processed=50, status="success")
    
    # Estadísticas
    print("\n3. PIPELINE STATS")
    stats = state.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")