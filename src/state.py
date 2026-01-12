"""
state.py - Checkpoint Management Module

CONCEPTO FUNDAMENTAL: Stateful Processing
==========================================
Un pipeline de datos necesita "recordar" dónde se quedó.
Esto se llama "checkpoint" o "state management".

Analogía: Es como marcar la página de un libro.
- Cierras el libro (pipeline termina)
- Volvés más tarde (próxima ejecución)
- Abrís en la página marcada (checkpoint)
- No releés todo desde el principio

En Databricks:
- Delta Lake maneja esto automáticamente con ACID transactions
- Structured Streaming usa checkpoints en cloud storage
- Nosotros lo simulamos con archivos JSON (simple pero efectivo)
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
    CONCEPTO: State Management
    ==========================
    Maneja el estado del pipeline entre ejecuciones.
    
    ¿Qué guardamos?
    - Timestamp del último dato procesado exitosamente
    - Contadores (records procesados, errores)
    - Metadata de la ejecución (start_time, end_time, status)
    
    ¿Por qué es importante?
    1. **Incremental processing**: Solo procesamos lo nuevo
    2. **Fault tolerance**: Si falla, sabemos dónde retomar
    3. **Idempotencia**: Múltiples ejecuciones = mismo resultado
    4. **Auditoría**: Sabemos qué se procesó y cuándo
    """
    
    def __init__(self, state_dir: str = "data/state"):
        """
        Args:
            state_dir: Directorio donde guardamos checkpoints
        
        BEST PRACTICE: Separation of data by purpose
        - data/raw: Datos crudos de la API
        - data/curated: Datos limpiados
        - data/state: Metadata del pipeline
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / "checkpoint.json"
        
    def load_checkpoint(self) -> Optional[Dict]:
        """
        Carga el último checkpoint guardado.
        
        CONCEPTO: Checkpoint Recovery
        =============================
        Si el pipeline corrió antes, tenemos un checkpoint.
        Si es la primera vez, devolvemos None (bootstrap mode).
        
        Returns:
            Dict con el estado, o None si no existe
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
            # CONCEPTO: Fail-safe behavior
            # Si el checkpoint está corrupto, es más seguro empezar de nuevo
            # que procesar datos duplicados
            return None
    
    def save_checkpoint(
        self, 
        last_timestamp: str,
        records_processed: int,
        status: str = "success",
        metadata: Optional[Dict] = None
    ):
        """
        Guarda un nuevo checkpoint.
        
        CONCEPTO: ACID Properties (Atomicity)
        =====================================
        Queremos que el guardado sea atómico:
        - Se guarda todo o nada
        - No queremos checkpoints corruptos o a medias
        
        Técnica: Write-then-rename
        1. Escribimos a archivo temporal
        2. Si todo OK, renombramos a archivo final
        3. Rename es operación atómica en filesystems
        
        En Databricks: Delta Lake hace esto automáticamente con transaction log
        
        Args:
            last_timestamp: Último timestamp procesado exitosamente
            records_processed: Cantidad de registros procesados
            status: success | failed | partial
            metadata: Cualquier data adicional (ej: errores, warnings)
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
        Obtiene el último timestamp procesado.
        
        CONCEPTO: Incremental Load Pattern
        ===================================
        Esta es la función clave para incremental processing:
        1. Obtenemos último timestamp procesado
        2. Le decimos a la API: "dame todo DESPUÉS de esto"
        3. Procesamos solo lo nuevo
        4. Guardamos nuevo checkpoint
        
        Returns:
            ISO 8601 timestamp string, o None si es primera ejecución
        """
        checkpoint = self.load_checkpoint()
        
        if checkpoint is None:
            return None
        
        return checkpoint.get('last_processed_timestamp')
    
    def mark_failed(self, error_message: str):
        """
        Marca una ejecución como fallida sin avanzar el checkpoint.
        
        CONCEPTO: Fault Tolerance
        =========================
        Si el pipeline falla:
        - NO avanzamos el checkpoint
        - Guardamos el error para debugging
        - Próxima ejecución reintentará desde el mismo punto
        
        Esto garantiza que no perdemos datos si algo falla.
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
        Obtiene estadísticas del pipeline.
        
        CONCEPTO: Observability
        =======================
        En producción necesitamos saber:
        - ¿Cuándo corrió por última vez?
        - ¿Cuántos registros procesó?
        - ¿Hubo errores?
        
        Esto se usa para:
        - Dashboards de monitoring
        - Alertas (ej: "hace 24hs que no corre")
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
    CONCEPTO: Execution Tracking
    =============================
    Además del checkpoint (estado del pipeline),
    queremos trackear cada ejecución individual.
    
    Esto es útil para:
    - Ver histórico de ejecuciones
    - Detectar degradación de performance
    - Auditoría completa
    
    En Databricks: Esto lo hace Databricks Workflows automáticamente
    """
    
    def __init__(self, state_dir: str = "data/state"):
        self.state_dir = Path(state_dir)
        self.runs_file = self.state_dir / "run_history.jsonl"
    
    def start_run(self) -> str:
        """
        Inicia tracking de una ejecución.
        
        Returns:
            run_id: UUID único para esta ejecución
        """
        import uuid
        run_id = str(uuid.uuid4())
        
        self.run_id = run_id
        self.start_time = datetime.utcnow()
        
        logger.info(f"Started run {run_id}")
        return run_id
    
    def end_run(self, records_processed: int, status: str, error: Optional[str] = None):
        """
        Finaliza tracking de una ejecución.
        
        BEST PRACTICE: JSONL format
        ===========================
        Usamos JSONL (JSON Lines) en lugar de JSON array porque:
        - Cada línea es un JSON independiente
        - Append-only (no necesitamos reescribir todo el archivo)
        - Fácil de procesar en streaming
        - Compatible con herramientas como jq, pandas, Spark
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