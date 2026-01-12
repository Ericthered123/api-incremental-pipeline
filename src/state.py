"""
state.py - Pipeline State Management

CONCEPTO FUNDAMENTAL: Checkpointing
==================================
El estado permite que el pipeline sea:
- Incremental
- Idempotente
- Fault-tolerant

Guardamos el último timestamp procesado exitosamente.
Si el pipeline falla, retomamos desde ahí.
"""

import json
import os
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class StateManager:
    """
    CONCEPTO: Control Plane
    ======================
    Este componente NO procesa datos.
    Controla el estado del pipeline.
    """

    def __init__(self, state_dir: str):
        self.state_dir = state_dir
        os.makedirs(state_dir, exist_ok=True)
        self.state_file = os.path.join(state_dir, "checkpoint.json")

    def get_last_processed_timestamp(self) -> Optional[str]:
        """
        Devuelve el último timestamp procesado (ISO 8601).

        Returns:
            str | None
        """
        if not os.path.exists(self.state_file):
           if not os.path.exists(self.state_file):
            return None

        with open(self.state_file, "r") as f:
            state = json.load(f)

        return state["last_processed_at"]

    def save_checkpoint(self, last_timestamp: str, records_processed: int, status: str):
        """
        Guarda el checkpoint solo cuando el pipeline terminó OK.

        CONCEPTO: Exactly-once semantics (best effort)
        """
        state = {
            "last_processed_at": last_timestamp,
            "records_processed": records_processed,
            "status": status,
            "updated_at": datetime.utcnow().isoformat()
        }

        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=2)

    def mark_failed(self, error: str):
        logger.error(f"Run failed: {error}")
class RunTracker:
    """
    CONCEPTO: Run-level metadata tracking
    ====================================
    Guarda información de cada ejecución del pipeline.
    """

    def __init__(self, state_dir: str):
        self.state_dir = state_dir
        os.makedirs(state_dir, exist_ok=True)
        self.history_file = os.path.join(state_dir, "run_history.jsonl")
        self.current_run = None

    def start_run(self) -> str:
        run_id = datetime.utcnow().isoformat()
        self.current_run = {
            "run_id": run_id,
            "started_at": datetime.utcnow().isoformat(),
            "status": "running"
        }
        return run_id

    def end_run(self, records_processed: int, status: str, error: str = None):
        self.current_run.update({
            "ended_at": datetime.utcnow().isoformat(),
            "records_processed": records_processed,
            "status": status,
            "error": error
        })

        with open(self.history_file, "a") as f:
            f.write(json.dumps(self.current_run) + "\n")