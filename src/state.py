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

    def load_checkpoint(self) -> Optional[str]:
        """
        Devuelve el último timestamp procesado (ISO 8601).

        Returns:
            str | None
        """
        if not os.path.exists(self.state_file):
            logger.info("No checkpoint found (first run)")
            return None

        with open(self.state_file, "r") as f:
            state = json.load(f)

        logger.info(f"Loaded checkpoint: {state['last_processed_at']}")
        return state["last_processed_at"]

    def save_checkpoint(self, timestamp: str):
        """
        Guarda el checkpoint solo cuando el pipeline terminó OK.

        CONCEPTO: Exactly-once semantics (best effort)
        """
        state = {
            "last_processed_at": timestamp,
            "updated_at": datetime.utcnow().isoformat()
        }

        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=2)

        logger.info(f"Checkpoint saved: {timestamp}")
