"""
transform.py - Data Transformation Module (Silver Layer)

CONCEPTO FUNDAMENTAL: Silver Layer
=================================
- Datos limpios
- Schema consistente
- Enriquecidos
- Listos para analytics

Equivalente en Spark:
- withColumn
- cast
- explode
- deduplication
"""

from typing import Dict, List
from datetime import datetime
import hashlib
import logging

logger = logging.getLogger(__name__)


class EventTransformer:
    """
    CONCEPTO: Canonical Data Model
    ==============================
    Convertimos eventos crudos de GitHub
    a un esquema consistente y analítico.
    """

    def transform(self, raw_events: List[Dict]) -> List[Dict]:
        """
        Transforma eventos crudos a eventos Silver.

        Steps:
        - Normalización
        - Enrichment
        - Deduplicación
        """
        if not raw_events:
            return []

        transformed = []
        seen_ids = set()

        for event in raw_events:
            clean = self._normalize_event(event)

            # Deduplicación por event_id
            if clean["event_id"] in seen_ids:
                continue

            seen_ids.add(clean["event_id"])
            transformed.append(clean)

        logger.info(f"Transformed {len(transformed)} events")
        return transformed

    def _normalize_event(self, event: Dict) -> Dict:
        """
        CONCEPTO: Schema enforcement
        =============================
        Garantiza que TODOS los eventos tengan
        las mismas columnas.
        """
        created_at = event["created_at"]
        created_dt = datetime.fromisoformat(created_at.replace("Z", ""))

        event_type = event["type"]

        normalized = {
            # Identidad
            "event_id": event["id"],
            "event_type": event_type,

            # Repo
            "repo_name": event["repo"]["name"],

            # Actor
            "actor_login": event.get("actor", {}).get("login"),

            # Tiempo
            "created_at": created_at,
            "event_date": created_dt.date().isoformat(),
            "event_hour": created_dt.hour,

            # Clasificaciones
            "activity_category": self._classify_activity(event_type),
            "time_of_day": self._time_of_day(created_dt.hour),

            # Flags
            "is_active_contribution": event_type in {
                "PushEvent",
                "PullRequestEvent",
                "IssuesEvent"
            },

            # Metadata lineage
            "_source": event["_source"],
            "_ingestion_timestamp": event["_ingestion_timestamp"],
            "_content_hash": self._hash_event(event)
        }

        # Enrichment específico por tipo
        if event_type == "PushEvent":
            normalized["push_commits_count"] = len(
                event.get("payload", {}).get("commits", [])
            )

        if event_type == "PullRequestEvent":
            pr = event.get("payload", {}).get("pull_request", {})
            normalized["pr_action"] = event.get("payload", {}).get("action")
            normalized["pr_merged"] = pr.get("merged")

        return normalized

    def _classify_activity(self, event_type: str) -> str:
        """
        CONCEPTO: Domain mapping
        """
        mapping = {
            "PushEvent": "code_change",
            "PullRequestEvent": "code_review",
            "IssuesEvent": "issue_tracking",
            "WatchEvent": "engagement",
            "ForkEvent": "engagement"
        }
        return mapping.get(event_type, "other")

    def _time_of_day(self, hour: int) -> str:
        """
        CONCEPTO: Feature engineering
        """
        if 6 <= hour < 12:
            return "morning"
        if 12 <= hour < 18:
            return "afternoon"
        if 18 <= hour < 22:
            return "evening"
        return "night"

    def _hash_event(self, event: Dict) -> str:
        """
        CONCEPTO: Content-based deduplication
        """
        payload = f"{event['id']}{event['type']}{event['created_at']}"
        return hashlib.sha256(payload.encode()).hexdigest()
