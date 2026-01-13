"""
transform.py - Data Transformation Module

FUNDAMENTAL CONCEPT: Data Quality & Transformation
====================================================
API data is rarely ready for direct analysis.

We need:
1. Clean (null handling, deduplication)
2. Validate (schema, data types)
3. Enrich (add calculated fields)
4. Filter (only relevant data)

MEDALLION ARCHITECTURE (Databricks pattern)
==============================================
Bronze → Silver → Gold

Bronze (Raw): Data as received from the API
Silver (Curated): Clean, validated, and deduplicated data
Gold (Metrics): Aggregations and metrics for analysis

This module performs the Bronze → Silver transformation
"""

from typing import List, Dict, Optional, Set
from datetime import datetime
import logging
import hashlib

logger = logging.getLogger(__name__)


class EventTransformer:
    """
   CONCEPT: Single Responsibility
    ================================
    This class ONLY transforms data.
    It doesn't bring them (that's fetch.py) or persist them (that's main.py).
    
    Advantages:
    - Easy to test (clear inputs and outputs)
    - Reusable in other pipelines
    - Changes in transformation do not affect intake
    """
    
    def __init__(self, event_types_filter: Optional[List[str]] = None):
        """
        Args
            event_types_filter: List of event types to maintain
                               None = keep all
        
        CONCEPT: Configuration over Code
        We pass configuration as a parameter, not hardcoded.
        """
        self.event_types_filter = event_types_filter
        
        # CONCEPTO: Deduplication Set
        # Guardamos IDs de eventos ya vistos en esta ejecución
        # para evitar duplicados dentro del mismo batch
        self._seen_ids: Set[str] = set()
    
    def clean_event(self, event: Dict) -> Optional[Dict]:
        """
        Cleans and validates an individual event.
        
        CONCEPT: Data Quality Checks
        ==============================
        We apply quality rules:
        1. Do you have the required fields?
        2. Are the data types correct?
        3. Do values make sense? (ex: date not in the future)
        
        In production this expands with:
        - Schema validation (Pydantic, Great Expectations)
        - Data profiling
        - Quality alerts
        
        Returns:
            Clean event, or None if it does not pass validation
        """
        # VALIDACIÓN 1: Campos requeridos
        required_fields = ['id', 'type', 'created_at', 'repo']
        
        for field in required_fields:
            if field not in event:
                logger.warning(f"Event missing required field '{field}': {event.get('id', 'unknown')}")
                return None
        
        # VALIDACIÓN 2: Deduplicación
        event_id = event['id']
        if event_id in self._seen_ids:
            logger.debug(f"Duplicate event filtered: {event_id}")
            return None
        
        self._seen_ids.add(event_id)
        
        # VALIDACIÓN 3: Filtro por tipo de evento
        if self.event_types_filter:
            if event['type'] not in self.event_types_filter:
                return None
        
        # LIMPIEZA: Normalización de datos
        cleaned = {
            # IDs como strings (pueden ser muy grandes para int)
            'event_id': str(event['id']),
            'event_type': event['type'],
            
            # Timestamps normalizados a ISO 8601
            'created_at': self._normalize_timestamp(event['created_at']),
            
            # Información del repo (anidada → flat)
            'repo_id': event['repo']['id'],
            'repo_name': event['repo']['name'],
            'repo_url': event['repo'].get('url', ''),
            
            # Actor (quien hizo la acción)
            'actor_id': event.get('actor', {}).get('id'),
            'actor_login': event.get('actor', {}).get('login'),
            
            # Payload (varía según tipo de evento)
            'payload': event.get('payload', {}),
            
            # CONCEPTO: Data Lineage
            # Metadata sobre el procesamiento
            '_ingestion_timestamp': event.get('_ingestion_timestamp'),
            '_transformation_timestamp': datetime.utcnow().isoformat(),
            '_source': event.get('_source', 'unknown')
        }
        
        # ENRIQUECIMIENTO: Campos calculados
        cleaned['event_date'] = cleaned['created_at'][:10]  # YYYY-MM-DD
        cleaned['event_hour'] = int(cleaned['created_at'][11:13])  # Hour of day
        
        # Hash para detección de duplicados completos (no solo por ID)
        cleaned['_content_hash'] = self._compute_hash(cleaned)
        
        return cleaned
    
    def _normalize_timestamp(self, ts: str) -> str:
        """
        Standardize timestamp to ISO 8601 format.
        
        CONCEPT: Data Standardization
        ===============================
        APIs can return timestamps in different formats.
        We standardize EVERYTHING to ISO 8601 (universal format).
        
        In Databricks: TimestampType does this automatically.
        """
        try:
            # GitHub usa ISO 8601, pero validamos
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            return dt.isoformat()
        except Exception as e:
            logger.warning(f"Invalid timestamp format: {ts}")
            # Fallback: timestamp de ingesta
            return datetime.utcnow().isoformat()
    
    def _compute_hash(self, event: Dict) -> str:
        """
       Calculates hash of the event content.
        
        CONCEPT: Content-based Deduplication
        =====================================
        Two events can have different IDs but same content
        (ex: duplicate by API retry).
        
        Hash allows us to detect duplicates by CONTENT,
        not just by ID.
        
        In Databricks: Delta Lake can do this with MERGE USING
        hash columns as part of the key.
        """
        import json
        
        # Excluimos campos de metadata para el hash
        content_fields = {k: v for k, v in event.items() if not k.startswith('_')}
        
        # JSON determinístico (siempre mismo orden)
        content_str = json.dumps(content_fields, sort_keys=True)
        
        return hashlib.md5(content_str.encode()).hexdigest()
    
    def transform_batch(self, events: List[Dict]) -> List[Dict]:
        """
        Transform a batch of events.
        
        CONCEPT: Batch Processing
        ==========================
        We process multiple records in a single step.
        This is more efficient than processing one at a time.
        
        In Spark/Databricks: this automatically parallels
        across partitions.
        
        Args:
            events: List of raw events (Bronze)
            
        Returns:
            Clean Events List (Silver)
        """
        cleaned_events = []
        
        stats = {
            'total': len(events),
            'valid': 0,
            'filtered': 0,
            'invalid': 0
        }
        
        for event in events:
            cleaned = self.clean_event(event)
            
            if cleaned is None:
                stats['filtered'] += 1
            else:
                cleaned_events.append(cleaned)
                stats['valid'] += 1
        
        stats['invalid'] = stats['total'] - stats['valid'] - stats['filtered']
        
        logger.info(
            f"Batch transformation: {stats['valid']} valid, "
            f"{stats['filtered']} filtered, {stats['invalid']} invalid "
            f"(total: {stats['total']})"
        )
        
        return cleaned_events
    
    def extract_payload_features(self, event: Dict) -> Dict:
        """
       QUERY LENGTH LIMIT EXCEEDED. MAX ALLOWED QUERY : 500 CHARS
        """
        event_type = event.get('event_type')
        payload = event.get('payload', {})
        
        features = {}
        
        if event_type == 'PushEvent':
            # Push events: commits
            features['push_size'] = payload.get('size', 0)
            features['push_ref'] = payload.get('ref', '')
            features['push_commits_count'] = len(payload.get('commits', []))
            
        elif event_type == 'PullRequestEvent':
            # Pull request events
            pr = payload.get('pull_request', {})
            features['pr_action'] = payload.get('action')
            features['pr_state'] = pr.get('state')
            features['pr_merged'] = pr.get('merged', False)
            features['pr_additions'] = pr.get('additions', 0)
            features['pr_deletions'] = pr.get('deletions', 0)
            
        elif event_type == 'IssuesEvent':
            # Issue events
            issue = payload.get('issue', {})
            features['issue_action'] = payload.get('action')
            features['issue_state'] = issue.get('state')
            features['issue_labels'] = [l['name'] for l in issue.get('labels', [])]
            
        elif event_type == 'WatchEvent':
            # Star events
            features['watch_action'] = payload.get('action')
        
        return features
    
    def enrich_event(self, event: Dict) -> Dict:
        """
       Enrich the event with additional features.
        
        CONCEPT: Feature Engineering
        ==============================
        We add calculated fields that facilitate analysis:
        - Categorization
        - Boolean flags
        - Derived Features
        
        In ML pipelines, this step is critical for models.
        """
        enriched = event.copy()
        
        # Extrae features del payload
        payload_features = self.extract_payload_features(event)
        enriched.update(payload_features)
        
        # FEATURE: Categoría de actividad
        event_type = event.get('event_type', '')
        
        if 'Push' in event_type or 'Commit' in event_type:
            enriched['activity_category'] = 'code_change'
        elif 'PullRequest' in event_type or 'Review' in event_type:
            enriched['activity_category'] = 'code_review'
        elif 'Issue' in event_type:
            enriched['activity_category'] = 'issue_management'
        elif 'Create' in event_type or 'Delete' in event_type:
            enriched['activity_category'] = 'repository_admin'
        else:
            enriched['activity_category'] = 'other'
        
        # FEATURE: Es evento de contribución activa?
        active_types = {'PushEvent', 'PullRequestEvent', 'IssuesEvent', 'CommitCommentEvent'}
        enriched['is_active_contribution'] = event_type in active_types
        
        # FEATURE: Período del día (útil para análisis de actividad)
        hour = enriched.get('event_hour', 0)
        if 6 <= hour < 12:
            enriched['time_of_day'] = 'morning'
        elif 12 <= hour < 18:
            enriched['time_of_day'] = 'afternoon'
        elif 18 <= hour < 24:
            enriched['time_of_day'] = 'evening'
        else:
            enriched['time_of_day'] = 'night'
        
        return enriched


# TESTING: Demuestra transformación completa
if __name__ == "__main__":
    print("=== TRANSFORMATION DEMO ===\n")
    
    # Evento de ejemplo (simulado)
    sample_event = {
        'id': '12345',
        'type': 'PushEvent',
        'created_at': '2024-01-15T14:30:00Z',
        'repo': {
            'id': 1234,
            'name': 'apache/spark',
            'url': 'https://api.github.com/repos/apache/spark'
        },
        'actor': {
            'id': 5678,
            'login': 'contributor1'
        },
        'payload': {
            'size': 3,
            'ref': 'refs/heads/main',
            'commits': [
                {'sha': 'abc123', 'message': 'Fix bug'},
                {'sha': 'def456', 'message': 'Add feature'},
                {'sha': 'ghi789', 'message': 'Update docs'}
            ]
        },
        '_ingestion_timestamp': '2024-01-15T14:31:00Z',
        '_source': 'github_api'
    }
    
    # Transformar
    transformer = EventTransformer(event_types_filter=['PushEvent', 'PullRequestEvent'])
    
    print("1. CLEANED EVENT")
    cleaned = transformer.clean_event(sample_event)
    for key, value in cleaned.items():
        if not key.startswith('payload'):
            print(f"   {key}: {value}")
    
    print("\n2. ENRICHED EVENT")
    enriched = transformer.enrich_event(cleaned)
    print(f"   activity_category: {enriched['activity_category']}")
    print(f"   is_active_contribution: {enriched['is_active_contribution']}")
    print(f"   time_of_day: {enriched['time_of_day']}")
    print(f"   push_commits_count: {enriched['push_commits_count']}")
    
    print("\n3. BATCH PROCESSING")
    # Simulamos batch con duplicado
    batch = [sample_event, sample_event, sample_event]  # 3 copias
    result = transformer.transform_batch(batch)
    print(f"   Input: {len(batch)} events")
    print(f"   Output: {len(result)} events (deduped)")