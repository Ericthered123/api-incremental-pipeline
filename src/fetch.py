"""
fetch.py - API Ingestion Module

CONCEPTO FUNDAMENTAL: Ingesta Incremental
==========================================
En data engineering, "incremental" significa procesar solo datos NUEVOS,
no todo desde cero cada vez. Esto es crítico para:
- Reducir costos (menos API calls, menos procesamiento)
- Mejorar performance (solo lo necesario)
- Evitar duplicados

PATRÓN: Checkpoint-based incremental load
- Guardamos "última fecha procesada" (checkpoint)
- Próxima ejecución: solo traemos datos DESPUÉS de esa fecha
- Si falla, reintentamos desde el último checkpoint exitoso
"""

import requests
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime
import json

# BEST PRACTICE: Structured logging
# In prod, logs in JSON facilitate analysis and alerts
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class APIFetcher:
    """
    CONCEPTO: Separation of Concerns
    =================================
    Esta clase SOLO se encarga de traer datos de la API.
    No transforma, no calcula métricas, no decide qué hacer con los datos.
    Esto hace el código:
    - Testeable (puedes mockear la API)
    - Reutilizable (otros pipelines pueden usar esta clase)
    - Mantenible (cambios en la API solo afectan este archivo)
    """
    
    def __init__(self, base_url: str, timeout: int = 10, max_retries: int = 3):
        """
        Args:
            base_url: URL base de la API
            timeout: Segundos antes de timeout
            max_retries: Reintentos en caso de fallo
        
        CONCEPTO: Configuration injection
        No hardcodeamos valores, los recibimos como parámetros.
        Esto permite testear y cambiar config sin tocar código.
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()  # Reuses HTTP connections
        
    def fetch_events(self, repo_owner: str, repo_name: str, since: Optional[str] = None) -> List[Dict]:
        """
        Trae eventos de un repositorio de GitHub.
        
        CONCEPTO: Idempotencia
        ======================
        Si llamamos esta función múltiples veces con el mismo 'since',
        obtenemos los mismos resultados. No tiene efectos secundarios.
        Esto es CRÍTICO en data pipelines porque:
        - Podemos reintentar sin miedo
        - Los resultados son predecibles
        - Debugging es más fácil
        
        Args:
            repo_owner: Dueño del repo (ej: "apache")
            repo_name: Nombre del repo (ej: "spark")
            since: ISO 8601 timestamp (ej: "2024-01-01T00:00:00Z")
                   Solo trae eventos DESPUÉS de esta fecha
        
        Returns:
            Lista de eventos (cada uno es un dict con los datos de GitHub)
        """
        endpoint = f"{self.base_url}/repos/{repo_owner}/{repo_name}/events"
        
        # BEST PRACTICE: Retry con Exponential Backoff
        # If the API fails temporarily, we wait increasingly longer
        # before retrying (1s, 2s, 4s, 8s...)
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching events from {repo_owner}/{repo_name} (attempt {attempt + 1})")
                
                params = {}
                # GitHub API: us 'If-Modified-Since' header for incremental
                headers = {}
                if since:
                    headers['If-Modified-Since'] = since
                
                response = self.session.get(
                    endpoint,
                    params=params,
                    headers=headers,
                    timeout=self.timeout
                )
                
                # CONCEPTO: Rate Limiting
                # Public APIs limit requests. We respect those limits.
                rate_limit_remaining = response.headers.get('X-RateLimit-Remaining')
                if rate_limit_remaining:
                    logger.info(f"Rate limit remaining: {rate_limit_remaining}")
                
                # BEST PRACTICE: Explicit management of status codes
                if response.status_code == 304:
                    # 304 = Not Modified = no new data
                    logger.info("No new events since last fetch (304)")
                    return []
                
                if response.status_code == 403:
                    # 403 = Rate limit exceeded
                    reset_time = response.headers.get('X-RateLimit-Reset')
                    logger.warning(f"Rate limit exceeded. Resets at {reset_time}")
                    raise Exception("Rate limit exceeded")
                
                response.raise_for_status()  # Raise exception for 4xx/5xx
                
                events = response.json()
                logger.info(f"Successfully fetched {len(events)} events")
                
                # CONCEPT: Data Lineage
                # Add metadata about WHEN and HOW we obtained this data
                for event in events:
                    event['_ingestion_timestamp'] = datetime.utcnow().isoformat()
                    event['_source'] = 'github_api'
                
                return events
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    # Exponential backoff: 2^attempt secs
                    wait_time = 2 ** attempt
                    logger.info(f"Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached")
                    raise
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                else:
                    raise
        
        return []
    
    def fetch_multiple_repos(self, repos: List[tuple], since: Optional[str] = None) -> Dict[str, List[Dict]]:
        """
        Trae eventos de múltiples repos.
        
        CONCEPTO: Batch Processing
        ==========================
        En lugar de procesar repo por repo en calls separados,
        los agrupamos. En producción, esto se paralelizaría.
        
        Args:
            repos: Lista de tuplas (owner, repo_name)
            since: Timestamp para incremental load
            
        Returns:
            Dict con {repo_full_name: [events]}
        """
        all_events = {}
        
        for owner, repo in repos:
            repo_key = f"{owner}/{repo}"
            try:
                events = self.fetch_events(owner, repo, since)
                all_events[repo_key] = events
                
                # BEST PRACTICE: Rate limiting proactivo
                # GitHub allows 60 requests/hora without auth
                # 1 sec between request to avoid hitting the limit
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to fetch {repo_key}: {str(e)}")
                # CONCEPT: Fault Tolerance
                # If a repo fails, we continue with the others
                # We don't want an error to stop the entire pipeline
                all_events[repo_key] = []
        
        return all_events


# CONCEPTO: Self-documenting code
# Este bloque permite ejecutar el módulo standalone para testing
if __name__ == "__main__":
    # Ejemplo de uso
    fetcher = APIFetcher(base_url="https://api.github.com")
    
    # Repos de ejemplo (proyectos de data engineering populares)
    test_repos = [
        ("apache", "spark"),
        ("delta-io", "delta"),
    ]
    
    # Primera ejecución: sin 'since' (trae últimos eventos)
    print("=== PRIMERA EJECUCIÓN (BOOTSTRAP) ===")
    events = fetcher.fetch_multiple_repos(test_repos)
    
    for repo, repo_events in events.items():
        print(f"{repo}: {len(repo_events)} events")
        if repo_events:
            # Mostramos el más reciente
            latest = max(repo_events, key=lambda x: x['created_at'])
            print(f"  Latest event: {latest['type']} at {latest['created_at']}")
    
    # Segunda ejecución: incremental (solo eventos después de ahora)
    print("\n=== SEGUNDA EJECUCIÓN (INCREMENTAL) ===")
    now = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    events = fetcher.fetch_multiple_repos(test_repos, since=now)
    
    for repo, repo_events in events.items():
        print(f"{repo}: {len(repo_events)} new events")