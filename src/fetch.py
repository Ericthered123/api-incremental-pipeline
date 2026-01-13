"""
fetch.py - API Ingestion Module

FUNDAMENTAL CONCEPT: Incremental Ingestion
===========================================
In data engineering, "incremental" means processing only NEW data,
not everything from scratch each time. This is critical for:
- Reducing costs (fewer API calls, less processing)
- Improving performance (only what's necessary)
- Avoiding duplicates

PATTERN: Checkpoint-based incremental load
- We save the "last processed date" (checkpoint)
- Next execution: we only fetch data AFTER that date
- If it fails, we retry from the last successful checkpoint
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
    CONCEPT: Separation of Concerns

=================================
This class ONLY handles retrieving data from the API.
It does not transform, calculate metrics, or decide what to do with the data.

This is what the code does:

- Testable (you can mock the API)

- Reusable (other pipelines can use this class)

- Maintainable (changes to the API only affect this file)
    """
    
    def __init__(self, base_url: str, timeout: int = 10, max_retries: int = 3):
        """
        Args:

        base_url: Base URL of the API

        timeout: Seconds before timeout

        max_retries: Retries in case of failure

        CONCEPT: Configuration injection
        We don't hardcode values; we receive them as parameters.

        This allows us to test and change configurations without touching code.
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()  # Reuses HTTP connections
        
    def fetch_events(self, repo_owner: str, repo_name: str, since: Optional[str] = None) -> List[Dict]:
        """
        Retrieves events from a GitHub repository.

            CONCEPT: Idempotence

            =====================
            If we call this function multiple times with the same 'since',

            we get the same results. There are no side effects.

            This is CRITICAL in data pipelines because:

            - We can retry without fear

            - The results are predictable

            - Debugging is easier

            Args:

            repo_owner: Repo owner (e.g., "apache")

            repo_name: Repo name (e.g., "spark")

            since: ISO 8601 timestamp (e.g., "2024-01-01T00:00:00Z")

            Only retrieves events AFTER this date

            Returns:

            List of events (each is a dict with the GitHub data)
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
       

        Retrieves events from multiple repositories.

        CONCEPT: Batch Processing

        =========================

        Instead of processing repositories one by one in separate calls,

        we group them. In production, this would be parallelized.

        Args:

        repos: List of tuples (owner, repo_name)

        since: Timestamp for incremental load

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


# CONCEPT: Self-documenting code
# This block allows you to run the standalone module for testing.
if __name__ == "__main__":
    # Exanple of use
    fetcher = APIFetcher(base_url="https://api.github.com")
    
    # Repos of example (proyectos de data engineering populares)
    test_repos = [
        ("apache", "spark"),
        ("delta-io", "delta"),
    ]
    
    # First execution : without 'since' (mos recent events)
    print("=== FIRST EXECUTION (BOOTSTRAP) ===")
    events = fetcher.fetch_multiple_repos(test_repos)
    
    for repo, repo_events in events.items():
        print(f"{repo}: {len(repo_events)} events")
        if repo_events:
            # Mostramos el más reciente
            latest = max(repo_events, key=lambda x: x['created_at'])
            print(f"  Latest event: {latest['type']} at {latest['created_at']}")
    
    # Segunda ejecución: incremental (solo eventos después de ahora)
    print("\n=== SECOND EXECUTION (INCREMENTAL) ===")
    now = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    events = fetcher.fetch_multiple_repos(test_repos, since=now)
    
    for repo, repo_events in events.items():
        print(f"{repo}: {len(repo_events)} new events")