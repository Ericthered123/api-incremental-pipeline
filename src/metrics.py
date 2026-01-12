"""
metrics.py - Metrics & Aggregation Module

CONCEPTO FUNDAMENTAL: Aggregation Layer (Gold)
==============================================
En Medallion Architecture:
- Bronze: Raw data
- Silver: Cleaned data
- Gold: Aggregated metrics ready for consumption

Este módulo implementa la capa Gold.
Transforma datos granulares en insights agregados.

En Databricks:
- Delta Live Tables hace esto declarativamente
- Aggregate tables optimizadas para queries
- Materialized views para performance
"""

from typing import List, Dict
from collections import defaultdict, Counter
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class MetricsCalculator:
    """
    CONCEPTO: Aggregation Patterns
    ===============================
    Los analytics típicamente necesitan agregaciones:
    - COUNT: ¿Cuántos eventos por tipo?
    - SUM: ¿Total de commits?
    - GROUP BY: ¿Actividad por repo? ¿Por usuario?
    - TIME SERIES: ¿Tendencias por hora/día?
    
    Este patrón es idéntico en SQL, Pandas, Spark:
    SELECT repo, COUNT(*) FROM events GROUP BY repo
    """
    
    def __init__(self):
        """
        Inicializa acumuladores para métricas.
        
        CONCEPTO: Accumulators
        ======================
        Mantenemos contadores en memoria mientras procesamos el batch.
        En Spark distribuido: cada worker mantiene su accumulator,
        luego se combinan (reduce).
        """
        self.metrics = {}
    
    def calculate_event_type_metrics(self, events: List[Dict]) -> Dict:
        """
        Métricas por tipo de evento.
        
        CONCEPTO: Group By Aggregation
        ===============================
        SQL equivalente:
        SELECT event_type, COUNT(*) as count
        FROM events
        GROUP BY event_type
        
        Returns:
            {
                'PushEvent': 150,
                'PullRequestEvent': 45,
                ...
            }
        """
        type_counts = Counter(event['event_type'] for event in events)
        
        logger.info(f"Event types: {dict(type_counts)}")
        return dict(type_counts)
    
    def calculate_repo_metrics(self, events: List[Dict]) -> Dict:
        """
        Métricas por repositorio.
        
        CONCEPTO: Multi-dimensional Aggregation
        ========================================
        Agregamos por repo Y por tipo de evento.
        
        SQL equivalente:
        SELECT 
            repo_name,
            COUNT(*) as total_events,
            COUNT(DISTINCT actor_login) as unique_contributors,
            COUNT(CASE WHEN is_active_contribution THEN 1 END) as active_events
        FROM events
        GROUP BY repo_name
        
        En Spark: esto se paralelizaría por particiones de repo.
        """
        repo_stats = defaultdict(lambda: {
            'total_events': 0,
            'event_types': Counter(),
            'unique_actors': set(),
            'active_contributions': 0,
            'first_event': None,
            'last_event': None
        })
        
        for event in events:
            repo = event['repo_name']
            stats = repo_stats[repo]
            
            # Contadores
            stats['total_events'] += 1
            stats['event_types'][event['event_type']] += 1
            
            # Unique actors (contributors)
            if event.get('actor_login'):
                stats['unique_actors'].add(event['actor_login'])
            
            # Active contributions
            if event.get('is_active_contribution'):
                stats['active_contributions'] += 1
            
            # Time range
            event_time = event['created_at']
            if stats['first_event'] is None or event_time < stats['first_event']:
                stats['first_event'] = event_time
            if stats['last_event'] is None or event_time > stats['last_event']:
                stats['last_event'] = event_time
        
        # CONCEPTO: Serializable Metrics
        # Convertimos sets a listas para poder guardar como JSON
        serializable_stats = {}
        for repo, stats in repo_stats.items():
            serializable_stats[repo] = {
                'total_events': stats['total_events'],
                'unique_contributors': len(stats['unique_actors']),
                'active_contributions': stats['active_contributions'],
                'top_event_types': dict(stats['event_types'].most_common(5)),
                'time_range': {
                    'first': stats['first_event'],
                    'last': stats['last_event']
                }
            }
        
        return serializable_stats
    
    def calculate_contributor_metrics(self, events: List[Dict]) -> Dict:
        """
        Métricas por contributor.
        
        CONCEPTO: User Analytics
        ========================
        Útil para identificar:
        - Contributors más activos
        - Patterns de contribución
        - Diversidad de actividades
        
        SQL equivalente:
        SELECT 
            actor_login,
            COUNT(*) as total_contributions,
            COUNT(DISTINCT repo_name) as repos_contributed_to,
            MAX(created_at) as last_activity
        FROM events
        WHERE actor_login IS NOT NULL
        GROUP BY actor_login
        """
        contributor_stats = defaultdict(lambda: {
            'total_contributions': 0,
            'repos': set(),
            'event_types': Counter(),
            'last_activity': None
        })
        
        for event in events:
            actor = event.get('actor_login')
            if not actor:
                continue
            
            stats = contributor_stats[actor]
            
            stats['total_contributions'] += 1
            stats['repos'].add(event['repo_name'])
            stats['event_types'][event['event_type']] += 1
            
            event_time = event['created_at']
            if stats['last_activity'] is None or event_time > stats['last_activity']:
                stats['last_activity'] = event_time
        
        # Serializable
        serializable_stats = {}
        for actor, stats in contributor_stats.items():
            serializable_stats[actor] = {
                'total_contributions': stats['total_contributions'],
                'repos_count': len(stats['repos']),
                'repos': list(stats['repos']),
                'top_activities': dict(stats['event_types'].most_common(3)),
                'last_activity': stats['last_activity']
            }
        
        return serializable_stats
    
    def calculate_time_series_metrics(self, events: List[Dict]) -> Dict:
        """
        Métricas en series de tiempo.
        
        CONCEPTO: Time Series Analysis
        ===============================
        Agregamos por ventanas de tiempo para ver tendencias.
        
        En Databricks:
        - Window functions
        - Tumbling/Sliding windows en Structured Streaming
        - Time travel queries en Delta Lake
        
        SQL equivalente:
        SELECT 
            DATE(created_at) as date,
            HOUR(created_at) as hour,
            COUNT(*) as event_count
        FROM events
        GROUP BY DATE(created_at), HOUR(created_at)
        ORDER BY date, hour
        """
        # Agregación por día
        daily_stats = defaultdict(lambda: {
            'event_count': 0,
            'event_types': Counter(),
            'active_repos': set()
        })
        
        # Agregación por hora
        hourly_stats = defaultdict(lambda: {
            'event_count': 0,
            'event_types': Counter()
        })
        
        for event in events:
            date = event['event_date']  # YYYY-MM-DD
            hour = event['event_hour']
            
            # Daily aggregation
            daily_stats[date]['event_count'] += 1
            daily_stats[date]['event_types'][event['event_type']] += 1
            daily_stats[date]['active_repos'].add(event['repo_name'])
            
            # Hourly aggregation
            hour_key = f"{date}T{hour:02d}"
            hourly_stats[hour_key]['event_count'] += 1
            hourly_stats[hour_key]['event_types'][event['event_type']] += 1
        
        # Serializable
        return {
            'daily': {
                date: {
                    'event_count': stats['event_count'],
                    'active_repos_count': len(stats['active_repos']),
                    'top_event_types': dict(stats['event_types'].most_common(3))
                }
                for date, stats in sorted(daily_stats.items())
            },
            'hourly': {
                hour: {
                    'event_count': stats['event_count'],
                    'event_types': dict(stats['event_types'])
                }
                for hour, stats in sorted(hourly_stats.items())
            }
        }
    
    def calculate_activity_metrics(self, events: List[Dict]) -> Dict:
        """
        Métricas de actividad (features específicas).
        
        CONCEPTO: Domain-Specific Metrics
        ==================================
        Métricas que tienen sentido para el dominio (GitHub).
        En otros dominios serían diferentes (ej: e-commerce = revenue, conversions)
        """
        activity_stats = {
            'total_events': len(events),
            'active_contributions': sum(1 for e in events if e.get('is_active_contribution')),
            'activity_by_category': Counter(e['activity_category'] for e in events),
            'activity_by_time_of_day': Counter(e['time_of_day'] for e in events)
        }
        
        # Push-specific metrics
        push_events = [e for e in events if e['event_type'] == 'PushEvent']
        if push_events:
            activity_stats['push_metrics'] = {
                'total_pushes': len(push_events),
                'total_commits': sum(e.get('push_commits_count', 0) for e in push_events),
                'avg_commits_per_push': sum(e.get('push_commits_count', 0) for e in push_events) / len(push_events)
            }
        
        # PR-specific metrics
        pr_events = [e for e in events if e['event_type'] == 'PullRequestEvent']
        if pr_events:
            activity_stats['pr_metrics'] = {
                'total_prs': len(pr_events),
                'pr_actions': Counter(e.get('pr_action') for e in pr_events),
                'merged_prs': sum(1 for e in pr_events if e.get('pr_merged'))
            }
        
        return activity_stats
    
    def calculate_all_metrics(self, events: List[Dict]) -> Dict:
        """
        Calcula todas las métricas en un solo paso.
        
        CONCEPTO: Single-Pass Aggregation
        ==================================
        En lugar de iterar sobre los datos múltiples veces,
        calculamos todas las métricas en una sola pasada.
        
        Esto es crítico para performance en big data:
        - Menos I/O
        - Mejor uso de cache
        - Más eficiente en Spark (menos shuffles)
        
        Returns:
            Dict con todas las métricas organizadas por categoría
        """
        if not events:
            logger.warning("No events to calculate metrics")
            return {}
        
        logger.info(f"Calculating metrics for {len(events)} events")
        
        metrics = {
            'summary': {
                'total_events': len(events),
                'calculation_timestamp': datetime.utcnow().isoformat()
            },
            'event_types': self.calculate_event_type_metrics(events),
            'repos': self.calculate_repo_metrics(events),
            'contributors': self.calculate_contributor_metrics(events),
            'time_series': self.calculate_time_series_metrics(events),
            'activity': self.calculate_activity_metrics(events)
        }
        
        logger.info("Metrics calculation completed")
        return metrics
    
    def get_top_repos(self, metrics: Dict, top_n: int = 10) -> List[Dict]:
        """
        Obtiene top N repos por actividad.
        
        CONCEPTO: Ranking
        =================
        Ordenamos y tomamos top N.
        En SQL: ORDER BY ... LIMIT N
        En Spark: orderBy(...).limit(N)
        """
        repos = metrics.get('repos', {})
        
        sorted_repos = sorted(
            repos.items(),
            key=lambda x: x[1]['total_events'],
            reverse=True
        )
        
        return [
            {'repo': repo, **stats}
            for repo, stats in sorted_repos[:top_n]
        ]
    
    def get_top_contributors(self, metrics: Dict, top_n: int = 10) -> List[Dict]:
        """
        Obtiene top N contributors por actividad.
        """
        contributors = metrics.get('contributors', {})
        
        sorted_contributors = sorted(
            contributors.items(),
            key=lambda x: x[1]['total_contributions'],
            reverse=True
        )
        
        return [
            {'contributor': actor, **stats}
            for actor, stats in sorted_contributors[:top_n]
        ]


# TESTING: Demuestra cálculo de métricas
if __name__ == "__main__":
    print("=== METRICS CALCULATION DEMO ===\n")
    
    # Eventos de ejemplo (simulados)
    sample_events = [
        {
            'event_id': '1',
            'event_type': 'PushEvent',
            'created_at': '2024-01-15T10:00:00Z',
            'event_date': '2024-01-15',
            'event_hour': 10,
            'repo_name': 'apache/spark',
            'actor_login': 'contributor1',
            'is_active_contribution': True,
            'activity_category': 'code_change',
            'time_of_day': 'morning',
            'push_commits_count': 3
        },
        {
            'event_id': '2',
            'event_type': 'PullRequestEvent',
            'created_at': '2024-01-15T11:00:00Z',
            'event_date': '2024-01-15',
            'event_hour': 11,
            'repo_name': 'apache/spark',
            'actor_login': 'contributor2',
            'is_active_contribution': True,
            'activity_category': 'code_review',
            'time_of_day': 'morning',
            'pr_action': 'opened'
        },
        {
            'event_id': '3',
            'event_type': 'PushEvent',
            'created_at': '2024-01-15T14:00:00Z',
            'event_date': '2024-01-15',
            'event_hour': 14,
            'repo_name': 'delta-io/delta',
            'actor_login': 'contributor1',
            'is_active_contribution': True,
            'activity_category': 'code_change',
            'time_of_day': 'afternoon',
            'push_commits_count': 5
        }
    ]
    
    calculator = MetricsCalculator()
    
    print("1. EVENT TYPE METRICS")
    event_types = calculator.calculate_event_type_metrics(sample_events)
    for event_type, count in event_types.items():
        print(f"   {event_type}: {count}")
    
    print("\n2. REPO METRICS")
    repos = calculator.calculate_repo_metrics(sample_events)
    for repo, stats in repos.items():
        print(f"   {repo}:")
        print(f"      Total events: {stats['total_events']}")
        print(f"      Contributors: {stats['unique_contributors']}")
    
    print("\n3. ALL METRICS")
    all_metrics = calculator.calculate_all_metrics(sample_events)
    print(f"   Total events: {all_metrics['summary']['total_events']}")
    print(f"   Repos tracked: {len(all_metrics['repos'])}")
    print(f"   Contributors: {len(all_metrics['contributors'])}")