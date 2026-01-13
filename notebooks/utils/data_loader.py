"""
notebooks/utils/data_loader.py

Helper utilities para cargar y procesar datos en notebooks.
Evita código repetitivo y centraliza lógica común.
"""

import json
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime


class PipelineDataLoader:
    """
    Utility class para cargar datos del pipeline en notebooks.
    
    Usage:
        loader = PipelineDataLoader()
        metrics = loader.get_metrics()
        events_df = loader.get_events()
    """
    
    def __init__(self, data_dir: str = '../data'):
        """
        Args:
            data_dir: Path to data directory (relative to notebook)
        """
        self.data_dir = Path(data_dir)
        
        # Paths
        self.raw_dir = self.data_dir / 'raw'
        self.curated_dir = self.data_dir / 'curated'
        self.metrics_dir = self.data_dir / 'metrics'
        self.state_dir = self.data_dir / 'state'
    
    def get_metrics(self, latest: bool = True) -> Optional[Dict]:
        """
        Load pipeline metrics.
        
        Args:
            latest: If True, load latest_metrics.json, else load most recent timestamped file
            
        Returns:
            Dict with metrics or None if not found
        """
        if latest:
            metrics_file = self.metrics_dir / 'latest_metrics.json'
        else:
            # Get most recent timestamped file
            metrics_files = list(self.metrics_dir.glob('metrics_*.json'))
            if not metrics_files:
                return None
            metrics_file = max(metrics_files, key=lambda x: x.stat().st_mtime)
        
        if not metrics_file.exists():
            return None
        
        with open(metrics_file) as f:
            return json.load(f)
    
    def get_events(
        self, 
        layer: str = 'curated',
        as_dataframe: bool = True
    ) -> Optional[pd.DataFrame | List[Dict]]:
        """
        Load events from specified layer.
        
        Args:
            layer: 'raw' or 'curated'
            as_dataframe: Return as DataFrame (True) or list of dicts (False)
            
        Returns:
            Events as DataFrame or list, None if not found
        """
        if layer == 'raw':
            events_dir = self.raw_dir
            pattern = 'events_*.json'
        elif layer == 'curated':
            events_dir = self.curated_dir
            pattern = 'events_curated_*.json'
        else:
            raise ValueError(f"Invalid layer: {layer}. Use 'raw' or 'curated'")
        
        # Get most recent file
        event_files = list(events_dir.glob(pattern))
        if not event_files:
            return None
        
        latest_file = max(event_files, key=lambda x: x.stat().st_mtime)
        
        with open(latest_file) as f:
            events = json.load(f)
        
        if as_dataframe:
            df = pd.DataFrame(events)
            
            # Parse timestamps
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'])
            if 'event_date' in df.columns:
                df['event_date'] = pd.to_datetime(df['event_date'])
            
            return df
        
        return events
    
    def get_checkpoint(self) -> Optional[Dict]:
        """Load current checkpoint."""
        checkpoint_file = self.state_dir / 'checkpoint.json'
        
        if not checkpoint_file.exists():
            return None
        
        with open(checkpoint_file) as f:
            return json.load(f)
    
    def get_run_history(self, as_dataframe: bool = True) -> Optional[pd.DataFrame | List[Dict]]:
        """
        Load pipeline run history.
        
        Args:
            as_dataframe: Return as DataFrame (True) or list (False)
            
        Returns:
            Run history as DataFrame or list
        """
        run_file = self.state_dir / 'run_history.jsonl'
        
        if not run_file.exists():
            return None
        
        with open(run_file) as f:
            runs = [json.loads(line) for line in f]
        
        if as_dataframe:
            df = pd.DataFrame(runs)
            
            # Parse timestamps
            if 'start_time' in df.columns:
                df['start_time'] = pd.to_datetime(df['start_time'])
            if 'end_time' in df.columns:
                df['end_time'] = pd.to_datetime(df['end_time'])
            
            return df
        
        return runs
    
    def get_summary(self) -> Dict:
        """
        Get quick summary of pipeline status.
        
        Returns:
            Dict with summary statistics
        """
        summary = {
            'data_available': False,
            'last_update': None,
            'total_events': 0,
            'last_run_status': 'unknown'
        }
        
        # Check metrics
        metrics = self.get_metrics()
        if metrics:
            summary['data_available'] = True
            summary['total_events'] = metrics['summary']['total_events']
            summary['last_update'] = metrics['summary']['calculation_timestamp']
        
        # Check checkpoint
        checkpoint = self.get_checkpoint()
        if checkpoint:
            summary['last_run_status'] = checkpoint.get('status', 'unknown')
        
        return summary
    
    def filter_events(
        self,
        df: pd.DataFrame,
        event_types: Optional[List[str]] = None,
        repos: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Filter events DataFrame.
        
        Args:
            df: Events DataFrame
            event_types: List of event types to include
            repos: List of repos to include
            start_date: Start date (ISO format)
            end_date: End date (ISO format)
            
        Returns:
            Filtered DataFrame
        """
        filtered = df.copy()
        
        if event_types:
            filtered = filtered[filtered['event_type'].isin(event_types)]
        
        if repos:
            filtered = filtered[filtered['repo_name'].isin(repos)]
        
        if start_date:
            filtered = filtered[filtered['created_at'] >= pd.to_datetime(start_date)]
        
        if end_date:
            filtered = filtered[filtered['created_at'] <= pd.to_datetime(end_date)]
        
        return filtered


class MetricsCalculator:
    """
    Helper class para calcular métricas ad-hoc en notebooks.
    """
    
    @staticmethod
    def event_type_distribution(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate event type distribution."""
        return df['event_type'].value_counts().reset_index()
    
    @staticmethod
    def repo_stats(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate repository statistics."""
        return df.groupby('repo_name').agg({
            'event_id': 'count',
            'actor_login': 'nunique',
            'created_at': ['min', 'max']
        }).reset_index()
    
    @staticmethod
    def contributor_stats(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate contributor statistics."""
        return df.groupby('actor_login').agg({
            'event_id': 'count',
            'repo_name': 'nunique',
            'event_type': lambda x: x.mode()[0] if len(x) > 0 else None
        }).reset_index()
    
    @staticmethod
    def hourly_distribution(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate hourly event distribution."""
        return df.groupby('event_hour').size().reset_index(name='count')
    
    @staticmethod
    def daily_trends(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate daily trends."""
        return df.groupby(df['created_at'].dt.date).agg({
            'event_id': 'count',
            'repo_name': 'nunique',
            'actor_login': 'nunique'
        }).reset_index()


class PlotTemplates:
    """
    Reusable plotting templates con configuración consistente.
    """
    
    @staticmethod
    def get_color_palette():
        """Get consistent color palette for the project."""
        return {
            'PushEvent': '#2ecc71',
            'PullRequestEvent': '#3498db',
            'IssuesEvent': '#e74c3c',
            'WatchEvent': '#f39c12',
            'ForkEvent': '#9b59b6',
            'CreateEvent': '#1abc9c',
            'DeleteEvent': '#e67e22',
            'primary': '#3498db',
            'secondary': '#95a5a6',
            'success': '#2ecc71',
            'warning': '#f39c12',
            'danger': '#e74c3c'
        }
    
    @staticmethod
    def get_plotly_template():
        """Get plotly template configuration."""
        return {
            'layout': {
                'font': {'family': 'Arial, sans-serif', 'size': 12},
                'title': {'font': {'size': 16, 'color': '#2c3e50'}},
                'hovermode': 'closest',
                'plot_bgcolor': '#f8f9fa',
                'paper_bgcolor': 'white'
            }
        }


# Convenience functions for quick access

def load_pipeline_data() -> Dict:
    """
    Quick loader for common notebook setup.
    
    Returns:
        Dict with 'metrics', 'events', 'checkpoint', 'run_history'
    """
    loader = PipelineDataLoader()
    
    return {
        'metrics': loader.get_metrics(),
        'events': loader.get_events(),
        'checkpoint': loader.get_checkpoint(),
        'run_history': loader.get_run_history()
    }


def get_pipeline_status() -> None:
    """Print pipeline status to console."""
    loader = PipelineDataLoader()
    summary = loader.get_summary()
    
    print("🔍 PIPELINE STATUS")
    print("=" * 50)
    print(f"Data Available: {'✓' if summary['data_available'] else '✗'}")
    print(f"Total Events: {summary['total_events']:,}")
    print(f"Last Update: {summary['last_update']}")
    print(f"Last Run: {summary['last_run_status']}")
    print("=" * 50)


# Example usage
if __name__ == "__main__":
    # Test the loader
    loader = PipelineDataLoader()
    
    print("Testing PipelineDataLoader...")
    
    metrics = loader.get_metrics()
    if metrics:
        print(f"✓ Loaded metrics: {metrics['summary']['total_events']} events")
    
    events_df = loader.get_events()
    if events_df is not None:
        print(f"✓ Loaded events: {len(events_df)} rows")
    
    summary = loader.get_summary()
    print(f"✓ Summary: {summary}")