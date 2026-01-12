/*
config.jsonnet - Configuration as Code

CONCEPTO FUNDAMENTAL: Configuration Management
==============================================
Separar configuración del código tiene beneficios:
1. Cambiar config sin tocar código
2. Diferentes configs por ambiente (dev/staging/prod)
3. Versionado de configuración
4. Type safety y validación

¿Por qué Jsonnet y no YAML/JSON?
================================
Jsonnet es JSON + lógica:
- Variables y funciones
- Herencia y composición
- Evita duplicación (DRY)
- Validación en compile-time

En Databricks:
- Databricks Asset Bundles usa YAML similar
- Terraform HCL también es declarativo con lógica
*/

// CONCEPTO: Local Variables
// Define constantes reutilizables
local base_url = 'https://api.github.com';
local data_dir = 'data';

// CONCEPTO: Function (reutilización)
// Función que crea config de retry
local retryConfig(max_retries=3, backoff=2) = {
  max_retries: max_retries,
  backoff_factor: backoff,
  timeout: 10,
};

// CONCEPTO: Object Composition
// Diferentes ambientes heredan de base config
local baseConfig = {
  // API Configuration
  api: {
    base_url: base_url,
    endpoints: {
      events: '/repos/{owner}/{repo}/events',
      repo: '/repos/{owner}/{repo}',
    },
    // Aplicamos la función de retry
    retry: retryConfig(max_retries=3, backoff=2),
    // Rate limiting (GitHub: 60 req/hour sin auth)
    rate_limit: {
      requests_per_hour: 60,
      respect_headers: true,
    },
  },

  // Data paths (Medallion Architecture)
  paths: {
    base: data_dir,
    // Bronze layer: raw data from API
    raw: data_dir + '/raw',
    // Silver layer: cleaned, validated data
    curated: data_dir + '/curated',
    // Gold layer: aggregated metrics
    metrics: data_dir + '/metrics',
    // State management
    state: data_dir + '/state',
  },

  // Repository configuration
  repos: [
    // Data engineering repos
    { owner: 'apache', name: 'spark' },
    { owner: 'delta-io', name: 'delta' },
    { owner: 'databricks', name: 'koalas' },
  ],

  // Transformation configuration
  transform: {
    // Solo procesar estos tipos de eventos
    event_types_filter: [
      'PushEvent',
      'PullRequestEvent',
      'IssuesEvent',
      'WatchEvent',
      'ForkEvent',
    ],
    // Deduplication strategy
    dedup_strategy: 'content_hash',
  },

  // Metrics configuration
  metrics: {
    // Calcular estas agregaciones
    calculations: [
      'event_types',
      'repos',
      'contributors',
      'time_series',
      'activity',
    ],
    // Top N para rankings
    top_n: 10,
  },

  // Logging configuration
  logging: {
    level: 'INFO',
    format: 'json',  // Structured logging
    output: 'stdout',
  },
};

// CONCEPTO: Environment-specific Configuration
// Dev config: más logging, menos repos
local devConfig = baseConfig {
  logging+: {
    level: 'DEBUG',
  },
  repos: [
    { owner: 'apache', name: 'spark' },
  ],
  api+: {
    retry+: {
      max_retries: 2,  // Menos retries en dev
    },
  },
};

// Prod config: optimizado para performance
local prodConfig = baseConfig {
  api+: {
    retry+: {
      max_retries: 5,  // Más retries en prod
      backoff_factor: 3,
    },
  },
  logging+: {
    level: 'WARNING',  // Solo warnings/errors en prod
  },
};

// CONCEPTO: Conditional Export
// Exporta config según variable de ambiente
// En uso real: jsonnet -V env=prod config.jsonnet
{
  // Default: dev config
  dev: devConfig,
  prod: prodConfig,
  
  // Helper: función para obtener config por ambiente
  getConfig(env='dev'):: 
    if env == 'prod' then prodConfig
    else devConfig,
}