/*
config.jsonnet - Configuration as Code

FUNDAMENTAL CONCEPT: Configuration Management
===============================================
Separating configuration from code has benefits:
1. Changing configurations without touching code
2. Different configurations for each environment (dev/staging/prod)
3. Configuration versioning
4. Type safety and validation

Why JSONNET and not YAML/JSON?

=================================
Jsonnet is JSON + logic:
- Variables and functions
- Inheritance and composition
- Prevents duplication (DRY)
- Compile-time validation

In Databricks:
- Databricks Asset Bundles uses similar YAML
- Terraform HCL is also declarative with logic
*/

// CONCEPT: Local Variables
// Defines reusable constants
local base_url = 'https://api.github.com';
local data_dir = 'data';

// CONCEPT: Function (reuse)
// Function that creates a retry configuration
local retryConfig(max_retries=3, backoff=2) = {
  max_retries: max_retries,
  backoff_factor: backoff,
  timeout: 10,
};

// CONCEPT: Object Composition
// Different environments inherit from the base configuration
local baseConfig = {
  // API Configuration
  api: {
    base_url: base_url,
    endpoints: {
      events: '/repos/{owner}/{repo}/events',
      repo: '/repos/{owner}/{repo}',
    },
    // We apply the retry function

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
    // Only process these types of events
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
    // Calculate these aggregations
    calculations: [
      'event_types',
      'repos',
      'contributors',
      'time_series',
      'activity',
    ],
    // Top N for rankings
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
// Dev config: more logging, less repos
local devConfig = baseConfig {
  logging+: {
    level: 'DEBUG',
  },
  repos: [
    { owner: 'apache', name: 'spark' },
  ],
  api+: {
    retry+: {
      max_retries: 2,  // less retries en dev
    },
  },
};

// Prod config: optimized for performance
local prodConfig = baseConfig {
  api+: {
    retry+: {
      max_retries: 5,  // more retries en prod
      backoff_factor: 3,
    },
  },
  logging+: {
    level: 'WARNING',  // only warnings/errors en prod
  },
};

// CONCEPTO: Conditional Export
// Export config according to var of env 
// In real case-scenario: jsonnet -V env=prod config.jsonnet
{
  // Default: dev config
  dev: devConfig,
  prod: prodConfig,
  
  // Helper: func to obtain config por env
  getConfig(env='dev'):: 
    if env == 'prod' then prodConfig
    else devConfig,
}