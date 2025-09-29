#!/bin/bash

# Grafana Dashboard Generator Script for GORP
# Generates JSON dashboard files for importing into Grafana

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARDS_DIR="${SCRIPT_DIR}/../dashboards"
PROMETHEUS_UID="${PROMETHEUS_UID:-prometheus}"
NAMESPACE="${NAMESPACE:-gorp}"

# Create dashboards directory if it doesn't exist
mkdir -p "$DASHBOARDS_DIR"

echo "Generating GORP Grafana dashboards..."
echo "Prometheus UID: $PROMETHEUS_UID"
echo "Namespace: $NAMESPACE"
echo "Output directory: $DASHBOARDS_DIR"

# Generate Overview Dashboard
cat > "$DASHBOARDS_DIR/gorp-overview.json" << 'EOF'
{
  "uid": "gorp-overview",
  "title": "GORP Database Overview",
  "tags": ["gorp", "database", "orm"],
  "style": "dark",
  "timezone": "browser",
  "editable": true,
  "hideControls": false,
  "graphTooltip": 1,
  "time": {
    "from": "now-1h",
    "to": "now"
  },
  "timepicker": {
    "refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"],
    "time_options": ["5m", "15m", "1h", "6h", "12h", "24h", "2d", "7d", "30d"]
  },
  "refresh": "5s",
  "schemaVersion": 27,
  "version": 1,
  "templating": {
    "list": [
      {
        "name": "instance",
        "type": "query",
        "label": "Instance",
        "query": "label_values(gorp_database_connections_active, instance)",
        "refresh": 1,
        "current": {"selected": true, "text": "All", "value": "$__all"},
        "hide": 0,
        "includeAll": true,
        "multi": true,
        "allValue": ".*",
        "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"},
        "sort": 1
      },
      {
        "name": "database",
        "type": "query",
        "label": "Database",
        "query": "label_values(gorp_database_connections_active{instance=~\"$instance\"}, database)",
        "refresh": 1,
        "current": {"selected": true, "text": "All", "value": "$__all"},
        "hide": 0,
        "includeAll": true,
        "multi": true,
        "allValue": ".*",
        "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"},
        "sort": 1
      }
    ]
  },
  "panels": [
    {
      "id": 1,
      "title": "Database Connections",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 12, "x": 0, "y": 0},
      "targets": [
        {
          "expr": "gorp_database_connections_active{instance=~\"$instance\",database=~\"$database\"}",
          "legendFormat": "Active - {{instance}} {{database}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        },
        {
          "expr": "gorp_database_connections_idle{instance=~\"$instance\",database=~\"$database\"}",
          "legendFormat": "Idle - {{instance}} {{database}}",
          "refId": "B",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        },
        {
          "expr": "gorp_database_connections_open{instance=~\"$instance\",database=~\"$database\"}",
          "legendFormat": "Total - {{instance}} {{database}}",
          "refId": "C",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      },
      "options": {
        "tooltip": {"mode": "multi", "sort": "desc"},
        "legend": {"displayMode": "list", "placement": "bottom"}
      }
    },
    {
      "id": 2,
      "title": "Query Rate",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 12, "x": 12, "y": 0},
      "targets": [
        {
          "expr": "rate(gorp_database_queries_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
          "legendFormat": "{{instance}} {{database}} {{query_type}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "qps",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      },
      "options": {
        "tooltip": {"mode": "multi", "sort": "desc"},
        "legend": {"displayMode": "list", "placement": "bottom"}
      }
    },
    {
      "id": 3,
      "title": "Query Latency",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 12, "x": 0, "y": 9},
      "targets": [
        {
          "expr": "histogram_quantile(0.95, rate(gorp_database_query_duration_seconds_bucket{instance=~\"$instance\",database=~\"$database\"}[5m]))",
          "legendFormat": "95th percentile - {{instance}} {{database}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        },
        {
          "expr": "histogram_quantile(0.50, rate(gorp_database_query_duration_seconds_bucket{instance=~\"$instance\",database=~\"$database\"}[5m]))",
          "legendFormat": "50th percentile - {{instance}} {{database}}",
          "refId": "B",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        },
        {
          "expr": "rate(gorp_database_query_duration_seconds_sum{instance=~\"$instance\",database=~\"$database\"}[5m]) / rate(gorp_database_query_duration_seconds_count{instance=~\"$instance\",database=~\"$database\"}[5m])",
          "legendFormat": "Average - {{instance}} {{database}}",
          "refId": "C",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      },
      "options": {
        "tooltip": {"mode": "multi", "sort": "desc"},
        "legend": {"displayMode": "list", "placement": "bottom"}
      }
    },
    {
      "id": 4,
      "title": "Error Rate",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 12, "x": 12, "y": 9},
      "targets": [
        {
          "expr": "rate(gorp_database_errors_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
          "legendFormat": "{{instance}} {{database}} {{error_type}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      },
      "thresholds": {
        "mode": "absolute",
        "steps": [
          {"color": "green", "value": null},
          {"color": "yellow", "value": 0.1},
          {"color": "red", "value": 1.0}
        ]
      },
      "options": {
        "tooltip": {"mode": "multi", "sort": "desc"},
        "legend": {"displayMode": "list", "placement": "bottom"}
      }
    },
    {
      "id": 5,
      "title": "Transactions",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 12, "x": 0, "y": 18},
      "targets": [
        {
          "expr": "rate(gorp_database_transactions_total{instance=~\"$instance\",database=~\"$database\",status=\"committed\"}[5m])",
          "legendFormat": "Committed - {{instance}} {{database}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        },
        {
          "expr": "rate(gorp_database_transactions_total{instance=~\"$instance\",database=~\"$database\",status=\"rolled_back\"}[5m])",
          "legendFormat": "Rolled Back - {{instance}} {{database}}",
          "refId": "B",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      },
      "options": {
        "tooltip": {"mode": "multi", "sort": "desc"},
        "legend": {"displayMode": "list", "placement": "bottom"}
      }
    },
    {
      "id": 6,
      "title": "Database Sizes",
      "type": "stat",
      "gridPos": {"h": 9, "w": 12, "x": 12, "y": 18},
      "targets": [
        {
          "expr": "gorp_database_size_bytes{instance=~\"$instance\",database=~\"$database\"}",
          "legendFormat": "{{instance}} {{database}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "bytes"
        }
      },
      "options": {
        "reduceOptions": {
          "values": false,
          "calcs": ["lastNotNull"]
        },
        "orientation": "auto",
        "textMode": "auto",
        "colorMode": "value",
        "graphMode": "area"
      }
    }
  ],
  "links": [
    {
      "icon": "external link",
      "tags": ["gorp"],
      "targetBlank": false,
      "title": "GORP Performance Dashboard",
      "type": "dashboards",
      "url": "/d/gorp-performance/gorp-performance-details"
    }
  ]
}
EOF

# Generate Performance Dashboard
cat > "$DASHBOARDS_DIR/gorp-performance.json" << 'EOF'
{
  "uid": "gorp-performance",
  "title": "GORP Performance Details",
  "tags": ["gorp", "performance", "database"],
  "style": "dark",
  "timezone": "browser",
  "editable": true,
  "hideControls": false,
  "graphTooltip": 1,
  "time": {
    "from": "now-6h",
    "to": "now"
  },
  "refresh": "10s",
  "schemaVersion": 27,
  "version": 1,
  "templating": {
    "list": [
      {
        "name": "instance",
        "type": "query",
        "label": "Instance",
        "query": "label_values(gorp_database_connections_active, instance)",
        "refresh": 1,
        "current": {"selected": true, "text": "All", "value": "$__all"},
        "hide": 0,
        "includeAll": true,
        "multi": true,
        "allValue": ".*",
        "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"},
        "sort": 1
      },
      {
        "name": "database",
        "type": "query",
        "label": "Database",
        "query": "label_values(gorp_database_connections_active{instance=~\"$instance\"}, database)",
        "refresh": 1,
        "current": {"selected": true, "text": "All", "value": "$__all"},
        "hide": 0,
        "includeAll": true,
        "multi": true,
        "allValue": ".*",
        "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"},
        "sort": 1
      }
    ]
  },
  "panels": [
    {
      "id": 1,
      "title": "Query Latency Distribution",
      "type": "heatmap",
      "gridPos": {"h": 9, "w": 24, "x": 0, "y": 0},
      "targets": [
        {
          "expr": "increase(gorp_database_query_duration_seconds_bucket{instance=~\"$instance\",database=~\"$database\"}[5m])",
          "legendFormat": "{{le}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"},
          "format": "heatmap"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 80
          }
        }
      },
      "options": {
        "calculate": true,
        "yAxis": {
          "unit": "s"
        }
      }
    },
    {
      "id": 2,
      "title": "Slow Queries (>1s)",
      "type": "stat",
      "gridPos": {"h": 9, "w": 12, "x": 0, "y": 9},
      "targets": [
        {
          "expr": "increase(gorp_database_queries_slow_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
          "legendFormat": "{{instance}} {{database}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        }
      },
      "thresholds": {
        "mode": "absolute",
        "steps": [
          {"color": "green", "value": null},
          {"color": "yellow", "value": 1.0},
          {"color": "red", "value": 10.0}
        ]
      },
      "options": {
        "reduceOptions": {
          "values": false,
          "calcs": ["lastNotNull"]
        },
        "orientation": "auto",
        "textMode": "auto",
        "colorMode": "background"
      }
    },
    {
      "id": 3,
      "title": "Connection Pool Status",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 12, "x": 12, "y": 9},
      "targets": [
        {
          "expr": "gorp_database_connection_pool_max{instance=~\"$instance\",database=~\"$database\"}",
          "legendFormat": "Max - {{instance}} {{database}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        },
        {
          "expr": "gorp_database_connection_pool_in_use{instance=~\"$instance\",database=~\"$database\"}",
          "legendFormat": "In Use - {{instance}} {{database}}",
          "refId": "B",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        },
        {
          "expr": "gorp_database_connection_pool_idle{instance=~\"$instance\",database=~\"$database\"}",
          "legendFormat": "Idle - {{instance}} {{database}}",
          "refId": "C",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      }
    },
    {
      "id": 4,
      "title": "Database-Specific Metrics",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 24, "x": 0, "y": 18},
      "targets": [
        {
          "expr": "gorp_postgresql_buffer_cache_hit_ratio{instance=~\"$instance\",database=~\"$database\"}",
          "legendFormat": "PostgreSQL Buffer Cache Hit Ratio - {{instance}} {{database}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        },
        {
          "expr": "gorp_mysql_innodb_buffer_pool_reads{instance=~\"$instance\",database=~\"$database\"}",
          "legendFormat": "MySQL InnoDB Buffer Pool Reads - {{instance}} {{database}}",
          "refId": "B",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        },
        {
          "expr": "gorp_sqlite_page_cache_hits{instance=~\"$instance\",database=~\"$database\"}",
          "legendFormat": "SQLite Page Cache Hits - {{instance}} {{database}}",
          "refId": "C",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      }
    }
  ]
}
EOF

# Generate Security Dashboard
cat > "$DASHBOARDS_DIR/gorp-security.json" << 'EOF'
{
  "uid": "gorp-security",
  "title": "GORP Security Monitoring",
  "tags": ["gorp", "security", "audit"],
  "style": "dark",
  "timezone": "browser",
  "editable": true,
  "hideControls": false,
  "graphTooltip": 1,
  "time": {
    "from": "now-24h",
    "to": "now"
  },
  "refresh": "30s",
  "schemaVersion": 27,
  "version": 1,
  "templating": {
    "list": [
      {
        "name": "instance",
        "type": "query",
        "label": "Instance",
        "query": "label_values(gorp_security_violations_total, instance)",
        "refresh": 1,
        "current": {"selected": true, "text": "All", "value": "$__all"},
        "hide": 0,
        "includeAll": true,
        "multi": true,
        "allValue": ".*",
        "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"},
        "sort": 1
      },
      {
        "name": "database",
        "type": "query",
        "label": "Database",
        "query": "label_values(gorp_security_violations_total{instance=~\"$instance\"}, database)",
        "refresh": 1,
        "current": {"selected": true, "text": "All", "value": "$__all"},
        "hide": 0,
        "includeAll": true,
        "multi": true,
        "allValue": ".*",
        "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"},
        "sort": 1
      }
    ]
  },
  "panels": [
    {
      "id": 1,
      "title": "Security Violations",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 12, "x": 0, "y": 0},
      "targets": [
        {
          "expr": "rate(gorp_security_violations_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
          "legendFormat": "{{instance}} {{database}} {{violation_type}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      },
      "thresholds": {
        "mode": "absolute",
        "steps": [
          {"color": "green", "value": null},
          {"color": "red", "value": 0.1}
        ]
      }
    },
    {
      "id": 2,
      "title": "Failed Authentication Attempts",
      "type": "stat",
      "gridPos": {"h": 9, "w": 12, "x": 12, "y": 0},
      "targets": [
        {
          "expr": "increase(gorp_security_auth_failed_total{instance=~\"$instance\",database=~\"$database\"}[1h])",
          "legendFormat": "{{instance}} {{database}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        }
      },
      "thresholds": {
        "mode": "absolute",
        "steps": [
          {"color": "green", "value": null},
          {"color": "yellow", "value": 5.0},
          {"color": "red", "value": 20.0}
        ]
      },
      "options": {
        "colorMode": "background"
      }
    },
    {
      "id": 3,
      "title": "SQL Injection Attempts",
      "type": "stat",
      "gridPos": {"h": 9, "w": 12, "x": 0, "y": 9},
      "targets": [
        {
          "expr": "increase(gorp_security_sql_injection_blocked_total{instance=~\"$instance\",database=~\"$database\"}[1h])",
          "legendFormat": "{{instance}} {{database}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        }
      },
      "thresholds": {
        "mode": "absolute",
        "steps": [
          {"color": "green", "value": null},
          {"color": "red", "value": 1.0}
        ]
      },
      "options": {
        "colorMode": "background"
      }
    },
    {
      "id": 4,
      "title": "Audit Events",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 12, "x": 12, "y": 9},
      "targets": [
        {
          "expr": "rate(gorp_security_audit_events_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
          "legendFormat": "{{instance}} {{database}} {{event_type}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      }
    },
    {
      "id": 5,
      "title": "Connection Security Events",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 12, "x": 0, "y": 18},
      "targets": [
        {
          "expr": "rate(gorp_security_connection_events_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
          "legendFormat": "{{instance}} {{database}} {{event_type}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      }
    },
    {
      "id": 6,
      "title": "Parameter Sanitization Events",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 12, "x": 12, "y": 18},
      "targets": [
        {
          "expr": "rate(gorp_security_sanitization_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
          "legendFormat": "{{instance}} {{database}} {{sanitization_type}}",
          "refId": "A",
          "datasource": {"type": "prometheus", "uid": "${PROMETHEUS_UID}"}
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 2,
            "fillOpacity": 10
          }
        }
      }
    }
  ]
}
EOF

echo "Dashboard generation complete!"
echo "Generated dashboards:"
echo "  - $DASHBOARDS_DIR/gorp-overview.json"
echo "  - $DASHBOARDS_DIR/gorp-performance.json"
echo "  - $DASHBOARDS_DIR/gorp-security.json"
echo ""
echo "To import these dashboards into Grafana:"
echo "1. Open Grafana UI"
echo "2. Go to '+' (Create) -> Import"
echo "3. Upload the JSON files or copy/paste their contents"
echo "4. Configure the Prometheus datasource UID if needed"
echo ""
echo "Environment variables used:"
echo "  PROMETHEUS_UID=$PROMETHEUS_UID"
echo "  NAMESPACE=$NAMESPACE"