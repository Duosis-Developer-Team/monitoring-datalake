# API Contract

## `POST /api/v1/obm/metrics` (and `POST /api/v1/obm/metrics`)

Accepts an OBM-formatted JSON payload, archives it, normalizes the known metric
classes, and returns `200 OK` on acceptance.

### Request

- Headers: `Content-Type: application/json`; when behind Nginx with mTLS:
  - `X-SSL-Client-Verify` (`SUCCESS` for valid client cert)
  - `X-SSL-Client-Subject`
  - `X-SSL-Client-Issuer`
  - `X-SSL-Client-Fingerprint`
- Body: must be parseable as JSON. Supported envelopes:
  - A single record object.
  - An array of record objects.
  - A wrapper object containing the list under `records`, `data`, `items`,
    `payload`, or `metrics_records`.

### Record fields

The CTO mapping recognises these common fields (raw → normalized):

| OBM field | Normalized key |
|---|---|
| `CollectionConfigName` | `collection_policy_name` |
| `MonitoredSystem` | `node_fqdn` |
| `MonitoredSystemID` | `node_short_name` |
| `MonitoredSystemTimezone` | `node_timezone_offset_h` |
| `node_ip_type` | `node_ip_type` |
| `node_ipv4_address` | `node_ipv4_address` |
| `node_ipv6_address` | `node_ipv6_address` |
| `tenant_id` | `tenant_id` |
| `timestamp_utc` | `timestamp_utc_s` |
| `producer_instance_id` / `_type` | passthrough |
| `cmdb_global_id` / `cmdb_id` | passthrough |
| `collection_data_flow` / `collection_type` | passthrough |

Each record's metric values must be either:
- Under a `metrics` (or `metric_values`, `values`, `data`) sub-object, **or**
- Sprinkled at the top level using OBM prefixed keys (`GBL_`, `BYDSK_`, `FS_`,
  `BYCPU_`, `BYNETIF_`).

### Class detection

In order:
1. Explicit `class_name`, `class`, or `metric_class` field.
2. Prefix-based inference (majority wins across the record's metric keys):
   - `GBL_` → `GLOBAL`
   - `BYDSK_` → `DISK`
   - `FS_` → `FILESYSTEM`
   - `BYCPU_` → `CPU`
   - `BYNETIF_` → `NETIF`
3. Otherwise `UNKNOWN` — the record is still accepted; metrics go into
   `extra_metrics` and `target_table` is `null`.

### Responses

- **200 OK** on accepted ingestion:
  ```json
  {
    "status": "ok",
    "request_id": "<uuid>",
    "accepted": true,
    "record_count": 1,
    "raw_payload_ref": "raw/2026/05/24/<uuid>.json",
    "quarantined": false
  }
  ```
- **400 Bad Request** for malformed JSON or empty body.
- **401 Unauthorized** when `ENFORCE_PROXY_MTLS_HEADER=true` and the request did
  not arrive through a verified-mTLS proxy.
- **413 Payload Too Large** when body exceeds `MAX_BODY_BYTES`.
- **422 Unprocessable Entity** when `STRICT_VALIDATION=true` and the record is
  missing both `MonitoredSystem` and `MonitoredSystemID`.

### Normalization output (per record)

```json
{
  "request_id": "<uuid>",
  "received_at": "2026-05-24T13:00:00Z",
  "source": "obm",
  "datasource": "SCOPE",
  "class_name": "GLOBAL",
  "target_table": "opsb_agent_node",
  "common": { "node_fqdn": "...", "tenant_id": "..." },
  "metrics": { "cpu_util_pct": 42.3 },
  "extra_metrics": { "GBL_FUTURE_METRIC": 99.9 },
  "raw_payload_ref": "raw/2026/05/24/<uuid>.json"
}
```

### Class → table mapping

| `class_name` | `target_key` | `target_table` |
|---|---|---|
| `GLOBAL` | `SCOPE_GLOBAL` | `opsb_agent_node` |
| `CONFIGURATION` | `SCOPE_GLOBAL` | `opsb_agent_node` |
| `DISK` | `SCOPE_DISK` | `opsb_agent_disk` |
| `FILESYSTEM` | `SCOPE_FILESYSTEM` | `opsb_agent_filesys` |
| `CPU` | `SCOPE_CPU` | `opsb_agent_cpu` |
| `NETIF` | `SCOPE_NETIF` | `opsb_agent_netif` |

The full metric-name table lives in
[`app/mappings/collection_policy_summary.json`](../app/mappings/collection_policy_summary.json).
