# Kafka Event Simulator — Backend Design & API Schema

## 1. Purpose

A standalone Java (Spring Boot) backend that simulates the upstream producer system, generating synthetic events at a configurable rate (with burst/late-event behavior) and pushing them to Kafka, so component X's batching + bucket-storage pipeline can be load-tested independent of the real upstream. Fully decoupled — usable via Postman/curl, with Streamlit as one client among possibly others. MongoDB backs persistence. Single job runs at a time. No auth for now.

---

## 2. High-Level Flow

```
Client (Streamlit / Postman)
        │
        ▼
POST /jobs  (job config)
        │
        ▼
JobService: validate → persist to MongoDB (jobs collection) → hand off to JobExecutor
        │
        ▼
JobExecutor (background thread):
   ├── RateScheduler   → computes target rate at time t (steady / burst / ramp)
   ├── PayloadGenerator → builds event JSON (fixed fields, ~1-1.5KB, seq no., timestamp)
   ├── LateEventInjector → decides per-event if it's "late", back-dates its timestamp field
   └── KafkaProducerPool → sends event to configured topic/partition strategy
        │
        ▼
Kafka topic (24 partitions) → component X consumes → batches/persists to bucket storage
```

The client only ever talks to the REST API. Everything from rate-limiting to Kafka production happens inside `JobExecutor`, invisible to the client beyond status/metrics polling.

---

## 3. Job Configuration Schema

This is the single object that flows from UI → `POST /jobs` → MongoDB `jobs` collection → `JobExecutor`. Every UI field maps 1:1 to a field here.

```json
{
  "jobName": "string, required — human-readable tag for history/reuse",

  "rate": {
    "steadyEventsPerSecond": "number, required — baseline rate (e.g. 770)",
    "burst": {
      "enabled": "boolean",
      "multiplier": "number — e.g. 4 (for 4x peak)",
      "pattern": "enum: NONE | STEP | LINEAR_RAMP | SINUSOIDAL",
      "schedule": {
        "startOffsetSeconds": "number — when burst begins after job start",
        "rampUpSeconds": "number — time to reach peak (0 for STEP)",
        "holdSeconds": "number — time to sustain peak",
        "rampDownSeconds": "number — time to return to steady (0 for STEP)"
      }
    }
  },

  "duration": {
    "mode": "enum: FIXED_DURATION | UNTIL_EVENT_CAP | UNTIL_STOPPED",
    "totalSeconds": "number — used if mode = FIXED_DURATION",
    "totalEventCap": "number — used if mode = UNTIL_EVENT_CAP"
  },

  "lateEvents": {
    "enabled": "boolean",
    "percentage": "number 0-100 — % of events marked late",
    "delayRangeMinutes": {
      "min": "number — e.g. 5",
      "max": "number — e.g. 360 (capped at 6 hours)"
    },
    "distribution": "enum: UNIFORM | CLUSTERED_NEAR_BOUNDARY"
  },

  "kafkaTarget": {
    "topic": "string, required",
    "partitionStrategy": {
      "type": "enum: ROUND_ROBIN | KEYED_RANDOM_POOL | KEYED_FIXED_POOL",
      "keyPoolSize": "number — used if type involves a key pool"
    },
    "producerProfile": {
      "preset": "enum: DEFAULT | HIGH_THROUGHPUT | LOW_LATENCY | CUSTOM",
      "overrides": {
        "acks": "string — e.g. '1', 'all'",
        "lingerMs": "number",
        "batchSize": "number",
        "compressionType": "enum: NONE | GZIP | SNAPPY | LZ4 | ZSTD",
        "bufferMemory": "number",
        "maxInFlightRequestsPerConnection": "number"
      }
    }
  },

  "payload": {
    "targetSizeBytes": "number — e.g. 1200 (1-1.5KB range)",
    "fieldsTemplate": "reference to a fixed schema/template (fields are static, only values vary — sequence no. and timestamp are always injected)"
  },

  "dryRun": "boolean — if true, generate & log locally, do not send to Kafka"
}
```

**Notes on why fields are shaped this way:**
- `rate.burst` is nested and optional-by-`enabled` so a plain steady-rate job doesn't need to specify a schedule at all.
- `duration.mode` is an enum because "run for X seconds" and "run until N events" and "run until stopped" are mutually exclusive control modes — better as one field than three ambiguous optional fields.
- `lateEvents.delayRangeMinutes` is validated server-side to reject max > 360 (your 6-hour ceiling) rather than trusting the client.
- `producerProfile.preset` gives quick presets for Streamlit dropdowns, but `overrides` lets Postman/advanced users set any Kafka producer property directly.

---

## 4. API Endpoints

### `POST /jobs`
Creates and immediately starts a job.

- **Request body:** the Job Configuration Schema above.
- **Validation:** rejects if a job is already `RUNNING`/`PAUSED` (single-job constraint) → `409 Conflict`. Rejects invalid ranges (e.g. late delay > 360 min, negative rates) → `400 Bad Request`.
- **Response (`201 Created`):**
```json
{
  "jobId": "string (Mongo ObjectId)",
  "status": "CREATED",
  "config": { "...echoed back..." }
}
```

### `GET /jobs/{id}`
Returns current state of a job.

```json
{
  "jobId": "string",
  "jobName": "string",
  "status": "CREATED | RUNNING | PAUSED | COMPLETED | STOPPED | FAILED",
  "config": { "...full config..." },
  "startedAt": "ISO timestamp",
  "endedAt": "ISO timestamp or null",
  "counters": {
    "eventsSent": "number",
    "eventsFailed": "number",
    "lateEventsSent": "number",
    "onTimeEventsSent": "number",
    "currentEffectiveRate": "number — actual events/sec right now"
  }
}
```

### `POST /jobs/{id}/pause`, `/resume`, `/stop`, `/kill`
No body needed. `stop` = graceful (flush producer buffer, finalize counters, mark `COMPLETED`). `kill` = immediate abort, mark `STOPPED`, counters reflect whatever was sent so far.

- **Response:** same shape as `GET /jobs/{id}`, reflecting new status.

### `GET /jobs`
Lists past/current jobs (history).

- **Query params:** `status` (optional filter), `limit`, `offset`.
- **Response:** array of job summaries (same shape as `GET /jobs/{id}` minus full config, to keep it light — full config available via the single-job endpoint).

### `POST /scenarios` / `GET /scenarios`
Save/list reusable job configs without running them (the "save as scenario" UI checkbox). Same schema as job config, just no execution/state fields.

---

## 5. How Parameters Drive Execution

| UI Input | Config Field | Effect at runtime |
|---|---|---|
| Steady rate | `rate.steadyEventsPerSecond` | Baseline tick rate for `RateScheduler`'s token bucket |
| Burst multiplier + pattern + schedule | `rate.burst.*` | `RateScheduler` computes a time-varying target rate; STEP jumps instantly, LINEAR_RAMP interpolates, SINUSOIDAL follows a sine curve between steady and peak |
| Duration / event cap | `duration.*` | `JobExecutor` checks this each tick to decide when to auto-transition to `COMPLETED` |
| Late event % + delay range + distribution | `lateEvents.*` | Per generated event, `LateEventInjector` rolls against `percentage`; if "late," picks a delay from the range (uniform or boundary-clustered) and sets the event's *payload timestamp* to `now - delay`, while still sending it to Kafka *now* |
| Topic + partition strategy | `kafkaTarget.*` | `KafkaProducerPool` uses this to decide the `ProducerRecord`'s key (null for round-robin, pooled key otherwise) and target topic |
| Producer profile | `kafkaTarget.producerProfile` | Maps to actual `KafkaProducer` config properties at pool initialization |
| Dry run toggle | `dryRun` | If true, `KafkaProducerPool` is swapped for a no-op logger — same code path, no real Kafka write |

---

## 6. How Events Actually Reach Kafka

1. `JobExecutor` runs a scheduling loop (fixed tick interval, e.g. every 100ms).
2. Each tick, `RateScheduler` reports "how many events should have been sent by now" based on elapsed time and the current phase (steady/ramp/peak) — this smooths out tick granularity instead of trying to hit an exact per-tick count.
3. For each event to send this tick:
   - `PayloadGenerator` builds the fixed-field JSON, injects a monotonic sequence number and a real send-timestamp field.
   - `LateEventInjector` optionally overwrites the *business* timestamp field (the one component X reads for bucketing) to simulate lateness — the Kafka message itself is still produced in real time.
   - `KafkaProducerPool` picks a producer instance (round-robin across the pool), builds the `ProducerRecord` with the configured key strategy, and sends asynchronously.
4. Producer callbacks increment `eventsSent`/`eventsFailed` counters in memory (persisted to Mongo later, once you add snapshotting — deferred as agreed).
5. Loop continues until `duration` condition is met or `/stop`/`/kill` is called.

---

## 7. Explicitly Deferred (per your last message)

- Periodic metrics snapshotting to MongoDB (`job_metrics` collection) — will layer on top of the counters already being tracked in memory.
- Fine-grained snapshot interval configuration.

These don't change the schema above — `counters` already exists in the job document; snapshotting just adds a time-series of that same shape.
