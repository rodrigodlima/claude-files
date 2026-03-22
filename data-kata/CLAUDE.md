# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

"TOP Salesman - Data KATA" is a real-time data pipeline that ingests sales data from 3 heterogeneous sources (PostgreSQL CDC, CSV files via MinIO, SOAP service), normalizes and aggregates them through Kafka Streams, and exposes results via a REST API backed by TimescaleDB.

## Commands

### Start the full environment

```bash
docker compose up -d --build   # Build and start all services
docker compose ps              # Check container status
docker compose logs -f         # Stream logs
docker compose down            # Stop services
docker compose down -v         # Stop and remove volumes
./clean.sh                     # Full cleanup (containers, networks, volumes, local images)
```

### Run tests

```bash
./run_tests.sh          # Auto-detect Java 25 locally, fall back to Docker
./run_tests.sh local    # Require local Java 25 (SDKMAN)
./run_tests.sh docker   # Run tests inside Docker (maven:3.9-eclipse-temurin-25)
```

To run tests for a single Java service:

```bash
cd services/<service-name>
mvn test
mvn test -Dtest=ClassName          # Run a specific test class
mvn test -Dtest=ClassName#method   # Run a single test method
```

### Query the API (after startup)

```bash
curl http://localhost:8090/health
curl http://localhost:8090/api/aggregates/top-sales-per-city
curl http://localhost:8090/api/aggregates/top-salesman-country
curl http://localhost:8090/api/aggregates/summary
curl "http://localhost:8090/api/aggregates/summary?from=2026-03-13T00:00:00Z&to=2026-03-13T23:59:59Z"
```

## Architecture

### Data Flow

```
PostgreSQL → Debezium CDC → Kafka (electromart.public.*) → SalesEnricher → raw_postgres
MinIO CSV  → Webhook      → CsvConnector                              → raw_csv
SOAP svc   → Poll (5s)    → SoapConnector                            → raw_soap
                                                                          ↓
                                                              SalesAggregator (Kafka Streams)
                                                                          ↓
                                                              sales topic / sales-dlq
                                                                          ↓
                                                              SalesConsumer → TimescaleDB
                                                                          ↓
                                                              REST API (port 8090)
```

### Services

**Data sources** (`data-sources/`, Node.js 25):
- `postgresql/` — inserts random sales every 5s into PostgreSQL
- `csv-files/` — generates CSV files and uploads to MinIO every 5s
- `soap/` — Express.js mock SOAP server on port 8080

**Java services** (`services/`, Java 25 + Maven):
- `postgres-connector-source` — one-time Debezium connector registration via Kafka Connect REST API
- `postgres-enricher` — Kafka Streams topology; joins CDC sales events with product/salesman/store GlobalKTables; outputs enriched records to `raw_postgres`
- `csv-connector-source` — HTTP webhook (port 8085) that receives MinIO events, reads CSV, validates, publishes to `raw_csv`
- `soap-connector-source` — polls SOAP service every 5s, parses XML, validates, publishes to `raw_soap`
- `sales-aggregator` — merges all 3 raw topics, validates required fields, routes valid records to `sales`, invalid to `sales-dlq`; instruments with OpenTelemetry
- `sales-consumer` — Kafka consumer that writes to TimescaleDB and serves a REST API; deduplicates on `sale_id`; emits OpenTelemetry metrics (latency, revenue, duplicates)

### Kafka Topics

| Topic | Producer | Consumer |
|---|---|---|
| `electromart.public.sales/products/salesmen/stores` | Debezium | SalesEnricher |
| `raw_postgres` | SalesEnricher | SalesAggregator |
| `raw_csv` | CsvConnector | SalesAggregator |
| `raw_soap` | SoapConnector | SalesAggregator |
| `sales` | SalesAggregator | SalesConsumer |
| `sales-dlq` | SalesAggregator | (monitoring) |

### Avro Schemas

Managed by Confluent Schema Registry. Schemas are in `services/schema-registry-init/schemas/`:
- `raw-csv-value.json`, `raw-soap-value.json`, `raw-postgres-value.json` — source-specific
- `sales-value.json` — canonical normalized schema

### Observability Ports

| Service | Port |
|---|---|
| Sales Consumer API | 8090 |
| Grafana | 3000 (admin/admin) |
| Kafka UI | 8888 |
| Kafka Connect | 8083 |
| SOAP Service | 8080 |
| MinIO Console | 9001 (minioadmin/minioadmin123) |
| PostgreSQL | 5432 (electromart/electromart123) |
| TimescaleDB | 5433 (sales/sales123) |
| Jaeger UI | 16686 |

### Tech Stack

- **Streaming**: Apache Kafka 3.7.0, Kafka Streams, Debezium CDC
- **Languages**: Java 25, Node.js 25
- **Build**: Maven 3.9+
- **Storage**: PostgreSQL 18 (source), TimescaleDB (sink), MinIO (S3)
- **Observability**: OpenTelemetry 1.50.0, Prometheus, Grafana, Jaeger
- **Testing**: JUnit 5 (Jupiter)
