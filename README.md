# dbt Data Vault Warehouse Orchestrator

An automated pipeline and validation engine for Data Vault 2.0 architectures (Hubs, Links, Satellites) and dbt-compatible warehouse staging layers.

## Capabilities

- **Data Vault 2.0 Primitives**: Hub, Link, and Satellite modeling with deterministic SHA-256 hash keys and change data capture (CDC) hash diffs compatible with Snowflake and BigQuery.
- **dbt Transformation Staging**: Compiles raw transactional entity streams into staged vault models.
- **Integrity Validation**: Enforces referential integrity across Link relationships and detects attribute drift.

## Usage

```bash
pip install -e ".[dev]"
pytest -v
```
