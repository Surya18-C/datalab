# Rovia_Ingestion

Bronze ingestion of `s3://rovia-dms-s3-lake-databricks/atlas_prod` into
**`rovia_ai_co_pilot_workspace.dev_bronze`** — one streaming table per
subfolder — built on the
[datalab-framework](https://github.com/dediggibyte/datalab-framework)
(config-driven Lakeflow Spark Declarative Pipelines). This project holds the
configs, the deploy bundle, and one generator script; the framework does the
actual generation of the pipeline graph.

Workspace: <https://dbc-81f292a5-d857.cloud.databricks.com>

## How a table gets ingested

Each `atlas_prod/<folder>/` becomes one bronze streaming table via Auto
Loader, schema preserved as-is (no column mapping at this layer — that is a
silver-layer concern if this domain grows one). Adding, removing, or
renaming a table is **never** a hand-edited YAML change here — it's a
re-run of the discovery script:

```
atlas_prod/customers/*.parquet  ──┐
atlas_prod/orders/*.parquet     ──┼─→ scripts/generate_bronze_tables.py ─→ config/ + resources/
atlas_prod/<...>/*.parquet      ──┘
```

The framework's
[template mechanism](https://github.com/dediggibyte/datalab-framework/blob/main/docs/FRAMEWORK.md#add-many-tables--templates-template--parameter_sets)
("the classic bronze ingest fleet") makes this a *generated instance list*,
not N hand-written files:

- [`config/rovia_ai_co_pilot_workspace/templates/atlas_bronze_ingest.yml`](config/rovia_ai_co_pilot_workspace/templates/atlas_bronze_ingest.yml)
  — the one parameterized table spec (author this by hand; it rarely changes).
- [`config/rovia_ai_co_pilot_workspace/dev_bronze/atlas_tables.yml`](config/rovia_ai_co_pilot_workspace/dev_bronze/atlas_tables.yml)
  — **generated**: one `{table, format}` entry per discovered subfolder.
- [`resources/rovia_ingestion_etl.pipeline.yml`](resources/rovia_ingestion_etl.pipeline.yml)
  — the pipeline's `configuration:` block has a **generated** section with
  one `datalab.landing_<table>` key per table, resolving each table's real
  `s3://` path (the framework requires `source.path_conf` to be a conf KEY,
  never a literal path — so the literal atlas_prod paths live here, not in
  the table configs).

## Prerequisites

- **uv** and **Java 17+** (local SparkSession for tests/`capture_graph`).
- **Databricks CLI ≥ 0.294.0**, authenticated against the workspace above:
  `databricks auth login --host https://dbc-81f292a5-d857.cloud.databricks.com`
- AWS credentials with `s3:ListBucket` on `rovia-dms-s3-lake-databricks`, for
  the discovery script only (`~/.aws/credentials`, env vars, or `--profile`).
- **One-time, before the first deploy** (kept out of the bundle lifecycle —
  a bundle destroy must never touch governance state):
  - Catalog `rovia_ai_co_pilot_workspace` and schema `dev_bronze` must exist.
  - A Unity Catalog **external location** (or storage credential) granting
    this workspace read access to `s3://rovia-dms-s3-lake-databricks/atlas_prod`.
  - An `rovia_ai_co_pilot_workspace.artifacts.wheels` volume, for the
    framework wheel (`make publish-wheel` from `datalab-framework`, or your
    own CI equivalent — see
    [ADOPTING.md](https://github.com/dediggibyte/datalab/blob/main/docs/ADOPTING.md)).

## Quick start

```bash
uv sync                                        # .venv from pyproject.toml
uv run python scripts/generate_bronze_tables.py   # discover tables, write config/
uv run pytest                                  # validate the config locally (no workspace)

databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run rovia_ingestion_etl -t dev
```

### Discover tables

```bash
uv run python scripts/generate_bronze_tables.py               # default profile/region
uv run python scripts/generate_bronze_tables.py --profile rovia-dev
uv run python scripts/generate_bronze_tables.py --dry-run      # preview, writes nothing
```

Re-run it any time atlas_prod gains, loses, or renames a subfolder — it
rewrites only the two generated sections
(`config/.../dev_bronze/atlas_tables.yml` and the marked block in
`resources/rovia_ingestion_etl.pipeline.yml`) in place, idempotently. It also
samples one object per subfolder to guess the Auto Loader `format`
(`parquet`/`csv`/`json`/`avro`/`orc`/`text`; defaults to `parquet`, the AWS
DMS S3-target default, when nothing is found). Sanity-check the output
before deploying — grep for `format: parquet` if a table you expect isn't
Parquet.

### Validate locally (no workspace)

```bash
uv run pytest    # tests/test_pipeline_graph.py — capture_graph over config/
uv run ruff check src tests scripts
```

`tests/test_pipeline_graph.py` reads whatever tables are currently in
`atlas_tables.yml` and asserts the framework builds one streaming table per
table — the same check the pipeline does at import, run locally.

## Project layout

```
Rovia_Ingestion/
  pyproject.toml                        pins datalab (git dep, see "Version" below)
  databricks.yml                        bundle: variables + dev target
  config/rovia_ai_co_pilot_workspace/
    domain.yml                          domain settings (catalog/schema conf keys)
    templates/atlas_bronze_ingest.yml   the one parameterized bronze-ingest spec
    dev_bronze/atlas_tables.yml         GENERATED — one entry per atlas_prod subfolder
  src/rovia_ingestion_etl/
    transformations/pipeline.py         the 3-line pipeline entrypoint
  resources/rovia_ingestion_etl.pipeline.yml   SDP pipeline + per-table landing paths
  scripts/generate_bronze_tables.py     discovers atlas_prod subfolders, regenerates config/
  schemas/*.json                        vendored editor-validation schemas (pinned framework version)
  tests/test_pipeline_graph.py          capture_graph smoke test, no workspace needed
```

## Adding logic beyond raw ingestion

Bronze here is a pure passthrough by design (schema preserved, Auto Loader
`schemaEvolutionMode: addNewColumns`). If a table later needs column
renames/casts, dqx quality rules, or a silver layer, add `columns:` /
`quality:` / a `silver/` folder — see
[FRAMEWORK.md](https://github.com/dediggibyte/datalab-framework/blob/main/docs/FRAMEWORK.md)
in the framework repo, or copy the `customer_sales` example from
[dediggibyte/datalab](https://github.com/dediggibyte/datalab/tree/main/projects/customer_sales).

## Version

Pinned to `datalab-framework` **v0.10.0** in both
[`pyproject.toml`](pyproject.toml) (git dependency) and
[`databricks.yml`](databricks.yml) (`var.datalab_version`, the UC-Volume
wheel) — keep the two in lockstep when upgrading; see
[CHANGELOG.md](https://github.com/dediggibyte/datalab-framework/blob/main/CHANGELOG.md)
before bumping.
