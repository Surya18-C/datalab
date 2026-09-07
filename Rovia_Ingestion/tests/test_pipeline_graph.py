# Databricks notebook source
"""Generated-graph contract for the atlas_prod bronze fleet.

capture_graph registers every LDP object the config tree would create —
locally, with no Databricks — so this fails fast if
scripts/generate_bronze_tables.py produced something the framework can't
validate, before it ever reaches a deploy. Table names are read from the
generated instance file rather than hardcoded, so this test tracks whatever
tables are currently configured.
"""

from pathlib import Path

import pytest
import yaml

from datalab.framework.devtools import capture_graph

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "rovia_ai_co_pilot_workspace"
INSTANCE_FILE = CONFIG_DIR / "dev_bronze" / "atlas_tables.yml"

CATALOG = "cat"
BRONZE_SCHEMA = "dev_bronze"


def _configured_table_names() -> list[str]:
    instance = yaml.safe_load(INSTANCE_FILE.read_text(encoding="utf-8"))
    return [entry["table"] for entry in instance["parameter_sets"]]


@pytest.fixture()
def configured_spark(spark):
    spark.conf.set("datalab.catalog", CATALOG)
    spark.conf.set("datalab.bronze_schema", BRONZE_SCHEMA)
    for table in _configured_table_names():
        spark.conf.set(f"datalab.landing_{table}", f"/tmp/atlas_prod/{table}")
    return spark


def test_one_streaming_table_per_configured_atlas_table(configured_spark):
    tables = _configured_table_names()
    assert tables, "atlas_tables.yml has no parameter_sets — run generate_bronze_tables.py"

    captured = capture_graph(configured_spark, CONFIG_DIR)
    names = {name for _, name in captured}
    kinds = {name: kind for kind, name in captured if kind != "Flow"}

    for table in tables:
        target = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
        assert {f"{table}__input", target} <= names, f"missing dataset graph for {table!r}"
        assert kinds[target] == "StreamingTable"


