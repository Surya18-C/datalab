# Databricks notebook source
"""Rovia ingestion pipeline and FAQ volume-copy task.

One streaming table per atlas_prod subfolder, generated entirely from
``config/rovia_ai_co_pilot_workspace/``: the framework validates the
template + its instance file and registers every LDP dataset. Add or remove
a table by re-running ``scripts/generate_bronze_tables.py`` — never edit
this file to add a table.
"""

from pathlib import PurePosixPath

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# The active session is the SDP-runtime session on Databricks and the session
# the OSS `spark-pipelines` CLI creates locally — one entrypoint, every profile.
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
try:
    from datalab.config import pipeline_conf
    from datalab.framework import generate

    config_dir = pipeline_conf(spark, "datalab.config_dir")
except (ModuleNotFoundError, ValueError):
    config_dir = None
if config_dir:
    generate(spark, config_dir)


def main() -> None:
    dbutils = DBUtils(spark)
    source = "s3://rovia-dms-s3-lake-databricks/rag_documents/faqs/"
    target = "/Volumes/rovia_ai_co_pilot_workspace/dev_bronze/faqs/"
    files = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.pdf")
        .load(source)
        .select("path")
        .collect()
    )
    dbutils.fs.mkdirs(target)
    for row in files:
        destination = target + PurePosixPath(row.path).name
        if not dbutils.fs.cp(row.path, destination):
            raise RuntimeError(f"Failed to copy {row.path} to {destination}")
    print(f"Copied {len(files)} FAQ PDF file(s) to {target}")
