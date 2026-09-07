# Databricks notebook source
#!/usr/bin/env python
"""Discover atlas_prod subfolders in S3 and regenerate the bronze config.

Rovia's landing bucket (s3://rovia-dms-s3-lake-databricks/atlas_prod/) has
one subfolder per source table (an AWS DMS S3-target layout). This script:

  1. Lists the first-level subfolders under the landing root (each = one
     table) and, for each, samples one object to guess its file format.
  2. (Re)writes config/rovia_ai_co_pilot_workspace/dev_bronze/atlas_tables.yml
     — one `parameter_sets` entry per table, expanding
     ../templates/atlas_bronze_ingest.yml (see docs/FRAMEWORK.md "Add many
     tables — templates" in datalab-framework).
  3. (Re)writes the generated block in
     resources/rovia_ingestion_etl.pipeline.yml `configuration:` — one
     `datalab.landing_<table>` key per table, resolving the template's
     `path_conf: datalab.landing_${param.table}` to the real s3:// path
     (the framework requires path_conf to be a conf KEY, never a literal
     path — see docs/FRAMEWORK.md "Sources").

Author-time only: this script and its `boto3`/`PyYAML` deps never run inside
the pipeline — Auto Loader reads S3 directly once the workspace has a Unity
Catalog external location / storage credential granting access to the
bucket. Safe to re-run any time the bucket gains or loses a table; it is
idempotent and only rewrites the two generated sections, in place.

Usage:
    uv run python scripts/generate_bronze_tables.py
    uv run python scripts/generate_bronze_tables.py --dry-run
    uv run python scripts/generate_bronze_tables.py --profile my-aws-profile
    uv run python scripts/generate_bronze_tables.py \
        --s3-uri s3://rovia-dms-s3-lake-databricks/atlas_prod --region us-east-1

Requires AWS credentials with s3:ListBucket on the landing bucket (env vars,
`~/.aws/credentials`, an SSO profile via --profile, or an assumed role) —
this is a local/CI discovery step, independent of the workspace's own S3
access used by the deployed pipeline.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LANDING_URI = "s3://rovia-dms-s3-lake-databricks/atlas_prod"
INSTANCE_FILE = (
    PROJECT_ROOT / "config" / "rovia_ai_co_pilot_workspace" / "dev_bronze" / "atlas_tables.yml"
)
PIPELINE_FILE = PROJECT_ROOT / "resources" / "rovia_ingestion_etl.pipeline.yml"

BEGIN_MARKER = "# --- BEGIN GENERATED ATLAS_PROD LANDING PATHS ---"
END_MARKER = "# --- END GENERATED ATLAS_PROD LANDING PATHS ---"

# Auto Loader (cloudFiles) format names — https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options
_EXTENSION_TO_FORMAT = {
    ".parquet": "parquet",
    ".csv": "csv",
    ".tsv": "csv",
    ".json": "json",
    ".jsonl": "json",
    ".avro": "avro",
    ".orc": "orc",
    ".txt": "text",
}
DEFAULT_FORMAT = "parquet"  # AWS DMS S3-target default
# FAQs are owned by the dedicated rag_documents pipeline, not the atlas fleet.
EXCLUDED_TABLES = {"faqs"}


@dataclass(frozen=True)
class BronzeTable:
    name: str  # sanitized -> valid table Identifier
    source_folder: str  # raw S3 subfolder name (may differ from `name`)
    format: str


def sanitize_identifier(raw: str) -> str:
    """Map an arbitrary S3 folder name to the framework's Identifier pattern
    (``^[A-Za-z_][A-Za-z0-9_]*$`` — see datalab-framework/src/datalab/framework/models.py)."""
    name = raw.strip().strip("/").lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        raise ValueError(f"S3 folder {raw!r} sanitizes to an empty table name")
    if not re.match(r"^[a-z_]", name):
        name = f"t_{name}"
    return name


def discover_tables(s3_uri: str, *, profile: str | None, region: str | None) -> list[BronzeTable]:
    import boto3

    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"--s3-uri must look like s3://bucket/prefix, got {s3_uri!r}")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    session = boto3.Session(profile_name=profile, region_name=region)
    client = session.client("s3")

    subfolders: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            folder = common_prefix["Prefix"][len(prefix) :].strip("/")
            if folder:
                subfolders.append(folder)

    tables: list[BronzeTable] = []
    seen_names: dict[str, str] = {}
    for folder in sorted(subfolders):
        if folder in EXCLUDED_TABLES:
            continue
        name = sanitize_identifier(folder)
        if name in seen_names:
            raise ValueError(
                f"S3 subfolders {seen_names[name]!r} and {folder!r} both sanitize to "
                f"table name {name!r} — rename one or extend sanitize_identifier()"
            )
        seen_names[name] = folder
        fmt = _detect_format(client, bucket, f"{prefix}{folder}/")
        tables.append(BronzeTable(name=name, source_folder=folder, format=fmt))
    return tables


def _detect_format(client, bucket: str, folder_prefix: str) -> str:
    response = client.list_objects_v2(Bucket=bucket, Prefix=folder_prefix, MaxKeys=10)
    for obj in response.get("Contents", []):
        key = obj["Key"]
        suffix = Path(key).suffix.lower()
        fmt = _EXTENSION_TO_FORMAT.get(suffix)
        if fmt:
            return fmt
    return DEFAULT_FORMAT


def render_instance_file(tables: list[BronzeTable]) -> str:
    lines = [
        "# yaml-language-server: $schema=../../../schemas/template_instance.schema.json",
        "#",
        "# GENERATED FILE — do not hand-edit the parameter_sets list.",
        "# Regenerate with: uv run python scripts/generate_bronze_tables.py",
        "#",
        "# One parameter_sets entry per subfolder discovered under",
        f"# {DEFAULT_LANDING_URI}/ — each expands the",
        "# ../templates/atlas_bronze_ingest.yml template into one bronze streaming",
        "# table. This file's own name is free (it doesn't name a single table); the",
        "# expanded table names must each be unique within dev_bronze/.",
        "schema_version: 1",
        "template: atlas_bronze_ingest",
        "parameter_sets:",
    ]
    for table in tables:
        comment = f"  # s3 folder: {table.source_folder}" if table.source_folder != table.name else ""
        lines.append(f"  - {{table: {table.name}, format: {table.format}}}{comment}")
    return "\n".join(lines) + "\n"


def render_configuration_block(tables: list[BronzeTable], landing_root: str) -> str:
    root = landing_root.rstrip("/")
    lines = [BEGIN_MARKER]
    lines.append(
        "        # Managed by scripts/generate_bronze_tables.py — one"
    )
    lines.append(
        "        # datalab.landing_<table> key per s3://.../atlas_prod/<table>/"
    )
    lines.append(
        "        # subfolder, matching path_conf in the atlas_bronze_ingest template."
    )
    lines.append("        # Do not hand-edit this block; re-run the script instead.")
    for table in tables:
        lines.append(f"        datalab.landing_{table.name}: {root}/{table.source_folder}/")
    lines.append(END_MARKER)
    return "\n".join(lines)


def update_pipeline_file(path: Path, tables: list[BronzeTable], landing_root: str) -> str:
    text = path.read_text(encoding="utf-8")
    if BEGIN_MARKER not in text or END_MARKER not in text:
        raise ValueError(f"{path}: missing {BEGIN_MARKER!r} / {END_MARKER!r} markers")
    pattern = re.compile(
        re.escape(BEGIN_MARKER) + r".*?" + re.escape(END_MARKER), re.DOTALL
    )
    replacement = render_configuration_block(tables, landing_root)
    new_text, count = pattern.subn(replacement, text)
    if count != 1:
        raise ValueError(f"{path}: expected exactly one generated block, found {count}")
    return new_text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--s3-uri", default=DEFAULT_LANDING_URI, help="Landing root (default: %(default)s)")
    parser.add_argument("--profile", default=None, help="AWS CLI/SSO profile to use")
    parser.add_argument("--region", default=None, help="AWS region (default: profile/env default)")
    parser.add_argument(
        "--dry-run", action="store_true", help="Discover and print the plan without writing any files"
    )
    args = parser.parse_args(argv)

    print(f"Discovering tables under {args.s3_uri} ...", file=sys.stderr)
    tables = discover_tables(args.s3_uri, profile=args.profile, region=args.region)
    if not tables:
        print(f"No subfolders found under {args.s3_uri} — nothing to generate.", file=sys.stderr)
        return 1

    print(f"Found {len(tables)} table(s):", file=sys.stderr)
    for table in tables:
        marker = "" if table.source_folder == table.name else f"  (from folder: {table.source_folder})"
        print(f"  - {table.name:<40} format={table.format}{marker}", file=sys.stderr)

    instance_contents = render_instance_file(tables)
    pipeline_contents = update_pipeline_file(PIPELINE_FILE, tables, args.s3_uri)

    if args.dry_run:
        print(f"\n--dry-run: would write {INSTANCE_FILE}", file=sys.stderr)
        print(f"--dry-run: would update {PIPELINE_FILE}", file=sys.stderr)
        return 0

    INSTANCE_FILE.write_text(instance_contents, encoding="utf-8")
    PIPELINE_FILE.write_text(pipeline_contents, encoding="utf-8")
    print(f"\nWrote {INSTANCE_FILE}", file=sys.stderr)
    print(f"Updated {PIPELINE_FILE}", file=sys.stderr)
    print(
        "\nNext: uv run pytest (validates the config locally), then "
        "make validate && make deploy.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
