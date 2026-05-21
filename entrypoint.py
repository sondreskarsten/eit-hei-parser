"""Pipeline entrypoint for the EIT-HEI parser.

Reads raw JSONL snapshots from GCS, resolves Norwegian partners via
name matching against enheter + known-institution dictionary, and
emits unified 12-column changelog.

Modes:

* ``daily`` — read latest snapshot, resolve, run CDC.
* ``bootstrap`` — read latest snapshot, emit all as new.
* ``check`` — print snapshot metadata.

Environment variables:

======================== ============================================= =================
Variable                 Description                                   Default
======================== ============================================= =================
GCS_BUCKET               GCS bucket                                    sondre_brreg_data
GCS_PREFIX               Path prefix                                   eit_hei
RUN_MODE                 ``daily``, ``bootstrap``, or ``check``        daily
SNAPSHOT_DATE            Specific snapshot date                        (auto: latest)
ENHETER_BUCKET           Bucket for enheter snapshots                  sondre_brreg_data
ENHETER_PREFIX           Prefix for enheter snapshots                  enheter/parsed/v1/state
======================== ============================================= =================
"""

import os
import sys
from datetime import date

from reader import GCSReader
from parser import build_partner_lookup, parse_projects
from cdc import EitHeiCDC

GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "eit_hei")
RUN_MODE = os.environ.get("RUN_MODE", "daily")
SNAPSHOT_DATE = os.environ.get("SNAPSHOT_DATE", "")
ENHETER_BUCKET = os.environ.get("ENHETER_BUCKET", "sondre_brreg_data")
ENHETER_PREFIX = os.environ.get("ENHETER_PREFIX", "enheter/parsed/v1/state")


def load_enheter_lookup(bucket_name, prefix):
    from google.cloud import storage as gcs_lib
    import pyarrow.parquet as pq
    import io

    client = gcs_lib.Client()
    bucket = client.bucket(bucket_name)

    dates = []
    iterator = bucket.list_blobs(prefix=f"{prefix}/", delimiter="/")
    for page in iterator.pages:
        for blob in page:
            name = blob.name.split("/")[-1]
            if name.endswith(".parquet"):
                dates.append(name.replace(".parquet", ""))
    dates.sort()

    if not dates:
        print("  No enheter snapshots found, name-matching limited to known institutions", flush=True)
        return {}

    latest = dates[-1]
    path = f"{prefix}/{latest}.parquet"
    print(f"  Loading enheter snapshot: {path}", flush=True)

    blob = bucket.blob(path)
    data = blob.download_as_bytes()
    table = pq.read_table(io.BytesIO(data), columns=["org_nr", "name"])

    lookup = {}
    org_nrs = table.column("org_nr").to_pylist()
    names = table.column("name").to_pylist()
    for orgnr, name in zip(org_nrs, names):
        if orgnr and name:
            lookup[name.upper().strip()] = str(orgnr).strip()

    print(f"  Enheter lookup: {len(lookup):,} entries", flush=True)
    return lookup


def main():
    print(f"{'='*60}", flush=True)
    print(f"  eit-hei-parser — mode: {RUN_MODE}", flush=True)
    print(f"  {date.today().isoformat()}", flush=True)
    print(f"  GCS: gs://{GCS_BUCKET}/{GCS_PREFIX}/", flush=True)
    print(f"{'='*60}", flush=True)

    reader = GCSReader(GCS_BUCKET, GCS_PREFIX)

    snapshot_dates = reader.list_snapshot_dates()
    if not snapshot_dates:
        print("  No snapshots found. Run eit-hei-collector first.", flush=True)
        sys.exit(1)

    snapshot = SNAPSHOT_DATE if SNAPSHOT_DATE else snapshot_dates[-1]
    print(f"  Using snapshot: {snapshot}", flush=True)

    if RUN_MODE == "check":
        manifest = reader.read_manifest(snapshot)
        if manifest:
            print(f"    Projects: {manifest.get('projects')}", flush=True)
            print(f"    Taxonomies: {manifest.get('taxonomies')}", flush=True)
        return

    projects = reader.read_projects(snapshot)
    partner_terms = reader.read_partner_institutions(snapshot)

    print(f"  Loaded: {len(projects)} projects, {len(partner_terms)} partner terms", flush=True)

    partner_lookup = build_partner_lookup(partner_terms)

    enheter_lookup = load_enheter_lookup(ENHETER_BUCKET, ENHETER_PREFIX)

    resolved, unresolved = parse_projects(projects, partner_lookup, enheter_lookup)
    print(f"  Resolved: {len(resolved)} participations, {len(unresolved)} unresolved", flush=True)

    cdc = EitHeiCDC(GCS_BUCKET, GCS_PREFIX)
    run_mode = "bootstrap" if RUN_MODE == "bootstrap" else "daily"
    stats = cdc.run(resolved, unresolved, date.today().isoformat(), run_mode=run_mode)

    print(f"\n  CDC: new={stats['new']}, modified={stats['modified']}, "
          f"changelog_rows={stats['changelog_rows']}, pool={stats['pool_size']}, "
          f"unresolved={stats['unresolved']}", flush=True)


if __name__ == "__main__":
    main()
