"""Change Data Capture layer for the EIT-HEI parser.

Compares resolved Norwegian participations against stored snapshots,
emits unified 12-column changelog.

State files on GCS::

    gs://{bucket}/{prefix}/
    ├── cdc/
    │   ├── pool.parquet
    │   ├── snapshots.parquet
    │   └── changelog/
    │       └── YYYY-MM-DD.parquet
    └── unresolved/
        └── YYYY-MM-DD.parquet

LUAS: ``(project_slug, partner_institution_id)`` — one participation
per project per partner.
"""

import io
import json
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage as gcs_lib


TRACKED_FIELDS = ["phase1_amount", "phase2_amount", "call_text", "project_strand"]

SNAPSHOT_SCHEMA = pa.schema([
    ("project_slug", pa.string()),
    ("partner_institution_id", pa.int32()),
    ("orgnr", pa.string()),
    ("partner_name", pa.string()),
    ("content_hash", pa.string()),
    ("is_lead_partner", pa.bool_()),
])

POOL_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
    ("n_participations", pa.int32()),
])

CHANGELOG_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("document_id", pa.string()),
    ("data_source", pa.string()),
    ("event_type", pa.string()),
    ("event_subtype", pa.string()),
    ("summary", pa.string()),
    ("changed_fields", pa.string()),
    ("valid_time", pa.string()),
    ("detected_time", pa.string()),
    ("details_json", pa.string()),
    ("source_run_mode", pa.string()),
    ("run_id", pa.string()),
])

UNRESOLVED_SCHEMA = pa.schema([
    ("project_slug", pa.string()),
    ("project_title", pa.string()),
    ("partner_institution_id", pa.int32()),
    ("partner_name", pa.string()),
    ("is_lead_partner", pa.bool_()),
    ("call_text", pa.string()),
])


class EitHeiCDC:

    def __init__(self, bucket_name, prefix="eit_hei"):
        self._client = gcs_lib.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.rstrip("/")

    def _gcs_path(self, *parts):
        return "/".join([self._prefix] + list(parts))

    def _read_parquet(self, path):
        blob = self._bucket.blob(path)
        if not blob.exists():
            return None
        return pq.read_table(io.BytesIO(blob.download_as_bytes()))

    def _write_parquet(self, table, path):
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd")
        buf.seek(0)
        blob = self._bucket.blob(path)
        blob.upload_from_file(buf, content_type="application/octet-stream")

    def _list_parsed_dates(self):
        prefix = self._gcs_path("parsed") + "/"
        dates = set()
        for blob in self._bucket.list_blobs(prefix=prefix):
            name = blob.name.split("/")[-1]
            if name.endswith(".parquet"):
                dates.add(name.replace(".parquet", ""))
        return sorted(dates)

    def _load_previous_parsed(self, run_date):
        dates = [d for d in self._list_parsed_dates() if d < run_date]
        if not dates:
            return {}
        t = self._read_parquet(self._gcs_path("parsed", f"{dates[-1]}.parquet"))
        if t is None:
            return {}
        d = t.to_pydict()
        result = {}
        for i in range(t.num_rows):
            key = (d["project_slug"][i], d["partner_institution_id"][i])
            result[key] = {"content_hash": d["content_hash"][i]}
            for f in TRACKED_FIELDS:
                if f in d:
                    result[key][f] = d[f][i]
        return result

    def _load_pool(self):
        t = self._read_parquet(self._gcs_path("cdc", "pool.parquet"))
        if t is None:
            return {}
        d = t.to_pydict()
        return {d["orgnr"][i]: {
            "first_seen": d["first_seen"][i],
            "last_seen": d["last_seen"][i],
            "n_participations": d["n_participations"][i],
        } for i in range(t.num_rows)}

    def run(self, resolved_rows, unresolved_rows, run_date, run_mode="daily"):
        run_id = str(uuid.uuid4())[:8]
        detected_time = datetime.now(timezone.utc).isoformat()

        old_snaps = self._load_previous_parsed(run_date)
        pool = self._load_pool()

        changelog_rows = []
        new_count = 0
        mod_count = 0
        new_snaps = {}

        for row in resolved_rows:
            key = (row["project_slug"], row["partner_institution_id"])
            h = row["content_hash"]
            snap_entry = {
                "project_slug": row["project_slug"],
                "partner_institution_id": row["partner_institution_id"],
                "orgnr": row["orgnr"],
                "partner_name": row["partner_name"],
                "content_hash": h,
                "is_lead_partner": row["is_lead_partner"],
            }
            for f in TRACKED_FIELDS:
                snap_entry[f] = str(row.get(f) or "")
            new_snaps[key] = snap_entry

            old_entry = old_snaps.get(key)

            if run_mode == "bootstrap" or old_entry is None:
                event_type = "new"
                changed_fields = None
                new_count += 1
            elif old_entry["content_hash"] != h:
                event_type = "modified"
                diffs = [f for f in TRACKED_FIELDS if str(row.get(f) or "") != str(old_entry.get(f) or "")]
                changed_fields = json.dumps(diffs) if diffs else json.dumps(["content_hash"])
                mod_count += 1
            else:
                continue

            role = "lead_partner" if row["is_lead_partner"] else "partner"
            subtype = f"eit_hei_{role}"

            summary_parts = [
                role,
                f"EIT-HEI {row['project_title']} ({row['project_slug']})",
            ]
            if row.get("phase1_amount"):
                summary_parts.append(row["phase1_amount"])
            summary = " — ".join(summary_parts)

            details = {
                "project_slug": row["project_slug"],
                "project_title": row["project_title"],
                "project_wp_id": row.get("project_wp_id"),
                "partner_institution_id": row["partner_institution_id"],
                "partner_name": row["partner_name"],
                "is_lead_partner": row["is_lead_partner"],
                "call_text": row.get("call_text"),
                "project_strand": row.get("project_strand"),
                "phase1_amount": row.get("phase1_amount"),
                "phase2_amount": row.get("phase2_amount"),
                "orgnr_resolution_method": row.get("orgnr_resolution_method"),
            }

            changelog_rows.append({
                "orgnr": row["orgnr"],
                "document_id": f"eit-hei-{row['project_slug']}-{row['partner_institution_id']}",
                "data_source": "eit_hei",
                "event_type": event_type,
                "event_subtype": subtype,
                "summary": summary,
                "changed_fields": changed_fields,
                "valid_time": row.get("project_modified", run_date)[:10] if row.get("project_modified") else run_date,
                "detected_time": detected_time,
                "details_json": json.dumps(details, ensure_ascii=False),
                "source_run_mode": run_mode,
                "run_id": run_id,
            })

            orgnr = row["orgnr"]
            if orgnr in pool:
                pool[orgnr]["last_seen"] = run_date
                pool[orgnr]["n_participations"] += 1
            else:
                pool[orgnr] = {
                    "first_seen": run_date,
                    "last_seen": run_date,
                    "n_participations": 1,
                }

        if run_mode != "bootstrap":
            for key, old_entry in old_snaps.items():
                if key not in new_snaps:
                    changelog_rows.append({
                        "orgnr": "", "document_id": f"eit-hei-{key[0]}-{key[1]}",
                        "data_source": "eit_hei", "event_type": "disappeared",
                        "event_subtype": "eit_hei_participation_ended",
                        "summary": f"Participation ended: {key[0]}",
                        "changed_fields": None, "valid_time": run_date, "detected_time": detected_time,
                        "details_json": None, "source_run_mode": run_mode, "run_id": run_id,
                    })

        if changelog_rows:
            cl_table = pa.Table.from_pylist(changelog_rows, schema=CHANGELOG_SCHEMA)
            self._write_parquet(cl_table, self._gcs_path("cdc", "changelog", f"{run_date}.parquet"))

        snap_rows = list(new_snaps.values())
        if snap_rows:
            snap_table = pa.Table.from_pylist(snap_rows, schema=SNAPSHOT_SCHEMA)
            self._write_parquet(snap_table, self._gcs_path("parsed", f"{run_date}.parquet"))

        pool_rows = [{"orgnr": k, **v} for k, v in pool.items()]
        if pool_rows:
            pool_table = pa.Table.from_pylist(pool_rows, schema=POOL_SCHEMA)
            self._write_parquet(pool_table, self._gcs_path("cdc", "pool.parquet"))

        if unresolved_rows:
            ur_rows = [{
                "project_slug": r["project_slug"],
                "project_title": r.get("project_title", ""),
                "partner_institution_id": r["partner_institution_id"],
                "partner_name": r["partner_name"],
                "is_lead_partner": r.get("is_lead_partner", False),
                "call_text": r.get("call_text", ""),
            } for r in unresolved_rows]
            ur_table = pa.Table.from_pylist(ur_rows, schema=UNRESOLVED_SCHEMA)
            self._write_parquet(ur_table, self._gcs_path("unresolved", f"{run_date}.parquet"))

        return {
            "new": new_count,
            "modified": mod_count,
            "changelog_rows": len(changelog_rows),
            "pool_size": len(pool),
            "snapshot_size": len(new_snaps),
            "unresolved": len(unresolved_rows),
        }
