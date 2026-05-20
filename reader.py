"""GCS reader for the EIT-HEI parser.

Reads JSONL snapshots from ``raw/{snapshot_date}/``.
"""

import json

from google.cloud import storage as gcs_lib


class GCSReader:

    def __init__(self, bucket_name, prefix="eit_hei"):
        self._client = gcs_lib.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.rstrip("/")

    def list_snapshot_dates(self):
        prefix = f"{self._prefix}/raw/"
        dates = set()
        iterator = self._bucket.list_blobs(prefix=prefix, delimiter="/")
        for page in iterator.pages:
            for p in page.prefixes:
                date_part = p.rstrip("/").split("/")[-1]
                if len(date_part) == 10:
                    dates.add(date_part)
        return sorted(dates)

    def read_jsonl(self, snapshot_date, filename):
        path = f"{self._prefix}/raw/{snapshot_date}/{filename}"
        blob = self._bucket.blob(path)
        if not blob.exists():
            return []
        text = blob.download_as_text(encoding="utf-8")
        return [json.loads(line) for line in text.strip().split("\n") if line.strip()]

    def read_projects(self, snapshot_date):
        return self.read_jsonl(snapshot_date, "projects.jsonl")

    def read_partner_institutions(self, snapshot_date):
        return self.read_jsonl(snapshot_date, "partner_institution.jsonl")

    def read_manifest(self, snapshot_date):
        path = f"{self._prefix}/raw/{snapshot_date}/manifest.json"
        blob = self._bucket.blob(path)
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())
