import functions_framework
import logging
import os
import re
import traceback
import yaml

from google.cloud import bigquery

# -------------------------
# Config
# -------------------------
BQ_DATASET = os.getenv("BQ_DATASET", "staging")
BQ_LOCATION = os.getenv("BQ_LOCATION", "us-central1")

with open("schemas.yaml", "r") as f:
    cfg = yaml.safe_load(f)

TABLES = cfg.get("tables", [])

BQ = bigquery.Client()



def create_schema_from_yaml(table_schema):
    schema = []
    for col in table_schema:
        fields = None
        if col.get("type") == "RECORD" and col.get("fields"):
            fields = create_schema_from_yaml(col["fields"])

        schema.append(
            bigquery.SchemaField(
                col["name"],
                col["type"],
                mode=col.get("mode", "NULLABLE"),
                fields=fields,
            )
        )
    return schema


def _pick_table_from_filename(filename: str):
    """
    Example filenames:
      users_1.jsonl     -> users
      events-2026.jsonl -> events
    """
    base = os.path.basename(filename).lower()
    m = re.match(r"([a-z0-9_]+)[-_].*\.(jsonl|ndjson|json)$", base)
    if not m:
        return None
    return m.group(1)


def _get_table_config(table_name: str):
    for t in TABLES:
        if t.get("name") == table_name:
            return t
    return None


def _check_if_table_exists(table_name: str, table_schema_yaml):
    table_ref = bigquery.DatasetReference(BQ.project, BQ_DATASET).table(table_name)

    try:
        BQ.get_table(table_ref)
        return
    except Exception:
        logging.warning(f"Creating table: {table_name}")
        schema = create_schema_from_yaml(table_schema_yaml)
        table = bigquery.Table(table_ref, schema=schema)
        created = BQ.create_table(table)
        print(f"Created table {created.project}.{created.dataset_id}.{created.table_id}")


def _load_table_from_uri(bucket_name: str, file_name: str, table_schema_yaml, table_name: str):
    uri = f"gs://{bucket_name}/{file_name}"
    table_ref = bigquery.DatasetReference(BQ.project, BQ_DATASET).table(table_name)

    schema = create_schema_from_yaml(table_schema_yaml)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = BQ.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
        location=BQ_LOCATION,
    )

    load_job.result()
    print(f"Loaded {uri} into {BQ.project}.{BQ_DATASET}.{table_name}")


def streaming(bucket: str, name: str):
    """
    Main ETL logic: choose table by filename, create table if needed,
    then load JSONL into BigQuery.
    """
    print("Bucket:", bucket)
    print("File:", name)

    try:
        table_key = _pick_table_from_filename(name)
        if not table_key:
            print(f"Skipping: filename does not match pattern: {name}")
            return

        table_cfg = _get_table_config(table_key)
        if not table_cfg:
            print(f"Skipping: no schema config for table key '{table_key}'")
            return

        fmt = table_cfg.get("format", "NEWLINE_DELIMITED_JSON")
        if fmt != "NEWLINE_DELIMITED_JSON":
            print(f"Skipping: unsupported format {fmt} for {table_key}")
            return

        schema_yaml = table_cfg["schema"]
        _check_if_table_exists(table_key, schema_yaml)
        _load_table_from_uri(bucket, name, schema_yaml, table_key)

    except Exception:
        print("Error streaming file. Cause:\n%s" % traceback.format_exc())


def _extract_from_audit_log(event_json: dict):
    """
    For Eventarc delivering Audit Log based events (storage.objects.create),
    bucket/object are usually in protoPayload.resourceName
    """
    proto = event_json.get("protoPayload", {}) or {}
    rn = proto.get("resourceName", "") or ""

    if "/buckets/" in rn and "/objects/" in rn:
        bucket = rn.split("/buckets/")[1].split("/objects/")[0]
        obj = rn.split("/objects/")[1]
        return bucket, obj


    resource = event_json.get("resource", {}) or {}
    labels = resource.get("labels", {}) or {}
    bucket = labels.get("bucket_name")

    return bucket, None



@functions_framework.http
def hello_auditlog(request):
    event_json = request.get_json(silent=True) or {}

    # Try audit log extraction
    bucket, obj = _extract_from_audit_log(event_json)

    # Fallback: sometimes Eventarc wraps cloudEvent under "data"
    if not obj:
        data = event_json.get("data")
        if isinstance(data, dict):
            bucket = data.get("bucket") or bucket
            obj = data.get("name")

    if not bucket or not obj:
        print("Could not extract bucket/object. Payload:")
        print(event_json)
        return ("Bad event payload", 400)

    streaming(bucket, obj)
    return ("OK", 200)



@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data
    bucket = data.get("bucket")
    name = data.get("name")
    streaming(bucket, name)