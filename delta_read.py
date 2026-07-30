import datetime
import io
import orjson
import re

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import write_deltalake, DeltaTable

from azure.storage.blob import BlobServiceClient
from azure.functions import Blueprint, AuthLevel, HttpRequest, HttpResponse, TimerRequest
from azure.identity import DefaultAzureCredential

bp_delta = Blueprint()

# comment

_STORAGE_SCOPE = "https://storage.azure.com/.default"
_CREDENTIAL = DefaultAzureCredential()
_CONTAINER_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
_ARROW_SCHEMA_WITH_STUDY = pa.schema([
    pa.field("ts",    pa.float64(), nullable=False),
    pa.field("tz",    pa.string()),
    pa.field("pid",   pa.string(),  nullable=False),
    pa.field("did",   pa.string()),
    pa.field("study", pa.string(),  nullable=False),
    pa.field("type",  pa.string(),  nullable=False),
    pa.field("data",  pa.string()),
])
_ARROW_SCHEMA_SANS_STUDY = pa.schema([
    pa.field("ts",    pa.float64(), nullable=False),
    pa.field("tz",    pa.string()),
    pa.field("pid",   pa.string(),  nullable=False),
    pa.field("did",   pa.string()),
    pa.field("type",  pa.string(),  nullable=False),
    pa.field("data",  pa.string()),
])

def _is_valid_container_name(study):
    return study in {"mtm-t2"}

@bp_delta.route(route="delta_read2", auth_level=AuthLevel.FUNCTION, methods=["GET"])
def delta_read2(req: HttpRequest):
    pid = req.headers.get("x-user-id")
    study = req.params.get("study")
    tipe = req.params.get("type")
    did = req.params.get("did")
    ts_start = req.params.get("ts_start")
    ts_end = req.params.get("ts_end")
    days = req.params.get("days", "1")

    if not all([pid, study, tipe]):
        return HttpResponse("Missing required params: pid, study, or type", status_code=400)

    if not _is_valid_container_name(study):
        return HttpResponse(f"Invalid study given: '{study}'", status_code=400)

    try:
        ts_end = float(ts_end) if ts_end else datetime.datetime.now(datetime.timezone.utc).timestamp()
        ts_start = float(ts_start) if ts_start else ts_end - float(days) * 86400
    except ValueError:
        return HttpResponse("Invalid time params. Use days=N or ts_start/ts_end as unix timestamps.", status_code=400)

    try:

        account_name = "trailsoutputs"
        storage_options = {"ACCOUNT_NAME": account_name, "BEARER_TOKEN": _CREDENTIAL.get_token(_STORAGE_SCOPE).token}
        dataset = DeltaTable(f"abfs://{study}/datums", storage_options=storage_options).to_pyarrow_dataset()

        condition = (
            (pc.field("pid")  == pid     ) &
            (pc.field("type") == tipe    ) &
            (pc.field("ts")   >  ts_start) &
            (pc.field("ts")   <= ts_end  )
        )

        if did:
           condition = condition & (pc.field("did") == did)

        table = dataset.scanner(columns=["ts", "data"], filter=condition).to_table()
        return HttpResponse(orjson.dumps(table.to_pylist()), status_code=200, mimetype="application/json")

    except Exception:
        return HttpResponse(status_code=500)