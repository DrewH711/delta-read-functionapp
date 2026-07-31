import datetime
import functools
import logging
import operator
import orjson

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from azure.core.exceptions import ClientAuthenticationError
from azure.functions import Blueprint, AuthLevel, HttpRequest, HttpResponse, FunctionApp
from azure.identity import DefaultAzureCredential

bp_delta = Blueprint()

_STORAGE_SCOPE = "https://storage.azure.com/.default"
_CREDENTIAL = DefaultAzureCredential()

def _is_valid_container_name(study):
    return study in {"mtm-t2"}

@bp_delta.route(route="delta_read", auth_level=AuthLevel.FUNCTION, methods=["GET"])
def delta_read(req: HttpRequest):
    study = req.params.get("study")
    tipe = req.params.get("type")
    pid = req.headers.get("x-user-id")
    did = req.params.get("did")
    ts_start = req.params.get("ts_start")
    ts_end = req.params.get("ts_end")
    days = req.params.get("days", "1")

    if not all([study, tipe]):
        return HttpResponse("Missing required params: study, or type", status_code=400)

    if not _is_valid_container_name(study):
        return HttpResponse(f"Invalid study given: '{study}'", status_code=400)

    try:
        ts_end = float(ts_end) if ts_end else datetime.datetime.now(datetime.timezone.utc).timestamp()
        ts_start = float(ts_start) if ts_start else ts_end - float(days) * 86400

    except ValueError:
        return HttpResponse("Invalid time params. Use days=N or ts_start/ts_end as unix timestamps.", status_code=400)

    table_uri = f"abfs://{study}/datums"
    account_name = "trailsoutputs"

    try:
        storage_options = {"ACCOUNT_NAME": account_name, "BEARER_TOKEN": _CREDENTIAL.get_token(_STORAGE_SCOPE).token}
        dataset = DeltaTable(table_uri, storage_options=storage_options).to_pyarrow_dataset()

    except ClientAuthenticationError:
        logging.exception(
            "Could not acquire a storage token for account '%s'. Check that the function app has a "
            "managed identity enabled and that AZURE_CLIENT_ID is set if it is user-assigned.",
            account_name
        )
        return HttpResponse(status_code=500)
    except TableNotFoundError:
        logging.warning("No delta table at %s (account '%s')", table_uri, account_name)
        return HttpResponse(f"No data for study '{study}'", status_code=404)
    except Exception:
        # Auth/permission failures from the storage account surface here as OSError,
        # so log the detail; the identity most likely lacks Storage Blob Data Reader.
        logging.exception("Failed to open %s on account '%s'", table_uri, account_name)
        return HttpResponse(status_code=500)

    try:
        filters = [(pc.field("type") == tipe) & (pc.field("ts") >= ts_start) & (pc.field("ts") <= ts_end)]

        optional_filters = (
            (pid, (pc.field("pid") == pid)),
            (did, (pc.field("did") == did)),
        )
        
        filters.extend(expression for value, expression in optional_filters if value is not None)
        condition = functools.reduce(operator.and_, filters)

        table = dataset.scanner(columns=["ts", "data"], filter=condition).to_table()
        return HttpResponse(orjson.dumps(table.to_pylist()), status_code=200, mimetype="application/json")

    except Exception:
        return HttpResponse(status_code=500)

app = FunctionApp()
app.register_blueprint(bp_delta)