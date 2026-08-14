import datetime
import functools
import logging
import operator
import orjson
import io

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import csv as pacsv
import time
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


def load_study(study) -> DeltaTable | HttpResponse:
    table_uri = f"abfs://{study}/datums"
    account_name = "trailsoutputs"

    if not _is_valid_container_name(study):
        return HttpResponse(f"Invalid study given: '{study}'", status_code=400)

    try:
        storage_options = {
            "ACCOUNT_NAME": account_name,
            "BEARER_TOKEN": _CREDENTIAL.get_token(_STORAGE_SCOPE).token,
        }
        dt = DeltaTable(table_uri, storage_options=storage_options)

    except ClientAuthenticationError:
        logging.exception(
            "Could not acquire a storage token for account '%s'. Check that the function app has a "
            "managed identity enabled and that AZURE_CLIENT_ID is set if it is user-assigned.",
            account_name,
        )
        return HttpResponse(status_code=500)
    except TableNotFoundError:
        logging.warning("No delta table at %s (account '%s')", table_uri, account_name)
        return HttpResponse(f"No data for study '{study}'", status_code=404)
    except Exception as e:
        # Auth/permission failures from the storage account surface here as OSError,
        # so log the detail; the identity most likely lacks Storage Blob Data Reader.
        logging.exception("Failed to open %s on account '%s'", table_uri, account_name)
        return HttpResponse(str(e), status_code=500)

    return dt


@bp_delta.route(route="delta_read", auth_level=AuthLevel.FUNCTION, methods=["GET"])
def delta_read(req: HttpRequest):
    study = req.params.get("study")
    tipe = req.params.get("type")
    pid = req.headers.get("x-user-id")
    did = req.params.get("did")
    ts_start = req.params.get("ts_start")
    ts_end = req.params.get("ts_end")
    days = req.params.get("days")

    if not all([study, tipe, days]) and not all([study, tipe, ts_start, ts_end]):
        return HttpResponse(
            "Missing required params: study, type, or time", status_code=400
        )

    study_response = load_study(study)
    if isinstance(study_response, HttpResponse):
        return study_response

    dataset = study_response.to_pyarrow_dataset()

    if days:
        today = datetime.datetime.now().astimezone()
        today = datetime.datetime(
            year=today.year, month=today.month, day=today.day
        ) + datetime.timedelta(days=1)
        start = today - datetime.timedelta(days=days + 1)  # type: ignore
        start = datetime.datetime(year=start.year, month=start.month, day=start.day)
        ts_start = start.timestamp()

    else:
        ts_start = float(ts_start)
        ts_end = float(ts_end)

    try:
        filters = [
            (pc.field("type") == tipe)
            & (pc.field("ts") >= ts_start)
            & (pc.field("ts") <= ts_end)
        ]

        optional_filters = (
            (pid, (pc.field("pid") == pid)),
            (did, (pc.field("did") == did)),
        )

        filters.extend(
            expression for value, expression in optional_filters if value is not None
        )
        condition = functools.reduce(operator.and_, filters)

        table = dataset.scanner(
            columns=["ts", "pid", "data"], filter=condition
        ).to_table()
        return HttpResponse(
            orjson.dumps(table.to_pylist()),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        return HttpResponse(str(e), status_code=500)


@bp_delta.route(route="download-data", auth_level=AuthLevel.FUNCTION, methods=["GET"])
def download_data(req: HttpRequest):
    study = req.params.get("study")

    if not study:
        return HttpResponse("Missing required params: study", status_code=400)

    study_response = load_study(study)
    if isinstance(study_response, HttpResponse):
        return study_response

    dataset = study_response.to_pyarrow_table()

    buffer = io.BytesIO()
    pacsv.write_csv(dataset, buffer)

    return HttpResponse(
        body=buffer.getvalue(),
        status_code=200,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment ; filename="{study}_data_{time.strftime("%Y-%m-%d")}.csv"'
        },
    )


@bp_delta.route(
    route="get/flows-per-day", auth_level=AuthLevel.FUNCTION, methods=["GET"]
)
def flows_per_day(req: HttpRequest):
    study = req.params.get("study")
    days = req.params.get("days")

    if not all([study, days]):
        return HttpResponse("Missing required params: study", status_code=400)

    study_response = load_study(study)
    if isinstance(study_response, HttpResponse):
        return study_response

    days = int(days)  # type: ignore
    dataset = study_response.to_pyarrow_dataset()

    flows_per_day = {}

    today = datetime.datetime.now().astimezone()
    offset = today.utcoffset().total_seconds() if today.utcoffset() else 0  # type: ignore
    today = datetime.datetime(
        year=today.year, month=today.month, day=today.day
    ) + datetime.timedelta(days=1)
    for i in range(1, days + 1):
        date = datetime.datetime.strftime(today - datetime.timedelta(days=i), "%m-%d")
        flows_per_day[date] = 0
    start = today - datetime.timedelta(days=days + 1)  # type: ignore
    start = datetime.datetime(year=start.year, month=start.month, day=start.day)
    ts_start = start.timestamp()

    tbl = dataset.to_table(
        columns={
            "day": pc.strftime(
                (pc.field("ts") + offset)
                .cast(pa.int64(), safe=False)
                .cast(pa.timestamp("s"))
                .cast(pa.date32()),
                "%m-%d",
            ),
            "data": pc.field("data"),
        },
        filter=(pc.field("ts") >= ts_start) & (pc.field("type") == "Flow"),
    )
    tbl = tbl.filter(pc.match_substring(tbl["data"], '"value":"Completion"'))
    counts = tbl.group_by("day").aggregate([("day", "count")])

    days_col, counts_col = counts.column("day"), counts.column("day_count")

    for i in range(len(days_col)):
        date = str(days_col[i])
        if date in flows_per_day:
            flows_per_day[date] = int(counts_col[i])

    return HttpResponse(
        orjson.dumps(flows_per_day), status_code=200, mimetype="application/json"
    )


@bp_delta.route(
    route="get/total-of-type", auth_level=AuthLevel.FUNCTION, methods=["GET"]
)
def total_of_type(req: HttpRequest):
    study = req.params.get("study")
    days = req.params.get("days")
    tipe = req.params.get("type")

    if not all([study, days, tipe]):
        return HttpResponse(
            "Missing at least one required param: study, days, type", status_code=400
        )

    study_response = load_study(study)
    if isinstance(study_response, HttpResponse):
        return study_response

    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    start = now - datetime.timedelta(days=int(days))  # type: ignore
    start = datetime.datetime(year=start.year, month=start.month, day=start.day)
    ts_start = start.timestamp()

    dataset = study_response.to_pyarrow_dataset()

    # adding a special type here to avoid writing a whole new function for completed flows
    if tipe == "CompletedFlow":
        subtable = dataset.to_table(
            columns=["ts", "data"],
            filter=(pc.field("ts") >= ts_start) & (pc.field("type") == "Flow"),
        )

        condition = pc.match_substring(pc.field("data"), pattern='"value":"Completion"')

        return HttpResponse(
            body=str(subtable.filter(condition).num_rows),
            status_code=200,
            mimetype="text/plain",
        )

    else:
        subtable = dataset.to_table(
            columns=["ts", "data"],
            filter=(pc.field("ts") >= ts_start) & (pc.field("type") == tipe),
        )
        return HttpResponse(
            body=str(subtable.num_rows), status_code=200, mimetype="text/plain"
        )


@bp_delta.route(
    route="get/active-participants", auth_level=AuthLevel.FUNCTION, methods=["GET"]
)
def get_active_participants(req: HttpRequest):
    # an active participant is a uid that has any flow data in the time specified
    study = req.params.get("study")
    days = req.params.get("days")

    if not all([study, days]):
        return HttpResponse(
            "Missing at least one required param: study or days", status_code=400
        )

    study_response = load_study(study)
    if isinstance(study_response, HttpResponse):
        return study_response

    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    start = now - datetime.timedelta(days=int(days))  # type: ignore
    start = datetime.datetime(year=start.year, month=start.month, day=start.day)
    ts_start = start.timestamp()

    dataset = study_response.to_pyarrow_dataset()

    subtable = dataset.to_table(
        columns=["pid"],
        filter=(pc.field("ts") >= ts_start) & (pc.field("type") == "Flow"),
    )

    total = int(pc.count_distinct(subtable["pid"]))

    return HttpResponse(body=str(total), status_code=200, mimetype="text/plain")


@bp_delta.route(
    route="get/sessions-per-day-by-participant",
    auth_level=AuthLevel.FUNCTION,
    methods=["GET"],
)
def sessions_per_day_by_participant(req: HttpRequest):
    # This endpoint will not return data for any nonexistent pids that are requested,
    # but will return all 0s if a pid has not completed any sessions in the requested time frame

    study = req.params.get("study")
    days = req.params.get("days")
    url_pids = req.params.get("pids")

    if not all([study, days]):
        return HttpResponse(
            "Missing at least one required param: study or days", status_code=400
        )

    days = int(days)  # type: ignore

    study_response = load_study(study)
    if isinstance(study_response, HttpResponse):
        return study_response

    dataset = study_response.to_pyarrow_dataset()

    pid_tbl = dataset.to_table(columns=["pid"])
    valid_pids = {pid.as_py() for pid in pc.unique(pid_tbl["pid"])}

    pids = None
    if url_pids:
        pids = [
            pid
            for pid in url_pids.replace("%2C", ",").replace("%2c", ",").split(",")
            if pid in valid_pids
        ]
        if len(pids) == 0:
            # all pids entered were invalid, return empty response
            return HttpResponse(
                orjson.dumps({}), status_code=200, mimetype="application/json"
            )

    today = datetime.datetime.now().astimezone()
    offset = today.utcoffset().total_seconds() if today.utcoffset() else 0  # type: ignore
    today = datetime.datetime(
        year=today.year, month=today.month, day=today.day
    ) + datetime.timedelta(days=1)
    dates = [
        datetime.datetime.strftime(today - datetime.timedelta(days=i), "%m-%d")
        for i in range(1, days + 1)
    ]
    start = today - datetime.timedelta(days=days + 1)
    start = datetime.datetime(year=start.year, month=start.month, day=start.day)
    ts_start = start.timestamp()

    condition = (pc.field("ts") >= ts_start) & (pc.field("type") == "Flow")
    if pids:
        condition = condition & pc.field("pid").isin(pids)

    tbl = dataset.to_table(
        columns={
            "pid": pc.field("pid"),
            "day": pc.strftime(
                (pc.field("ts") + offset)
                .cast(pa.int64(), safe=False)
                .cast(pa.timestamp("s"))
                .cast(pa.date32()),
                "%m-%d",
            ),
            "data": pc.field("data"),
        },
        filter=condition,
    )

    if pids:
        participant_ids = pids
    else:
        participant_ids = [pid.as_py() for pid in pc.unique(tbl["pid"])]

    sessions_per_day = {pid: {date: 0 for date in dates} for pid in participant_ids}

    tbl = tbl.filter(pc.match_substring(tbl["data"], '"value":"Completion"'))
    counts = tbl.group_by(["pid", "day"]).aggregate([("day", "count")])

    pids_col = counts.column("pid")
    days_col = counts.column("day")
    counts_col = counts.column("day_count")

    for i in range(len(pids_col)):
        pid = pids_col[i].as_py()
        date = days_col[i].as_py()
        if pid in sessions_per_day and date in sessions_per_day[pid]:
            sessions_per_day[pid][date] = counts_col[i].as_py()

    return HttpResponse(
        orjson.dumps(sessions_per_day), status_code=200, mimetype="application/json"
    )


app = FunctionApp()
app.register_blueprint(bp_delta)
