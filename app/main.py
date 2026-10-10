"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this file:
  1. Config and database connection
  2. Time helpers (epoch ms <-> datetime, IST, R1)
  3. Business-rule helpers (R2 - R5) and rounding
  4. Request models and validation
  5. Serialisation (MongoDB document -> API response)
  6. Endpoints: system, employees, attendance
  7. Analytics pipelines (each built by a function, so the explain endpoint can reuse it)
  8. Analytics endpoints and the explain endpoint
"""
import calendar
import json
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import Decimal128, json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.exceptions import RequestValidationError
from pydantic import AfterValidator, BaseModel, Field, StrictInt, StringConstraints, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# --------------------------------------------------------------------------- #
# 1. Config and database connection
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # real environment variables take priority over .env

client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,  # datetimes come back as aware UTC, never naive
    tzinfo=timezone.utc,
    serverSelectionTimeoutMS=3000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]
NON_PRESENCE = ["ABSENT", "LEAVE"]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]


def create_indexes() -> None:
    """Idempotent: create_index is a no-op when the same index already exists."""
    # emp_code is the business key. Unique index makes concurrent duplicate creates fail with 409.
    employees.create_index([("emp_code", ASCENDING)], unique=True, name="uniq_emp_code")
    # GET /employees?department=..  (filter + sort by emp_code), trend (department lookup).
    employees.create_index([("department", ASCENDING), ("emp_code", ASCENDING)], name="department_emp_code")
    # Department summary: employees who joined on/before the end of the month.
    employees.create_index([("joined_on", ASCENDING)], name="joined_on")

    # One record per employee per day. Makes punch-in race-safe; serves PATCH, monthly and $lookup by emp_code+date.
    logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uniq_emp_code_date")
    # GET /attendance default sort (date desc, emp_code asc) and month/day range scans for leaderboard.
    logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_desc_emp_code")
    # GET /attendance?status=.. keeps the same sort.
    logs.create_index([("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], name="status_date_emp_code")
    # Punch-out: latest record of an employee with punch_in <= punched_at.
    logs.create_index([("emp_code", ASCENDING), ("punch_in", DESCENDING)], name="emp_code_punch_in")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    create_indexes()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 2. Time helpers
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: treat naive values as UTC
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def now_utc() -> datetime:
    """Current instant at millisecond precision (what BSON can store)."""
    n = datetime.now(UTC)
    return n.replace(microsecond=n.microsecond // 1000 * 1000)


def parse_hhmm(value: str) -> tuple[int, int]:
    h, m = value.split(":")
    return int(h), int(m)


def is_overnight(emp: dict) -> bool:
    # "HH:MM" strings compare correctly as text.
    return emp["shift_end"] <= emp["shift_start"]


def at_ist(day: date, hhmm: str) -> datetime:
    h, m = parse_hhmm(hhmm)
    return datetime(day.year, day.month, day.day, h, m, tzinfo=IST)


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: IST calendar date of punch_in; for overnight shifts, a punch before shift_end belongs to the previous day."""
    local = punch_in.astimezone(IST)
    if is_overnight(emp):
        h, m = parse_hhmm(emp["shift_end"])
        if (local.hour, local.minute, local.second) < (h, m, 0):
            return local.date() - timedelta(days=1)
    return local.date()


def shift_end_dt(day: date, emp: dict) -> datetime:
    """Shift end for the shift that started on `day` (next calendar day for overnight shifts)."""
    end = at_ist(day, emp["shift_end"])
    return end + timedelta(days=1) if is_overnight(emp) else end


# --------------------------------------------------------------------------- #
# 3. Business rules and rounding
# --------------------------------------------------------------------------- #
def round_half_up(value, places: int) -> float:
    q = Decimal(1).scaleb(-places)
    return float(Decimal(str(value)).quantize(q, rounding=ROUND_HALF_UP))


def compute_late_minutes(punch_in: datetime, day: date, emp: dict) -> int:
    """R2: late only if strictly more than 10:00 after shift_start; then floor minutes since shift_start."""
    seconds = int((punch_in - at_ist(day, emp["shift_start"])).total_seconds())
    return seconds // 60 if seconds > 600 else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals half-up, using exact decimal arithmetic."""
    seconds = int((punch_out - punch_in).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out: datetime, day: date, emp: dict) -> int:
    """R3: floor minutes after shift end, only when >= 30."""
    seconds = int((punch_out - shift_end_dt(day, emp)).total_seconds())
    minutes = seconds // 60 if seconds > 0 else 0
    return minutes if minutes >= 30 else 0


def derived_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], day: date, emp: dict) -> dict:
    """The four derived fields for a record, per the table in DATA_MODEL.md."""
    if status in NON_PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    out = {"work_hours": None, "late_minutes": compute_late_minutes(punch_in, day, emp), "overtime_minutes": 0, "half_day": False}
    if punch_out is not None:
        wh = compute_work_hours(punch_in, punch_out)
        out.update(work_hours=wh, overtime_minutes=compute_overtime(punch_out, day, emp), half_day=wh < 4.5)
    return out


# --------------------------------------------------------------------------- #
# 4. Request models and validation
# --------------------------------------------------------------------------- #
def _valid_date(v: str) -> str:
    date.fromisoformat(v)  # raises ValueError -> 422 for e.g. 2026-02-30
    return v


DateStr = Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}$"), AfterValidator(_valid_date)]
MonthStr = Annotated[str, StringConstraints(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")]
HHMM = Annotated[str, StringConstraints(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")]
# Strict: rejects floats, strings and booleans. Range rejects second-looking values.
EpochMillis = Annotated[StrictInt, Field(ge=100_000_000_000, le=4_102_444_800_000)]


def validation_error(loc: list, msg: str, value: Any = None):
    """Raise a 422 with FastAPI's default validation body."""
    raise RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": value}])


class EmployeeIn(BaseModel):
    emp_code: Annotated[str, StringConstraints(pattern=r"^EMP\d{4,6}$")]
    name: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    email: Annotated[str, StringConstraints(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, StringConstraints(min_length=1, max_length=50)]
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: DateStr

    @model_validator(mode="after")
    def shifts_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None


class RegularizeIn(BaseModel):
    # Not Optional: an explicit null is a 422; omitted means unchanged.
    status: Status = None
    punch_in: EpochMillis = None
    punch_out: EpochMillis = None
    reason: Annotated[str, StringConstraints(min_length=5, max_length=200)]
    regularized_by: Annotated[str, StringConstraints(min_length=1, max_length=50)]


Page = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]


# --------------------------------------------------------------------------- #
# 5. Serialisation
# --------------------------------------------------------------------------- #
TIME_FIELDS = ("punch_in", "punch_out")


def serialize_employee(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def serialize_change(field: str, change: dict) -> dict:
    if field in TIME_FIELDS:
        return {"from": dt_to_ms(change.get("from")), "to": dt_to_ms(change.get("to"))}
    return {"from": change.get("from"), "to": change.get("to")}


def serialize_record(doc: dict) -> dict:
    """Fills legacy gaps (missing history / half_day / derived ints) and converts instants to epoch ms. No _id."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [
            {
                "at": dt_to_ms(h.get("at")),
                "by": h.get("by"),
                "reason": h.get("reason"),
                "changes": {k: serialize_change(k, v) for k, v in (h.get("changes") or {}).items()},
            }
            for h in (doc.get("history") or [])
        ],
    }


def get_employee_or_404(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 6. Endpoints: system, employees, attendance
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        employees.insert_one(doc)  # unique index decides; no check-then-insert race
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return serialize_employee(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: Page = 1, page_size: PageSize = 20):
    q = {"department": department} if department is not None else {}
    total = employees.count_documents(q)
    cursor = employees.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [serialize_employee(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc().replace(microsecond=0)
    day = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(ts, day, emp),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        logs.insert_one(doc)  # unique (emp_code, date): exactly one concurrent insert wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return serialize_record(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc().replace(microsecond=0)
    rec = logs.find_one({"emp_code": body.emp_code, "punch_in": {"$lte": ts}}, sort=[("punch_in", DESCENDING)])
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    p_in = rec["punch_in"].replace(microsecond=0)
    if ts <= p_in:
        validation_error(["body", "punched_at"], "punched_at must be after punch_in", body.punched_at)
    if ts - p_in > timedelta(hours=24):
        validation_error(["body", "punched_at"], "punched_at is more than 24 hours after punch_in", body.punched_at)

    derived = derived_fields(rec["status"], p_in, ts, date.fromisoformat(rec["date"]), emp)
    # Conditional update: only the request that still sees punch_out == null wins.
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None},
        {"$set": {"punch_out": ts, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return serialize_record(updated)


def attendance_query(emp_code, date_from, date_to, status) -> dict:
    if date_from and date_to and date_from > date_to:
        validation_error(["query", "date_from"], "date_from must be on or before date_to", date_from)
    q: dict = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to
    if status is not None:
        q["status"] = status
    return q


ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status: Optional[Status] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    q = attendance_query(emp_code, date_from, date_to, status)
    total = logs.count_documents(q)
    cursor = logs.find(q).sort(ATTENDANCE_SORT).skip((page - 1) * page_size).limit(page_size)
    return {"items": [serialize_record(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: Annotated[DateStr, Path()], body: RegularizeIn):
    emp = get_employee_or_404(emp_code)
    rec = logs.find_one({"emp_code": emp_code, "date": date})
    if rec is None:
        raise HTTPException(404, "no attendance record for this date")
    day = datetime.fromisoformat(date).date()  # `date` here is the path parameter, not the class
    sent = body.model_fields_set

    old = serialize_record(rec)  # normalised old values (epoch ms, defaults filled)
    status = body.status if "status" in sent else rec["status"]
    if status in NON_PRESENCE:
        if "punch_in" in sent or "punch_out" in sent:
            validation_error(["body"], f"{status} clears both punch times; do not send punch_in or punch_out")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if "punch_in" in sent else rec.get("punch_in")
        new_out = ms_to_dt(body.punch_out) if "punch_out" in sent else rec.get("punch_out")
        if new_in is None:
            validation_error(["body", "punch_in"], "a presence status requires punch_in")
        if attendance_date(new_in, emp) != day:
            validation_error(["body", "punch_in"], "punch_in must stay on the record's attendance date", body.punch_in)
        if new_out is not None:
            if new_out <= new_in:
                validation_error(["body", "punch_out"], "punch_out must be after punch_in", body.punch_out)
            if new_out - new_in > timedelta(hours=24):
                validation_error(["body", "punch_out"], "punch_out must be within 24 hours of punch_in", body.punch_out)

    new = {"status": status, "punch_in": new_in, "punch_out": new_out, **derived_fields(status, new_in, new_out, day, emp)}
    changes = {}
    for field, value in new.items():
        before = old[field]
        after = dt_to_ms(value) if field in TIME_FIELDS else value
        if before != after:
            changes[field] = {"from": rec.get(field) if field in TIME_FIELDS else before, "to": value}
    if not changes:
        validation_error(["body"], "the request changes nothing")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    # Optimistic concurrency: the update only applies if nobody changed the record since we read it.
    guard = {
        "_id": rec["_id"],
        "status": rec["status"],
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(rec.get("history") or [])]},
    }
    updated = logs.find_one_and_update(
        guard, {"$set": new, "$push": {"history": entry}}, return_document=ReturnDocument.AFTER
    )
    if updated is None:
        raise HTTPException(409, "record was modified concurrently, retry")
    return serialize_record(updated)



# --------------------------------------------------------------------------- #
# 7. Analytics pipelines
# --------------------------------------------------------------------------- #
def month_bounds(month: str) -> tuple[str, str]:
    y, m = map(int, month.split("-"))
    return f"{month}-01", f"{month}-{calendar.monthrange(y, m)[1]:02d}"


def working_days(start: date, end: date) -> int:
    """Mon-Fri days in [start, end], 0 if empty."""
    if start > end:
        return 0
    days = (end - start).days + 1
    full_weeks, rest = divmod(days, 7)
    return full_weeks * 5 + sum(1 for i in range(rest) if (start + timedelta(days=full_weeks * 7 + i)).weekday() < 5)


def mround(expr, places: int) -> dict:
    """MongoDB expression: round a non-negative number half-up using Decimal128 (MongoDB's $round is half-to-even)."""
    scale = 10**places
    return {
        "$toDouble": {
            "$divide": [{"$floor": {"$add": [{"$multiply": [{"$toDecimal": expr}, scale]}, Decimal128("0.5")]}}, scale]
        }
    }


def is_weekday(date_expr) -> dict:
    return {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": date_expr, "format": "%Y-%m-%d"}}}, 5]}


def presence_weight(var: str) -> dict:
    """1 for a present record, 0.5 for a present half day, 0 otherwise (R5, R6)."""
    return {
        "$cond": [
            {"$in": [f"{var}.status", PRESENCE]},
            {"$cond": [{"$ifNull": [f"{var}.half_day", False]}, 0.5, 1]},
            0,
        ]
    }


def is_late(var: str) -> dict:
    return {"$gt": [{"$ifNull": [f"{var}.late_minutes", 0]}, 0]}


def employee_monthly_pipeline(emp_code: str, month: str) -> list:
    start, end = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},
        {
            "$group": {
                "_id": None,
                "present_days": {"$sum": {"$cond": [is_weekday("$date"), presence_weight("$$ROOT"), 0]}},
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": {"$cond": [is_late("$$ROOT"), 1, 0]}},
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> list:
    start, end = month_bounds(month)
    match: dict = {"joined_on": {"$lte": end}}
    if department is not None:
        match["department"] = department

    def total(cond, value=1):
        """Sum `value` over this employee's logs `l` where `cond` holds."""
        return {"$sum": {"$map": {"input": "$logs", "as": "l", "in": {"$cond": [cond, value, 0]}}}}

    has_hours = {"$and": [{"$in": ["$$l.status", PRESENCE]}, {"$ne": [{"$ifNull": ["$$l.work_hours", None]}, None]}]}
    return [
        # Start from employees so people with zero logs still count in headcount (R9).
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": start, "$lte": end}}},
                    {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "work_hours": 1, "late_minutes": 1}},
                ],
                "as": "logs",
            }
        },
        {
            "$project": {
                "department": 1,
                "present_days": total(is_weekday("$$l.date"), presence_weight("$$l")),
                # Decimal sum so the average is exact before half-up rounding.
                "wh_sum": total(has_hours, {"$toDecimal": "$$l.work_hours"}),
                "wh_count": total(has_hours),
                "late_count": total(is_late("$$l")),
                "total_late_minutes": total(is_late("$$l"), "$$l.late_minutes"),
                "leave_count": total({"$eq": ["$$l.status", "LEAVE"]}),
                "on_duty_count": total({"$eq": ["$$l.status", "ON_DUTY"]}),
            }
        },
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": "$present_days"},
                "wh_sum": {"$sum": "$wh_sum"},
                "wh_count": {"$sum": "$wh_count"},
                "late_count": {"$sum": "$late_count"},
                "total_late_minutes": {"$sum": "$total_late_minutes"},
                "leave_count": {"$sum": "$leave_count"},
                "on_duty_count": {"$sum": "$on_duty_count"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": 1,
                "present_days": mround("$present_days", 2),
                "avg_work_hours": {
                    "$cond": [{"$gt": ["$wh_count", 0]}, mround({"$divide": ["$wh_sum", "$wh_count"]}, 2), None]
                },
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]


def late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list:
    start, end = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": start, "$lte": end}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        # Inner join: logs of unknown employees disappear here.
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        # Standard competition ranking: ties share a rank, next rank is skipped (1, 2, 2, 4).
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies after ranking, so ties at the cutoff are all kept
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return pipeline


def department_trend_pipeline(department: str, date_from: str, date_to: str) -> list:
    start = datetime.fromisoformat(date_from).replace(tzinfo=UTC)
    end_exclusive = datetime.fromisoformat(date_to).replace(tzinfo=UTC) + timedelta(days=1)
    zero = {"join": 0, "present": 0, "late": 0}
    rate = "$attendance_rate"
    return [
        {"$match": {"department": department}},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": date_from, "$lte": date_to}}},
                    {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}},
                ],
                "as": "logs",
            }
        },
        # Turn every employee into events: one "joined" event (counted from max(joined_on, from)) plus one per log.
        {
            "$project": {
                "events": {
                    "$concatArrays": [
                        [{"date": date_from, **zero}],  # anchor so the range is never empty
                        {
                            "$cond": [
                                {"$lte": ["$joined_on", date_to]},
                                [{"date": {"$max": ["$joined_on", date_from]}, "join": 1, "present": 0, "late": 0}],
                                [],
                            ]
                        },
                        {
                            "$map": {
                                "input": "$logs",
                                "as": "l",
                                "in": {
                                    "date": "$$l.date",
                                    "join": 0,
                                    "present": presence_weight("$$l"),
                                    "late": {"$cond": [is_late("$$l"), 1, 0]},
                                },
                            }
                        },
                    ]
                }
            }
        },
        {"$unwind": "$events"},
        {
            "$group": {
                "_id": {"$dateFromString": {"dateString": "$events.date", "format": "%Y-%m-%d"}},
                "joined": {"$sum": "$events.join"},
                "present_count": {"$sum": "$events.present"},
                "late_count": {"$sum": "$events.late"},
            }
        },
        # Gap filling: exactly one document per calendar day in [from, to].
        {"$densify": {"field": "_id", "range": {"step": 1, "unit": "day", "bounds": [start, end_exclusive]}}},
        {
            "$set": {
                "joined": {"$ifNull": ["$joined", 0]},
                "present_count": {"$ifNull": ["$present_count", 0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
                "is_working_day": {"$lte": [{"$isoDayOfWeek": "$_id"}, 5]},
            }
        },
        # Headcount on a day = running total of employees who joined on/before that day (R9).
        {
            "$setWindowFields": {
                "sortBy": {"_id": 1},
                "output": {"headcount": {"$sum": "$joined", "window": {"documents": ["unbounded", "current"]}}},
            }
        },
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        mround({"$divide": [{"$toDecimal": "$present_count"}, "$headcount"]}, 4),
                        None,
                    ]
                }
            }
        },
        # $avg ignores nulls and returns null when the whole window is null.
        {
            "$setWindowFields": {
                "sortBy": {"_id": 1},
                "output": {"ma": {"$avg": {"$toDecimal": rate}, "window": {"documents": [-6, "current"]}}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": {"$dateToString": {"date": "$_id", "format": "%Y-%m-%d"}},
                "is_working_day": 1,
                "headcount": 1,
                "present_count": {"$toDouble": "$present_count"},
                "late_count": 1,
                "attendance_rate": 1,
                "moving_avg_7d": {"$cond": [{"$eq": [{"$ifNull": ["$ma", None]}, None]}, None, mround("$ma", 4)]},
            }
        },
        {"$sort": {"date": 1}},
    ]


# --------------------------------------------------------------------------- #
# 8. Analytics endpoints and explain
# --------------------------------------------------------------------------- #
def trend_range(date_from: str, date_to: str) -> None:
    d1, d2 = date.fromisoformat(date_from), date.fromisoformat(date_to)
    if d2 < d1:
        validation_error(["query", "to"], "to must be on or after from", date_to)
    if (d2 - d1).days + 1 > 92:
        validation_error(["query", "to"], "range must not exceed 92 days", date_to)


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthStr):
    emp = get_employee_or_404(emp_code)
    start, end = month_bounds(month)
    wd = working_days(max(date.fromisoformat(start), date.fromisoformat(emp["joined_on"])), date.fromisoformat(end))
    rows = list(logs.aggregate(employee_monthly_pipeline(emp_code, month)))
    r = rows[0] if rows else {}
    present = r.get("present_days", 0)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": wd,
        "present_days": round_half_up(present, 2),
        "leave_days": r.get("leave_days", 0),
        "late_count": r.get("late_count", 0),
        "total_late_minutes": r.get("total_late_minutes", 0),
        "total_overtime_minutes": r.get("total_overtime_minutes", 0),
        "attendance_pct": round_half_up(Decimal(str(present)) / wd * 100, 2) if wd else None,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: MonthStr, department: Optional[str] = None):
    items = list(employees.aggregate(department_summary_pipeline(month, department)))
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: MonthStr, limit: Annotated[int, Query(ge=1, le=50)] = 10, department: Optional[str] = None):
    items = list(logs.aggregate(late_leaderboard_pipeline(month, limit, department)))
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    date_from: Annotated[DateStr, Query(alias="from")],
    date_to: Annotated[DateStr, Query(alias="to")],
):
    trend_range(date_from, date_to)
    if employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "unknown department")
    items = list(employees.aggregate(department_trend_pipeline(department, date_from, date_to)))
    return {"department": department, "items": items}


ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


def require(**params):
    for name, value in params.items():
        if value is None:
            raise RequestValidationError([{"type": "missing", "loc": ["query", name], "msg": "Field required", "input": None}])


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: Optional[MonthStr] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[DateStr], Query(alias="from")] = None,
    to: Optional[DateStr] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    """Runs explain(executionStats) on the same query/pipeline the real endpoint builds."""
    if endpoint == "attendance_list":
        coll = logs
        cmd = {
            "find": logs.name,
            "filter": attendance_query(emp_code, date_from, date_to, status),
            "sort": dict(ATTENDANCE_SORT),
            "skip": (page - 1) * page_size,
            "limit": page_size,
        }
    elif endpoint == "employee_monthly":
        require(emp_code=emp_code, month=month)
        coll, pipeline = logs, employee_monthly_pipeline(emp_code, month)
    elif endpoint == "department_summary":
        require(month=month)
        coll, pipeline = employees, department_summary_pipeline(month, department)
    elif endpoint == "late_leaderboard":
        require(month=month)
        coll, pipeline = logs, late_leaderboard_pipeline(month, limit, department)
    else:
        require(department=department, **{"from": from_}, to=to)
        trend_range(from_, to)
        coll, pipeline = employees, department_trend_pipeline(department, from_, to)
    if endpoint != "attendance_list":
        cmd = {"aggregate": coll.name, "pipeline": pipeline, "cursor": {}}
    result = db.command({"explain": cmd, "verbosity": "executionStats"})
    return {"endpoint": endpoint, "collection": coll.name, "explain": json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))}
