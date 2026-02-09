from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

SOURCE_NAME = "commcare_farmers_registration"
ODATA_PATH = "/a/tractrac-issam-project/api/odata/forms/v1/282066d7324b4ffdd7c0127d7b80d46d/feed"

# Your mapping: CommCare column -> bronze column
DF_TO_BRONZE = {
    "formid": "form_id",
    "form title": "title",
    "form first_name": "first_name",
    "form last_name": "last_name",
    "form date_of_birth": "date_of_birth",
    "form gender": "gender",
    "form age": "age",
    "form phone_number": "phone_number",
    "form email_address": "email_address",
    "form business_category": "business_category",
    "form location state_of_residence": "state_of_residence",
    "form location lga_of_residence": "lga_of_residence",
    "form location electoral_ward_of_residence": "electoral_ward_of_residence",
    "form location please_write_the_electoral_ward": "electoral_ward_of_residence_alt",
    "form location community": "community",
    "form location what_is_the_location_classification_of_the_community": "classification_of_community",
    "form educational_qualification highest_qualification": "highest_qualification",
    "form disability": "disability",
    "form disability_type type_of_disability": "type_of_disability",
    "form disability_type for_others_please_specify": "type_of_disability_alt",
    "form disability_classification": "disability_classification",
    "form please_list_the_languages_you_speak": "languages_you_speak",
    "form can_you_use_an_android_phone": "can_you_use_an_android_phone",
    "form do_you_have_an_android_phone": "do_you_have_an_android_phone",
    "form are_you_a_nursing_mother": "are_you_a_nursing_mother",
    "form are_you_an_nysc_member": "are_you_an_nysc_member",
    "form are_you_waiting_to_go_for_nysc": "are_you_waiting_to_go_for_nysc",
    "form what_is_your_employment_status": "employment_status",
    "form did_you_just_write_jambawaiting_admission_into_school": "just_wrote_jamb_awaiting_admission",
    "form marital_status": "marital_status",
    "form please_confirm_if_you_living_in_this_community_because_of_any_of_the_follow": "confirm_if_you_living_in_this_community_because_of_any_of_the_f",
    "form Capturing_Field_Worker_details Field_Worker_Full_Name": "fullname_field_worker",
    "form Capturing_Field_Worker_details Field_Worker_Phone_Number": "phone_number_field_worker",
    "form Age-group_category": "age_group_category",
    "form location Electoral_Ward_of_Participants": "electoral_ward_of_participants",
    "form IDP_Classification_of_Participant": "idp_classification_of_participant",
    "form duplicate_identifier": "duplicate_identifier",
    "form unique_id_of_prospective_participants": "unique_id_of_prospective_participants",
    "form case case_id": "case_id",
    "form_link": "form_link",
}

# Choose your watermark field:
WATERMARK_FIELD = "received_on"  # or "received_on"

DEFAULT_ARGS = {"owner": "tractrac", "retries": 3, "retry_delay": timedelta(minutes=3)}

def _get_commcare_session() -> requests.Session:
    conn = BaseHook.get_connection("commcare_http")
    s = requests.Session()
    if conn.login and conn.password:
        s.auth = (conn.login, conn.password)

    extra = conn.extra_dejson or {}
    token = extra.get("token")
    if token:
        hdr = extra.get("token_header", "Authorization")
        prefix = extra.get("token_prefix", "")
        s.headers[hdr] = f"{prefix}{token}"

    s.headers["Accept"] = "application/json"
    return s

def _extract_records(page: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    return (page.get("value") or []), page.get("@odata.nextLink")

def _parse_nextlink(next_link: str) -> Tuple[str, Dict[str, Any]]:
    u = urlparse(next_link)
    qs = parse_qs(u.query)
    params = {k: (v[0] if len(v) == 1 else v) for k, v in qs.items()}
    return u.path, params

def _to_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None

def _to_int(v: Any) -> Optional[int]:
    if v is None or str(v).strip() == "":
        return None
    try:
        return int(float(v))
    except Exception:
        return None

def _to_date(v: Any):
    if not v:
        return None
    return pendulum.parse(str(v)).date()

def _get_form_id(rec: Dict[str, Any]) -> str:
    # Your mapping uses "formid" -> "form_id"
    # Some feeds also have form_id directly; handle both.
    fid = rec.get("formid") or rec.get("form_id") or rec.get("_id") or rec.get("id")
    if not fid:
        raise ValueError("No form id found in record (expected formid/form_id/id/_id).")
    return str(fid)

with DAG(
    dag_id="commcare_farmers_registration_to_postgres",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/10 * * * *",  # every 10 mins (tune)
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commcare", "postgres", "bronze", "incremental"],
) as dag:

    @task
    def get_watermark() -> str:
        pg = PostgresHook("tractrac_postgres")
        with pg.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bronze.etl_state(source_name, last_watermark)
                VALUES (%s, '1970-01-01')
                ON CONFLICT (source_name) DO NOTHING
                """,
                (SOURCE_NAME,),
            )
            cur.execute("SELECT last_watermark FROM bronze.etl_state WHERE source_name=%s", (SOURCE_NAME,))
            (wm,) = cur.fetchone()
        return wm.isoformat()

    @task
    def fetch_incremental(watermark_iso: str) -> List[Dict[str, Any]]:
        http = BaseHook.get_connection("commcare_http")
        base_url = http.host.rstrip("/")
        s = _get_commcare_session()

        last_wm = pendulum.parse(watermark_iso).in_timezone("UTC")

        # Recommended: lookback window to catch late/edited records
        lookback = last_wm.subtract(days=1)

        params = {
            "$filter": f"{WATERMARK_FIELD} gt {lookback.to_iso8601_string()}",
            "$orderby": f"{WATERMARK_FIELD} asc",
        }

        all_recs: List[Dict[str, Any]] = []

        r = s.get(f"{base_url}{ODATA_PATH}", params=params, timeout=60)
        r.raise_for_status()
        page = r.json()

        recs, next_link = _extract_records(page)
        all_recs.extend(recs)

        while next_link:
            path, q = _parse_nextlink(next_link)
            r = s.get(f"{base_url}{path}", params=q, timeout=60)
            r.raise_for_status()
            page = r.json()
            recs, next_link = _extract_records(page)
            all_recs.extend(recs)

        return all_recs

    @task
    def upsert_bronze(records: List[Dict[str, Any]]) -> Optional[str]:
        """
        Upsert mapped values into bronze.farmers_registration.
        Returns the new watermark (max WATERMARK_FIELD seen).
        """
        if not records:
            return None

        # Bronze columns in insert order (must match SQL statement)
        cols = [
            "form_id", "title", "first_name", "last_name", "date_of_birth", "gender", "age",
            "phone_number", "email_address", "business_category",
            "state_of_residence", "lga_of_residence", "electoral_ward_of_residence",
            "electoral_ward_of_residence_alt", "community", "classification_of_community",
            "highest_qualification", "disability", "type_of_disability", "type_of_disability_alt",
            "disability_classification", "languages_you_speak", "can_you_use_an_android_phone",
            "do_you_have_an_android_phone", "are_you_a_nursing_mother", "are_you_an_nysc_member",
            "are_you_waiting_to_go_for_nysc", "employment_status", "just_wrote_jamb_awaiting_admission",
            "marital_status", "confirm_if_you_living_in_this_community_because_of_any_of_the_f",
            "fullname_field_worker", "phone_number_field_worker", "age_group_category",
            "electoral_ward_of_participants", "idp_classification_of_participant",
            "duplicate_identifier", "unique_id_of_prospective_participants", "case_id",
            "form_link",
            "payload",
        ]

        # Invert mapping: bronze_col -> commcare_key
        bronze_to_commcare = {v: k for k, v in DF_TO_BRONZE.items()}

        rows = []
        max_wm = None

        for rec in records:
            # watermark tracking
            wm_val = rec.get(WATERMARK_FIELD)
            if wm_val:
                ts = pendulum.parse(str(wm_val)).in_timezone("UTC")
                if max_wm is None or ts > max_wm:
                    max_wm = ts

            # build row dict
            row = {}
            row["form_id"] = _get_form_id(rec)

            for c in cols:
                if c in ("form_id", "payload"):
                    continue
                commcare_key = bronze_to_commcare.get(c)
                val = rec.get(commcare_key) if commcare_key else None

                if c == "age":
                    row[c] = _to_int(val)
                elif c == "date_of_birth":
                    row[c] = _to_date(val)
                else:
                    row[c] = _to_text(val)

            row["payload"] = json.dumps(rec)

            rows.append(tuple(row.get(c) for c in cols))

        pg = PostgresHook("tractrac_postgres")

        insert_sql = f"""
        INSERT INTO bronze.year_two_farmers_registration ({",".join(cols)})
        VALUES %s
        ON CONFLICT (form_id) DO UPDATE SET
          title = EXCLUDED.title,
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          date_of_birth = EXCLUDED.date_of_birth,
          gender = EXCLUDED.gender,
          age = EXCLUDED.age,
          phone_number = EXCLUDED.phone_number,
          email_address = EXCLUDED.email_address,
          business_category = EXCLUDED.business_category,
          state_of_residence = EXCLUDED.state_of_residence,
          lga_of_residence = EXCLUDED.lga_of_residence,
          electoral_ward_of_residence = EXCLUDED.electoral_ward_of_residence,
          electoral_ward_of_residence_alt = EXCLUDED.electoral_ward_of_residence_alt,
          community = EXCLUDED.community,
          classification_of_community = EXCLUDED.classification_of_community,
          highest_qualification = EXCLUDED.highest_qualification,
          disability = EXCLUDED.disability,
          type_of_disability = EXCLUDED.type_of_disability,
          type_of_disability_alt = EXCLUDED.type_of_disability_alt,
          disability_classification = EXCLUDED.disability_classification,
          languages_you_speak = EXCLUDED.languages_you_speak,
          can_you_use_an_android_phone = EXCLUDED.can_you_use_an_android_phone,
          do_you_have_an_android_phone = EXCLUDED.do_you_have_an_android_phone,
          are_you_a_nursing_mother = EXCLUDED.are_you_a_nursing_mother,
          are_you_an_nysc_member = EXCLUDED.are_you_an_nysc_member,
          are_you_waiting_to_go_for_nysc = EXCLUDED.are_you_waiting_to_go_for_nysc,
          employment_status = EXCLUDED.employment_status,
          just_wrote_jamb_awaiting_admission = EXCLUDED.just_wrote_jamb_awaiting_admission,
          marital_status = EXCLUDED.marital_status,
          confirm_if_you_living_in_this_community_because_of_any_of_the_f =
              EXCLUDED.confirm_if_you_living_in_this_community_because_of_any_of_the_f,
          fullname_field_worker = EXCLUDED.fullname_field_worker,
          phone_number_field_worker = EXCLUDED.phone_number_field_worker,
          age_group_category = EXCLUDED.age_group_category,
          electoral_ward_of_participants = EXCLUDED.electoral_ward_of_participants,
          idp_classification_of_participant = EXCLUDED.idp_classification_of_participant,
          duplicate_identifier = EXCLUDED.duplicate_identifier,
          unique_id_of_prospective_participants = EXCLUDED.unique_id_of_prospective_participants,
          case_id = EXCLUDED.case_id,
          form_link = EXCLUDED.form_link,
          payload = EXCLUDED.payload,
          ingested_at = now()
        """

        with pg.get_conn() as conn, conn.cursor() as cur:
            execute_values(cur, insert_sql, rows, page_size=500)
            conn.commit()

        return max_wm.isoformat() if max_wm else None

    @task
    def update_watermark(new_wm_iso: Optional[str]) -> None:
        if not new_wm_iso:
            return
        pg = PostgresHook("tractrac_postgres")
        with pg.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bronze.etl_state
                SET last_watermark = GREATEST(last_watermark, %s::timestamptz)
                WHERE source_name = %s
                """,
                (new_wm_iso, SOURCE_NAME),
            )
            conn.commit()

    wm = get_watermark()
    recs = fetch_incremental(wm)
    new_wm = upsert_bronze(recs)
    update_watermark(new_wm)