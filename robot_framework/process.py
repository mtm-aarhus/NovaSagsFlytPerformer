from OpenOrchestrator.database.queues import QueueElement
import os
import json
import uuid
from datetime import datetime
import pyodbc
from nova import *
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection


# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    orchestrator_connection.log_trace("Running process.")

    # --- Queue data ---
    queue_json = json.loads(queue_element.data)
    oprindelig_sagsbehandler = queue_json.get("Oprindelig sagsbehandler").upper()
    ny_sagsbehandler = queue_json.get("Ny sagsbehandler").upper()
    novasagsnummer = queue_json.get("Novasagsnummer").upper()

    # ---------- CONFIG ----------
    sql_server = orchestrator_connection.get_constant("SqlServer").value
    TABLE_NAME = "NovaSagsFlytLogs"
    conn_str = ("DRIVER={ODBC Driver 17 for SQL Server};"f"SERVER={sql_server};DATABASE=PyOrchestrator;Trusted_Connection=yes")


    def connect_db():
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Opret logtabel hvis den ikke findes
        cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{TABLE_NAME}')
        BEGIN
            CREATE TABLE {TABLE_NAME} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                sagsnummer NVARCHAR(100) NOT NULL,
                oprindelig_sagsbehandler NVARCHAR(100),
                ny_sagsbehandler NVARCHAR(100),

                fetch_case_status INT,
                fetch_case_response NVARCHAR(MAX),

                lookup_new_caseworker_status INT,
                lookup_new_caseworker_response NVARCHAR(MAX),

                update_tasks_status NVARCHAR(200),
                update_tasks_response NVARCHAR(MAX),

                update_case_status INT,
                update_case_response NVARCHAR(200),

                create_task_status INT,
                create_task_response NVARCHAR(200),

                processed_at NVARCHAR(50),

                created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            );

            CREATE INDEX IX_{TABLE_NAME}_Sagsnummer ON {TABLE_NAME}(sagsnummer);
        END
        """)
        conn.commit()
        return conn

    def commit_info_to_database(conn, oprindelig_sagsbehandler, ny_sagsbehandler, novasagsnummer):
        """Indsæt én log-række pr. bestilling."""
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO {TABLE_NAME} (sagsnummer, oprindelig_sagsbehandler, ny_sagsbehandler)
            VALUES (?, ?, ?)
            """,
            (novasagsnummer, oprindelig_sagsbehandler, ny_sagsbehandler),
        )
        conn.commit()

    def dict_preview(d, max_len=2000):
        """Safely JSON-dumps a dict and truncates (for logging)."""
        try:
            s = json.dumps(d, ensure_ascii=False)
        except Exception:
            s = str(d)
        if len(s) > max_len:
            return s[:max_len] + "…"
        return s

    def run_pipeline_for_unprocessed_rows(conn):
        access_token = get_access_token(orchestrator_connection)
        Nova_URL = orchestrator_connection.get_constant("KMDNovaURL").value

        cursor = conn.cursor()

        # Kun rækker, der ikke er afsluttet endnu (processed_at IS NULL)
        query = f"""
        SELECT id, sagsnummer, oprindelig_sagsbehandler, ny_sagsbehandler
        FROM {TABLE_NAME}
        WHERE processed_at IS NULL
        """
        cursor.execute(query)
        to_process = cursor.fetchall()

        orchestrator_connection.log_error(f"Found {len(to_process)} row(s) to process.")

        caseworker_cache: dict[str, dict | None] = {}

        for row_id, sagsnr, oldazident, newazident in to_process:
            orchestrator_connection.log_error(f"\nProcessing {sagsnr}: {oldazident} ➝ {newazident}")

            # Default row fields to update
            updates: dict[str, object] = {
                "fetch_case_status": None,
                "fetch_case_response": None,
                "lookup_new_caseworker_status": None,
                "lookup_new_caseworker_response": None,
                "update_tasks_status": None,
                "update_tasks_response": None,
                "update_case_status": None,
                "update_case_response": None,
                "create_task_status": None,
                "create_task_response": None,
                "processed_at": None,
            }

            try:
                # --- 1) Fetch case list og find sagen for den oprindelige sagsbehandler ---
                txn1 = str(uuid.uuid4())
                response_json = fetch_case(sagsnr, txn1, access_token, Nova_URL, orchestrator_connection)
                updates["fetch_case_status"] = 200
                updates["fetch_case_response"] = dict_preview(response_json)

                cases = response_json.get("cases") or []
                case_uuid = None
                caseworker_fullname = None

                for case in cases:
                    attrs = (case.get("caseAttributes") or {})
                    userFriendlyCaseNumber = (attrs.get("userFriendlyCaseNumber") or "").strip()

                    ksp = (case.get("caseworker") or {}).get("kspIdentity") or {}
                    racf = (ksp.get("racfId") or "").strip()

                    if (
                        racf.lower() == (oldazident or "").strip().lower()
                        and userFriendlyCaseNumber.lower() == (sagsnr or "").strip().lower()
                    ):
                        common = case.get("common") or {}
                        case_uuid = common.get("uuid")
                        caseworker_fullname = ksp.get("fullName")
                        break

                if not case_uuid:
                    raise RuntimeError("No case uuid matched the original caseworker / case number")

                # --- 2) Lookup ny sagsbehandler (med cache) ---
                cache_key = (newazident or "").strip().lower()
                if cache_key in caseworker_cache:
                    new_caseworker = caseworker_cache[cache_key]
                else:
                    new_caseworker = lookup_caseworker_by_racfId(
                        newazident, str(uuid.uuid4()), access_token, Nova_URL
                    )
                    caseworker_cache[cache_key] = new_caseworker

                if new_caseworker:
                    updates["lookup_new_caseworker_status"] = 200
                    updates["lookup_new_caseworker_response"] = dict_preview(new_caseworker)
                else:
                    updates["lookup_new_caseworker_status"] = 404
                    updates["lookup_new_caseworker_response"] = "Not found"
                    raise RuntimeError("New caseworker not found")

                new_caseworker_fullname = new_caseworker.get("kspIdentity", {}).get("fullName")

                # --- 3) Hent tasks og flyt dem fra oldazident til ny, hvis status != 'F' ---
                task_list = get_task_list(str(uuid.uuid4()), case_uuid, access_token, Nova_URL)
                tasks_to_update = [
                    t
                    for t in (task_list or [])
                    if t.get("caseworker", {})
                    .get("kspIdentity", {})
                    .get("racfId", "")
                    == oldazident
                    and t.get("taskStatusCode") != "F"
                ]

                per_task_results = []
                updated_count = 0
                skipped_count = 0

                for t in tasks_to_update:
                    try:
                        status_code = update_caseworker_task(t, access_token, Nova_URL, new_caseworker)
                        per_task_results.append(
                            {
                                "taskUuid": t.get("taskUuid"),
                                "title": t.get("taskTitle"),
                                "status": status_code,
                            }
                        )
                        if 200 <= int(status_code) < 300:
                            updated_count += 1
                        else:
                            skipped_count += 1
                    except Exception as e:
                        per_task_results.append(
                            {
                                "taskUuid": t.get("taskUuid"),
                                "title": t.get("taskTitle"),
                                "status": "ERROR",
                                "error": str(e)
                            }
                        )
                        skipped_count += 1

                updates["update_tasks_status"] = f"updated:{updated_count};failed:{skipped_count}"
                updates["update_tasks_response"] = dict_preview(per_task_results)

                # --- 4) Opdater sagen til ny sagsbehandler ---
                status_code_case = update_caseworker_case(case_uuid, new_caseworker, access_token, Nova_URL, orchestrator_connection)
                updates["update_case_status"] = status_code_case
                updates["update_case_response"] = f"case_uuid:{case_uuid}"

                # --- 5) Opret bekræftelsesopgave på sagen ---
                timestamp = datetime.now().strftime("%d-%m-%Y kl. %H:%M")

                desc = (
                    f"Robotten har overført sagen fra {caseworker_fullname} til {new_caseworker_fullname} "
                    f"{timestamp}. Husk at ændre assistent på opgaverne hvis det er relevant."
                )

                status_code_task = create_task(case_uuid, new_caseworker, desc, access_token, Nova_URL)
                updates["create_task_status"] = status_code_task
                updates["create_task_response"] = "Task created"

                # Markér som færdigbehandlet
                updates["processed_at"] = datetime.now().isoformat(timespec="seconds")

            except Exception as e:
                # Ved fejl gemmer vi det, vi har – processed_at forbliver NULL,
                # så rækken kan blive forsøgt igen senere.
                orchestrator_connection.log_error(f"Error on {sagsnr}: {e}")

            # Persist updates to SQL Server for denne log-række
            set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
            params = [updates[k] for k in updates.keys()] + [row_id]

            cursor.execute(
                f"UPDATE {TABLE_NAME} SET {set_clause} WHERE id = ?",
                params,
            )

        conn.commit()
        orchestrator_connection.log_error("\nDone.")


    conn = connect_db()
    commit_info_to_database(conn, oprindelig_sagsbehandler, ny_sagsbehandler, novasagsnummer)
    run_pipeline_for_unprocessed_rows(conn)
    conn.close()
