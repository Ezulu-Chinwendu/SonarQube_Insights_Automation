import os
import json
import requests
import pandas as pd
from datetime import datetime
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

def sonar_update(request):
    # === Step 1: Load environment variables ===
    try:
        sonar_url = os.environ["SONAR_URL"]
        sonar_token = os.environ["SONAR_TOKEN"]
        spreadsheet_name = os.environ["SPREADSHEET_NAME"]
        service_account_json = os.environ["SERVICE_ACCOUNT_JSON"]
    except KeyError as e:
        return f"Missing environment variable: {e}", 500

    # === Step 2: Parse the service account JSON with lenient parsing ===
    try:
        service_account_info = json.loads(service_account_json, strict=False)
        if "private_key" in service_account_info:
            service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        client = gspread.authorize(creds)
    except Exception as e:
        return f"Error initializing Google Sheets client: {e}", 500

    # === Step 3: Define SonarQube projects and metrics ===
    projects = ["AngularJS", "Dotnet", "SimpleApp_Angular"]
    metric_mapping = {
        "Security": "security_rating",
        "Reliability": "reliability_rating",
        "Maintainability": "software_quality_maintainability_rating",  
        "Duplications": "duplicated_lines_density"
    }

    today = datetime.today().strftime("%Y-%m-%d")
    columns = ["Date", "Project Name", "Quality Gate Status"] + list(metric_mapping.keys())
    all_rows = []

    # === Step 4: Fetch data from SonarQube ===
    def fetch_project_data(project_key):
        # Quality Gate
        qg_url = f"{sonar_url}/api/qualitygates/project_status?projectKey={project_key}"
        try:
            qg_resp = requests.get(qg_url, auth=(sonar_token, ""))
            qg_resp.raise_for_status()  # Will raise an HTTPError for bad status codes
            qg_status = qg_resp.json().get("projectStatus", {}).get("status", "N/A")
        except requests.exceptions.HTTPError as errh:
            return f"HTTP error occurred: {errh}", 500
        except requests.exceptions.RequestException as err:
            return f"Error fetching quality gate data: {err}", 500

        # Metrics
        metric_keys = ",".join(metric_mapping.values())
        metrics_url = f"{sonar_url}/api/measures/component?component={project_key}&metricKeys={metric_keys}"
        try:
            metric_resp = requests.get(metrics_url, auth=(sonar_token, ""))
            metric_resp.raise_for_status()  # Will raise an HTTPError for bad status codes
        except requests.exceptions.HTTPError as errh:
            return f"HTTP error occurred: {errh}", 500
        except requests.exceptions.RequestException as err:
            return f"Error fetching metrics data: {err}", 500
        
        values = {label: None for label in metric_mapping.keys()}
        if metric_resp.status_code == 200:
            measures = metric_resp.json().get("component", {}).get("measures", [])
            for m in measures:
                for label, key in metric_mapping.items():
                    if m["metric"] == key:
                        values[label] = m.get("value", None)

        return [today, project_key, qg_status] + [values[label] for label in metric_mapping.keys()]

    for project in projects:
        try:
            row = fetch_project_data(project)
            if isinstance(row, tuple):  # Check if an error occurred in fetching
                return row
            all_rows.append(row)
        except Exception as e:
            return f"Error fetching data for project {project}: {e}", 500

    # === Step 5: Update Google Sheet ===
    try:
        sheet = client.open(spreadsheet_name).sheet1
    except Exception as e:
        return f"Error opening spreadsheet: {e}", 500

    # Read existing records
    try:
        existing_data = sheet.get_all_records()
        existing_df = pd.DataFrame(existing_data) if existing_data else pd.DataFrame(columns=columns)
    except Exception as e:
        existing_df = pd.DataFrame(columns=columns)

    # Append new rows
    df_new = pd.DataFrame(all_rows, columns=columns)
    final_df = pd.concat([existing_df, df_new], ignore_index=True)

    try:
        sheet.clear()
        set_with_dataframe(sheet, final_df, include_index=False)
    except Exception as e:
        return f"Error writing to spreadsheet: {e}", 500

    return json.dumps({
        "status": "success",
        "data": df_new.to_dict(orient="records")
    }), 200
