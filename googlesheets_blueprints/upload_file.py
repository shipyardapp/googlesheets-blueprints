import os
import re
import json
import csv
import tempfile
import argparse
import socket
import glob

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
socket.setdefaulttimeout(600)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument(
        "--destination-file-name", dest="file_name", default="", required=False
    )
    parser.add_argument(
        "--starting-cell", dest="starting_cell", default="A1", required=False
    )
    parser.add_argument("--tab-name", dest="tab_name", default=None, required=False)
    parser.add_argument(
        "--service-account",
        dest="gcp_application_credentials",
        default=None,
        required=True,
    )
    parser.add_argument("--drive", dest="drive", default=None, required=False)
    return parser.parse_args()


def set_environment_variables(args):
    """
    Set GCP credentials as environment variables if they're provided via keyword
    arguments rather than seeded as environment variables. This will override
    system defaults.
    """
    credentials = args.gcp_application_credentials
    try:
        json_credentials = json.loads(credentials)
        fd, path = tempfile.mkstemp()
        print(f"Storing json credentials temporarily at {path}")
        with os.fdopen(fd, "w") as tmp:
            tmp.write(credentials)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        return path
    except Exception:
        print("Using specified json credentials file")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials
        return


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and
    trailing '/' characters.
    """
    folder_name = folder_name.strip("/")
    if folder_name != "":
        folder_name = os.path.normpath(folder_name)
    return folder_name


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path
    variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}'
    )
    combined_name = os.path.normpath(combined_name)

    return combined_name


def check_workbook_exists(service, spreadsheet_id, tab_name):
    """
    Checks if the workbook exists within the spreadsheet.
    """
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet["sheets"]
        exists = [True for sheet in sheets if sheet["properties"]["title"] == tab_name]
        return True if exists else False
    except Exception as e:
        print(
            f"Failed to check spreadsheet {spreadsheet_id} for a sheet "
            f"named {tab_name}"
        )
        raise (e)


def add_workbook(service, spreadsheet_id, tab_name):
    """
    Adds a workbook to the spreadsheet.
    """
    try:
        request_body = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": tab_name,
                        }
                    }
                }
            ]
        }

        response = (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute()
        )

        return response
    except Exception as e:
        print(e)


def upload_google_sheets_file(
    service, file_name, source_full_path, starting_cell, spreadsheet_id, tab_name
):
    """
    Uploads a single file to Google Sheets.
    """
    try:
        if not spreadsheet_id:
            file_metadata = {
                "properties": {"title": file_name},
                "namedRanges": {"range": starting_cell},
            }
            spreadsheet = (
                service.spreadsheets()
                .create(body=file_metadata, fields="spreadsheetId")
                .execute()
            )
            spreadsheet_id = spreadsheet["spreadsheetId"]

        # check if the workbook exists and create it if it doesn't
        workbook_exists = check_workbook_exists(
            service=service, spreadsheet_id=spreadsheet_id, tab_name=tab_name
        )
        if not workbook_exists:
            add_workbook(
                service=service, spreadsheet_id=spreadsheet_id, tab_name=tab_name
            )

        data = []
        with open(
            source_full_path, encoding="utf-8", newline=""
        ) as f:  # adding unicode encoding
            reader = csv.reader((line.replace("\0", "") for line in f), delimiter=",")
            for row in reader:
                # stripping any empty rows
                if set(row) != {""}:
                    data.append(row)

        if starting_cell:
            _range = f"{starting_cell}:ZZZ5000000"
        else:
            _range = "A1:ZZZ5000000"

        if tab_name:
            _range = f"{tab_name}!{_range}"

        body = {
            "value_input_option": "RAW",
            "data": [{"values": data, "range": _range, "majorDimension": "ROWS"}],
        }
        response = (
            service.spreadsheets()
            .values()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )
    except Exception as e:
        if isinstance(e, FileNotFoundError):
            print(f"File {source_full_path} does not exist.")
        elif hasattr(e, "content"):
            err_msg = json.loads(e.content)
            if "workbook above the limit" in err_msg["error"]["message"]:
                print(
                    f"Failed to upload due to input csv size {source_full_path}"
                    " being to large (Limit is 5,000,000 cells)"
                )
        else:
            print(f"Failed to upload spreadsheet {source_full_path} to " f"{file_name}")
        raise (e)

    print(f"{source_full_path} successfully uploaded to {file_name}")


def get_shared_drive_id(service, drive):
    """
    Search for the drive under shared Google Drives.
    """
    drives = service.drives().list().execute()
    drive_id = None
    for _drive in drives["drives"]:
        if _drive["name"] == drive:
            drive_id = _drive["id"]
    return drive_id


def get_spreadsheet_id_by_name(drive_service, file_name, drive):
    """
    Attempts to get sheet id from the Google Drive Client using the
    sheet name
    """
    try:
        drive_id = None
        if drive:
            drive_id = get_shared_drive_id(drive_service, drive)

        query = 'mimeType="application/vnd.google-apps.spreadsheet"'
        query += f' and name = "{file_name}"'
        if drive:
            results = (
                drive_service.files()
                .list(
                    q=str(query),
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    corpora="drive",
                    driveId=drive_id,
                    fields="files(id, name)",
                )
                .execute()
            )
        else:
            results = drive_service.files().list(q=str(query)).execute()
        files = results["files"]
        for _file in files:
            return _file["id"]
        return None
    except Exception as e:
        print(f"Failed to fetch spreadsheetId for {file_name}")
        raise (e)


def get_service(credentials):
    """
    Attempts to create the Google Drive Client with the associated
    environment variables
    """
    try:
        creds = service_account.Credentials.from_service_account_file(
            credentials, scopes=SCOPES
        )
        service = build("sheets", "v4", credentials=creds)
        drive_service = build("drive", "v3", credentials=creds)
        return service, drive_service
    except Exception as e:
        print(f"Error accessing Google Drive with service account " f"{credentials}")
        raise (e)


def main():
    args = get_args()
    tmp_file = set_environment_variables(args)
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = combine_folder_and_file_name(
        folder_name=f"{os.getcwd()}/{source_folder_name}", file_name=source_file_name
    )
    file_name = clean_folder_name(args.file_name)
    tab_name = args.tab_name
    starting_cell = "A1" if not args.starting_cell else args.starting_cell
    drive = args.drive

    if not os.path.isfile(source_full_path):
        print(f"{source_full_path} does not exist")
        raise SystemExit(1)

    if tmp_file:
        service, drive_service = get_service(credentials=tmp_file)
    else:
        service, drive_service = get_service(
            credentials=args.gcp_application_credentials
        )

    spreadsheet_id = get_spreadsheet_id_by_name(
        drive_service=drive_service, file_name=file_name, drive=drive
    )
    if not spreadsheet_id:
        if len(file_name) >= 44:
            spreadsheet_id = file_name
        else:
            print(f"The spreadsheet {file_name} does not exist")
            raise SystemExit(1)

    # check if workbook exists in the spreadsheet
    upload_google_sheets_file(
        service=service,
        file_name=file_name,
        source_full_path=source_full_path,
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        starting_cell=starting_cell,
    )

    if tmp_file:
        print(f"Removing temporary credentials file {tmp_file}")
        os.remove(tmp_file)


if __name__ == "__main__":
    main()
