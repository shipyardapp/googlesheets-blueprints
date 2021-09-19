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

SCOPES = ['https://spreadsheets.google.com/feeds',
          'https://www.googleapis.com/auth/drive']
socket.setdefaulttimeout(600)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--destination-file-name',
        dest='file_name',
        default='',
        required=False)
    parser.add_argument(
        '--cell-range',
        dest='cell_range',
        default='A1:ZZZ5000000',
        required=False)
    parser.add_argument(
        '--tab-name',
        dest='tab_name',
        default=None,
        required=False)
    parser.add_argument(
        '--service-account',
        dest='gcp_application_credentials',
        default=None,
        required=True)
    parser.add_argument('--drive', dest='drive', default=None, required=False)
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
        print(f'Storing json credentials temporarily at {path}')
        with os.fdopen(fd, 'w') as tmp:
            tmp.write(credentials)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
        return path
    except Exception:
        print('Using specified json credentials file')
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
        return


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and
    trailing '/' characters.
    """
    folder_name = folder_name.strip('/')
    if folder_name != '':
        folder_name = os.path.normpath(folder_name)
    return folder_name


def clear_google_sheet(
        service,
        file_name,
        cell_range,
        spreadsheet_id,
        tab_name):
    """
    Clears data from a single Google Sheet.
    """
    try:
        if not spreadsheet_id:
            file_metadata = {'properties': {
                'title': file_name
            },
                'namedRanges': {
                'range': cell_range
            }
            }
            spreadsheet = service.spreadsheets().create(
                body=file_metadata, fields='spreadsheetId').execute()
            spreadsheet_id = spreadsheet['spreadsheetId']

        if tab_name:
            cell_range = f'{tab_name}!{cell_range}'

        response = service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=cell_range).execute()
    except Exception as e:
        if hasattr(e, 'content'):
            err_msg = json.loads(e.content)
        else:
            print(f'Failed to clear spreadsheet {file_name}')
        raise(e)

    print(f'{file_name} succcessfully cleared between range {cell_range}.')


def get_shared_drive_id(service, drive):
    """
    Search for the drive under shared Google Drives.
    """
    drives = service.drives().list().execute()
    drive_id = None
    for _drive in drives['drives']:
        if _drive['name'] == drive:
            drive_id = _drive['id']
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
            results = drive_service.files().list(
                q=str(query),
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                corpora="drive",
                driveId=drive_id,
                fields="files(id, name)").execute()
        else:
            results = drive_service.files().list(q=str(query)).execute()
        files = results['files']
        for _file in files:
            return _file['id']
        return None
    except Exception as e:
        print(f'Failed to fetch spreadsheetId for {file_name}')
        raise(e)


def get_service(credentials):
    """
    Attempts to create the Google Drive Client with the associated
    environment variables
    """
    try:
        creds = service_account.Credentials.from_service_account_file(
            credentials, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        drive_service = build('drive', 'v3', credentials=creds)
        return service, drive_service
    except Exception as e:
        print(f'Error accessing Google Drive with service account '
              f'{credentials}')
        raise(e)


def main():
    args = get_args()
    tmp_file = set_environment_variables(args)
    file_name = clean_folder_name(args.file_name)
    tab_name = args.tab_name
    cell_range = 'A1:ZZZ5000000' if not args.cell_range else args.cell_range
    drive = args.drive

    if tmp_file:
        service, drive_service = get_service(credentials=tmp_file)
    else:
        service, drive_service = get_service(
            credentials=args.gcp_application_credentials)

    spreadsheet_id = get_spreadsheet_id_by_name(
        drive_service=drive_service, file_name=file_name, drive=drive)
    if not spreadsheet_id:
        if len(file_name) >= 44:
            spreadsheet_id = file_name
        else:
            print(f'The spreadsheet {file_name} does not exist')
            raise SystemExit(1)

    # check if workbook exists in the spreadsheet
    clear_google_sheet(service=service, file_name=file_name,
                       spreadsheet_id=spreadsheet_id,
                       tab_name=tab_name,
                       cell_range=cell_range)

    if tmp_file:
        print(f'Removing temporary credentials file {tmp_file}')
        os.remove(tmp_file)


if __name__ == '__main__':
    main()
