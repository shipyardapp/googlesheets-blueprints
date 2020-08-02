import os
import re
import json
import csv
import tempfile
import argparse

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

SCOPES = ['https://spreadsheets.google.com/feeds',
          'https://www.googleapis.com/auth/drive']


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--source-file-name',
        dest='file_name',
        default='',
        required=True)
    parser.add_argument(
        '--tab-name',
        dest='tab_name',
        default=None,
        required=False)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default=None,
        required=True)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--cell-range',
        dest='cell_range',
        default='A1:ZZZ5000000',
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


def extract_file_name_from_source_full_path(source_full_path):
    """
    Use the file name provided in the source_file_name variable. Should be run only
    if a destination_file_name is not provided.
    """
    destination_file_name = os.path.basename(source_full_path)
    return destination_file_name


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and trailing '/' characters.
    """
    folder_name = folder_name.strip('/')
    if folder_name != '':
        folder_name = os.path.normpath(folder_name)
    return folder_name


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')
    combined_name = os.path.normpath(combined_name)

    return combined_name


def determine_destination_name(
    destination_folder_name,
    destination_file_name,
):
    """
    Determine the final destination name of the file being downloaded.
    """
    destination_name = combine_folder_and_file_name(
        destination_folder_name, destination_file_name)
    return destination_name


def find_google_cloud_storage_file_names(bucket, prefix=''):
    """
    Fetched all the files in the bucket which are returned in a list as
    Google Blob objects
    """
    return list(bucket.list_blobs(prefix=prefix))


def find_matching_files(file_blobs, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for blob in file_blobs:
        if re.search(file_name_re, blob.name):
            matching_file_names.append(blob)

    return matching_file_names


def download_google_sheet_file(
        service,
        spreadsheet_id,
        file_name,
        tab_name,
        cell_range,
        destination_file_name=None):
    """
    Download th contents of a spreadsheet from Google Sheets to local storage in
    the current working directory.
    """
    local_path = os.path.normpath(f'{os.getcwd()}/{destination_file_name}')
    try:
        if tab_name:
            if check_workbook_exists(service=service,
                                     spreadsheet_id=spreadsheet_id,
                                     tab_name=tab_name):
                cell_range = f'{tab_name}!{cell_range}'
            else:
                print(f'The tab {tab_name} could not be found')
                raise SystemExit(1)
        sheet = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=cell_range).execute()

        if not sheet.get('values'):
            print(f'No values for {file_name}.. Not downloading')
            return

        values = sheet['values']
        with open(local_path, '+w') as f:
            writer = csv.writer(f)
            writer.writerows(values)
        print(
            f'Successfully downloaded {file_name} - {tab_name} to {local_path}')
    except Exception as e:
        print(f'Failed to download {file_name} from Google Sheets')
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


def check_workbook_exists(service, spreadsheet_id, tab_name):
    """
    Checks if the workbook exists within the spreadsheet.
    """
    try:
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet['sheets']
        exists = [True for sheet in sheets if sheet['properties']
                  ['title'] == tab_name]
        return True if exists else False
    except Exception as e:
        print(f'Failed to check workbook {tab_name} for spreadsheet '
              f'{spreadsheet_id}')
        raise(e)


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


def main():
    args = get_args()
    tmp_file = set_environment_variables(args)
    file_name = clean_folder_name(args.file_name)
    tab_name = args.tab_name
    cell_range = 'A1:ZZZ5000000' if not args.cell_range else args.cell_range
    drive = args.drive

    destination_folder_name = clean_folder_name(args.destination_folder_name)
    if not os.path.exists(destination_folder_name) and \
            (destination_folder_name != ''):
        os.makedirs(destination_folder_name)

    if tmp_file:
        service, drive_service = get_service(credentials=tmp_file)
    else:
        service, drive_service = get_service(
            credentials=args.gcp_application_credentials)

    spreadsheet_id = get_spreadsheet_id_by_name(
        drive_service=drive_service, file_name=file_name, drive=drive)
    if not spreadsheet_id:
        print(f'Sheet {file_name} does not exist')
        raise SystemExit(1)

    if not args.destination_file_name:
        args.destination_file_name = f'{file_name} - {tab_name}.csv'
    destination_name = determine_destination_name(
        destination_folder_name=destination_folder_name,
        destination_file_name=args.destination_file_name)

    if len(destination_name.rsplit('/', 1)) > 1:
        path = destination_name.rsplit('/', 1)[0]
        if not os.path.exists(path):
            os.makedirs(path)

    download_google_sheet_file(
        service=service,
        tab_name=tab_name,
        spreadsheet_id=spreadsheet_id,
        file_name=file_name,
        cell_range=cell_range,
        destination_file_name=destination_name)

    if tmp_file:
        print(f'Removing temporary credentials file {tmp_file}')
        os.remove(tmp_file)


if __name__ == '__main__':
    main()
