'''
To use these functions, grant the "Editor" permission to the service account email on the target Google Sheet and
                                                                            change the ID in the function parameter

"gg-service-agent@legalrag-471601.iam.gserviceaccount.com" 

Link to sheet: https://docs.google.com/spreadsheets/d/1xBgBiA1KwTNdqPfrqH5p_Sf-MhTCXMfy4ousb0WE4Ik/edit

Link to drive: https://drive.google.com/drive/folders/1BU77ORL7HjSVvY4rHwpUEVf9zGIi4RFs?usp=drive_link
'''
import io, sys, os
import gspread
import pandas as pd
import json
# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive
# from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from typing import List, Dict, Optional
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

google_api_creds = 'D:/Study/Education/Projects/Group_Project/secrets/ggsheet_credentials.json'
spreadsheet_id = os.getenv('GOOGLE_SHEET_ID') 
drive_id = os.getenv('GOOGLE_DRIVE_ID')

#Google Sheet
def gs_to_df_pandas(tab_name, spreadsheet_id = spreadsheet_id, creds_path=google_api_creds):
  gc = gspread.service_account(filename=creds_path)
  sh = gc.open_by_key(spreadsheet_id)
  wks = sh.worksheet(tab_name)
  df = pd.DataFrame(wks.get_all_records())
  return df

def gs_to_dict( tab_name, spreadsheet_id = spreadsheet_id, creds_path=google_api_creds):
    gc = gspread.service_account(filename=creds_path)
    sh = gc.open_by_key(spreadsheet_id)
    wks = sh.worksheet(tab_name)
    results_json = wks.get_all_records()
    return results_json

def write_df_to_gs(df, tab_name, spreadsheet_id = spreadsheet_id, creds_path=google_api_creds):
    import gspread
    from gspread.exceptions import WorksheetNotFound
    
    gc = gspread.service_account(filename=creds_path)
    sh = gc.open_by_key(spreadsheet_id)

    try:
        # Try to open existing worksheet
        wks = sh.worksheet(tab_name)
        # Find the last filled row
        last_row = len(wks.get_all_values())
        # Append DataFrame values (without header)
        wks.update(f"A{last_row+1}", df.values.tolist())
        return f"DataFrame appended to existing Google Sheet tab: {tab_name}"
    except WorksheetNotFound:
        # If worksheet does not exist, create it
        wks = sh.add_worksheet(title=tab_name, rows=str(len(df)+1), cols=str(len(df.columns)))
        wks.update([df.columns.values.tolist()] + df.values.tolist())
        return f"New tab created and DataFrame written to Google Sheet: {tab_name}"

#Google Drive
def get_drive_service(creds_path: str = google_api_creds, scopes: Optional[List[str]] = None):
    """
    Authenticate and return a Google Drive API service object.
    """
    if scopes is None:
        scopes = ['https://www.googleapis.com/auth/drive']
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=scopes)
    return build('drive', 'v3', credentials=creds)

def list_drive_files(
    folder_id: str = drive_id,
    creds_path: str = google_api_creds,
    prefix_path: str = ''
) -> List[Dict]:
    service = get_drive_service(creds_path)
    results = []

    def _list_recursive(current_id: str, current_path: str):
        query = f"'{current_id}' in parents and trashed = false"
        response = service.files().list(
            q=query,
            fields="files(id, name, mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        items = response.get('files', [])

        for item in items:
            full_path = os.path.join(current_path, item['name'])
            results.append({
                'id': item['id'],
                'name': item['name'],
                'path': full_path,
                'mimeType': item['mimeType']
            })
            # If folder, recurse deeper
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                _list_recursive(item['id'], full_path)

    _list_recursive(folder_id, prefix_path)
    
    def build_tree(paths):
        """Build nested dict tree from a list of path strings."""
        tree = lambda: defaultdict(tree)
        root = tree()
        for p in paths:
            parts = p.split(os.sep)
            node = root
            for part in parts:
                node = node[part]
        return root

    def print_tree(node, prefix=""):
        """Recursively pretty-print the directory tree."""
        items = sorted(node.keys())
        for i, key in enumerate(items):
            connector = "└── " if i == len(items) - 1 else "├── "
            print(prefix + connector + key)
            print_tree(node[key], prefix + ("    " if i == len(items) - 1 else "│   "))

    def show_drive_tree(file_list):
        """Pretty-print folder/file hierarchy from Drive file list."""
        paths = [f["path"] for f in file_list]
        tree = build_tree(paths)
        print_tree(tree)
        
    return show_drive_tree(results)

def find_file_full_path(
    filename: str,
    creds_path: str = google_api_creds,
    drive_id: Optional[str] = None
) -> Optional[Dict]:

    creds = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build("drive", "v3", credentials=creds)

    # === 2. Search for file ===
    q = f"name='{filename}' and trashed=false"
    params = {
        "q": q,
        "fields": "files(id, name, parents)",
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }
    if drive_id:
        params.update({"corpora": "drive", "driveId": drive_id})

    response = service.files().list(**params).execute()
    files = response.get("files", [])

    if not files:
        print(f"❌ File '{filename}' not found.")
        return None

    file = files[0]
    path_parts = [file["name"]]
    parent_id = file.get("parents", [None])[0]

    # === 3. Walk upward to reconstruct full folder path ===
    while parent_id:
        parent = service.files().get(
            fileId=parent_id,
            fields="id, name, parents",
            supportsAllDrives=True
        ).execute()
        path_parts.insert(0, parent["name"])
        parent_id = parent.get("parents", [None])[0]

    full_path = "/".join(path_parts[1:])

    # === 4. Return consistent dictionary ===
    return full_path

def read_drive_file(path: str, creds_path: str = google_api_creds, as_type: str = None, drive_id: str = drive_id):

    full_path = find_file_full_path(path)

    service = get_drive_service(creds_path)
    parts = full_path.replace("\\", "/").split("/")
    parent_id = drive_id

    # Traverse folder structure
    for i, part in enumerate(parts):
        q = f"'{parent_id}' in parents and name = '{part}' and trashed = false"
        results = service.files().list(q=q, fields="files(id, name, mimeType)").execute()
        files = results.get("files", [])

        if not files:
            raise FileNotFoundError(f"❌ Path component not found: {part} (full: {'/'.join(parts[:i+1])})")

        file = files[0]
        parent_id = file["id"]

    # If final part is folder
    if file["mimeType"] == "application/vnd.google-apps.folder":
        raise IsADirectoryError(f"'{full_path}' is a folder, not a file")

    file_name = file["name"]
    mime_type = file["mimeType"]

    # Infer file type
    if as_type is None:
        if file_name.endswith(".json") or "json" in mime_type:
            as_type = "json"
        elif file_name.endswith(".csv") or "csv" in mime_type:
            as_type = "csv"
        else:
            as_type = "txt"

    # Download
    request = service.files().get_media(fileId=file["id"])
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)

    # Parse content
    if as_type == "json":
        return json.load(io.TextIOWrapper(fh, encoding="utf-8"))
    elif as_type == "csv":
        return pd.read_csv(fh)
    elif as_type == "txt":
        return fh.read().decode("utf-8")
    else:
        raise ValueError(f"Unsupported file type: {as_type}")
    
    