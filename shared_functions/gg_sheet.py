'''
To use these functions, firstly grant the "Editor" permission to the service account email on the target Google Sheet

"gg-service-agent@legalrag-471601.iam.gserviceaccount.com" 
'''

import gspread
import pandas as pd
# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive
# from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

my_google_api_creds = 'D:/Study/Education/Projects/Group_Project_RAG/secrets/ggsheet_credentials.json'
spreadsheet_id = '1xBgBiA1KwTNdqPfrqH5p_Sf-MhTCXMfy4ousb0WE4Ik' #Default working spreadsheet

def gs_to_df_pandas(tab_name, spreadsheet_id = spreadsheet_id, creds_path=my_google_api_creds):
  gc = gspread.service_account(filename=creds_path)
  sh = gc.open_by_key(spreadsheet_id)
  wks = sh.worksheet(tab_name)
  df = pd.DataFrame(wks.get_all_records())
  return df

def gs_to_dict( tab_name, spreadsheet_id = spreadsheet_id, creds_path=my_google_api_creds):
    gc = gspread.service_account(filename=creds_path)
    sh = gc.open_by_key(spreadsheet_id)
    wks = sh.worksheet(tab_name)
    results_json = wks.get_all_records()
    return results_json

def write_df_to_gs(df, tab_name, spreadsheet_id = spreadsheet_id, creds_path=my_google_api_creds):
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
