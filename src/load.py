import os
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

class GDriveUploader:
    def __init__(self, service_json_path='client_secrets.json'):
        gauth = GoogleAuth()
        gauth.settings['client_config_backend'] = 'service'
        gauth.settings['service_config'] = {
            'client_json_file_path': service_json_path,
        }
        gauth.ServiceAuth()
        self.drive = GoogleDrive(gauth)

    def get_or_create_folder(self, folder_name, parent_id):
        """Checks if a folder exists; if not, creates it."""
        query = (f"title = '{folder_name}' and '{parent_id}' in parents "
                 f"and mimeType = 'application/vnd.google-apps.folder' and trashed = false")
        file_list = self.drive.ListFile({'q': query}).GetList()
        
        if file_list:
            return file_list[0]['id']
        else:
            folder_metadata = {
                'title': folder_name,
                'parents': [{'id': parent_id}],
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = self.drive.CreateFile(folder_metadata)
            folder.Upload()
            return folder['id']

    def upload_recursive(self, local_path, parent_id):
        """Uploads files and creates subdirectories in Google Drive."""
        # Optimization: Fetch all files in the current Drive folder once
        query = f"'{parent_id}' in parents and trashed = false"
        existing_items = {f['title']: f['id'] for f in self.drive.ListFile({'q': query}).GetList()}

        for item in os.listdir(local_path):
            local_item_path = os.path.join(local_path, item)
            
            if os.path.isdir(local_item_path):
                # If folder exists, get ID; otherwise create it
                subfolder_id = self.get_or_create_folder(item, parent_id)
                self.upload_recursive(local_item_path, subfolder_id)
            else:
                # Check against our local cache of existing files
                if item not in existing_items:
                    print(f"Uploading: {item}")
                    gfile = self.drive.CreateFile({'title': item, 'parents': [{'id': parent_id}]})
                    gfile.SetContentFile(local_item_path)
                    gfile.Upload()
                else:
                    print(f"Skipping (exists): {item}")

def upload_to_gdrive():
    """
    Orchestrates the upload of the three specific ETL output directories.
    Note: Replace the strings below with the actual Folder IDs from your browser URL.
    """
    # These IDs are found in the URL: drive.google.com/drive/folders/YOUR_ID_HERE
    TARGET_DRIVE_IDS = {
        "etl/raw": "FOLDER_ID_FOR_PARTICIPANT_DATA_RAW",
        "etl/organized_data": "FOLDER_ID_FOR_PARTICIPANT_DATA_ORGANIZED",
        "etl/phase_segmented": "FOLDER_ID_FOR_PARTICIPANT_DATA_PHASE_SEGMENTED"
    }

    # Ensure you have the client_secrets.json in your project root or provide the full path
    uploader = GDriveUploader(service_json_path='client_secrets.json')
    
    for local_path, drive_id in TARGET_DRIVE_IDS.items():
        if os.path.exists(local_path):
            print(f"\n--- Syncing {local_path} to Drive ID: {drive_id} ---")
            uploader.upload_recursive(local_path, drive_id)
        else:
            print(f"Warning: Local path {local_path} not found. Skipping.")

    print("\n--- Google Drive Sync Completed Successfully ---")