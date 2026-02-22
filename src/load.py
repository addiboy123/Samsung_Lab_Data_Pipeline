import os
from datetime import datetime
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

class GDriveUploader:
    def __init__(self):
        # 1. Use AIRFLOW_HOME if set, otherwise fallback to current dir
        project_root = os.getenv('AIRFLOW_HOME', os.getcwd())
        secrets_path = os.path.join(project_root, 'client_secrets.json')
        creds_path = os.path.join(project_root, 'mycreds.txt')

        gauth = GoogleAuth()
        gauth.settings['client_config_file'] = secrets_path

        # 2. SILENT LOAD: This part runs inside Airflow
        if os.path.exists(creds_path):
            gauth.LoadCredentialsFile(creds_path)
        else:
            raise FileNotFoundError(f"Missing 'mycreds.txt' at {creds_path}. Run this script manually first!")

        if gauth.access_token_expired:
            gauth.Refresh()
        else:
            gauth.Authorize()
            
        self.drive = GoogleDrive(gauth)

    def get_or_create_folder(self, folder_name, parent_id):
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
        query = f"'{parent_id}' in parents and trashed = false"
        existing_items = {f['title']: f['id'] for f in self.drive.ListFile({'q': query}).GetList()}

        for item in os.listdir(local_path):
            local_item_path = os.path.join(local_path, item)
            
            if os.path.isdir(local_item_path):
                subfolder_id = self.get_or_create_folder(item, parent_id)
                self.upload_recursive(local_item_path, subfolder_id)
            else:
                if item not in existing_items:
                    print(f"Uploading: {item}")
                    gfile = self.drive.CreateFile({'title': item, 'parents': [{'id': parent_id}]})
                    gfile.SetContentFile(local_item_path)
                    gfile.Upload()
                else:
                    print(f"Skipping (exists): {item}")

def upload_to_gdrive():
    project_root = os.getenv('AIRFLOW_HOME', os.getcwd())
    
    # Get today's date for features and plots
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    # Define your folders using the IDs (not URLs) from your previous steps
    # Note: features_extracted and plots are stored in dated subdirectories
    TARGET_DRIVE_IDS = {
        os.path.join(project_root, "etl/organized_data"): "1eNLj2mImpRbZhbCpwW2A_L9xVVcY_wuw",
        os.path.join(project_root, "etl/phase_segmented"): "12B0c6yaGuABtpXX4ezFU9EcxPKmo1pqF",
        os.path.join(project_root, "etl/features_extracted"): "1nRegir8GcKg_LwCld0irH2pkUhqzLiuF",  # Replace with actual ID
        os.path.join(project_root, "etl/plots"): "1lEXM0I5A5GWrwEhbDG-2YPZMOcqcLIkL"  # Replace with actual ID
    }

    uploader = GDriveUploader()
    
    for local_path, drive_id in TARGET_DRIVE_IDS.items():
        if os.path.exists(local_path):
            print(f"\n--- Syncing {local_path} ---")
            uploader.upload_recursive(local_path, drive_id)
        else:
            print(f"\n⚠️  Skipping {local_path} (directory not found)")

    print("\n--- Google Drive Sync Completed Successfully ---")



# ==========================================
# THE MANUAL SETUP BLOCK
# ==========================================

if __name__ == "__main__":
    project_root = os.getcwd() 
    secrets_path = os.path.join(project_root, 'client_secrets.json')
    creds_path = os.path.join(project_root, 'mycreds.txt')

    gauth = GoogleAuth()
    
    # 1. Set the client secrets path directly
    gauth.settings['client_config_file'] = secrets_path
    gauth.settings['client_config_backend'] = 'file'

    # 2. Check for the file and run the handshake
    if not os.path.exists(creds_path):
        print(f"\n--- INITIAL SETUP: STARTING HANDSHAKE ---")
        # BYPASS: Use LocalWebserverAuth if CommandLineAuth keeps looping on config
        # If you are on your local machine, this is actually more reliable
        try:
            gauth.LocalWebserverAuth() 
        except Exception:
            # Fallback to Command Line if browser fails
            gauth.CommandLineAuth()
            
        # 3. Save explicitly to the path
        gauth.SaveCredentialsFile(creds_path)
        print(f"\nSUCCESS: Credentials saved to {creds_path}")
    else:
        print(f"✅ Credentials already exist at {creds_path}.")