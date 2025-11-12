# Databricks notebook source
# Databricks cell
%pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib


# COMMAND ----------

dbutils.widgets.text('catalog_name','')
dbutils.widgets.text('folder_id','')
catalog_name=dbutils.widgets.get('catalog_name')
folder_id=dbutils.widgets.get('folder_id')

# COMMAND ----------

# DBTITLE 1,Second time onwards/Incremental run
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import os
import io
from datetime import datetime,timezone

# --- 1. Configs ---
SERVICE_ACCOUNT_FILE = f"/Volumes/{catalog_name}/staging/p2p_files/metadata/service_account.json"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

VOLUME_BASE_PATH = f"/Volumes/{catalog_name}/staging/p2p_files"  # REPLACE

# --- 2. Auth ---
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

# --- 3. Helper: Get folder path by tracing parents ---
# def get_full_path(file_id, drive_service, path_cache={}):
#     if file_id in path_cache:
#         return path_cache[file_id]
    
#     file = drive_service.files().get(fileId=file_id, fields='id, name, parents, mimeType').execute()
#     if 'parents' in file:
#         parent_path = get_full_path(file['parents'][0], drive_service, path_cache)
#         full_path = os.path.join(parent_path, file['name'])
#     else:
#         full_path = file['name']  # root level
#     path_cache[file_id] = full_path
#     return full_path

# --- 4. Recursively list all PDFs with folder path ---
def list_all_files(folder_id, drive_service, parent_path=""):
    query = f"'{folder_id}' in parents and trashed = false"
    files = drive_service.files().list(q=query, fields="files(id, name, mimeType,modifiedTime)").execute().get('files', [])
    results = []
    for f in files:
        if f['mimeType'] == 'application/pdf' or f['mimeType']=='text/csv':
            results.append({
                "id": f['id'],
                "name": f['name'],
                "modifiedTime":f['modifiedTime'],
                "full_path": os.path.join(parent_path, f['name'])
            })
        elif f['mimeType'] == 'application/vnd.google-apps.folder':
            new_path = os.path.join(parent_path, f['name'])
            results.extend(list_all_files(f['id'], drive_service, new_path))
    return results

# --- 5. Download and Write to Volume ---
def read_files_as_binary(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()
    buffer.seek(0)
    return buffer.read()

# --- 6. Main Execution ---
pdf_files = list_all_files(f"{folder_id}", drive_service)  # or provide a folder ID
for pdf in pdf_files:
    destination_path = os.path.join(VOLUME_BASE_PATH, pdf['full_path'])
    destination_dir = os.path.dirname(destination_path)
    file_name = os.path.basename(destination_path)
    # List files in the parent directory
    files_in_dir = dbutils.fs.ls(destination_dir)
    file_info = [f for f in files_in_dir if f.name.rstrip('/') == file_name]
    if file_info:
        modificationTime_sink = datetime.fromtimestamp(file_info[0].modificationTime / 1000, tz=timezone.utc)
        modificationTime_source = datetime.strptime(pdf['modifiedTime'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        if modificationTime_source > modificationTime_sink:
            print(f"Downloading: {pdf['full_path']}")
            binary_data = read_files_as_binary(pdf['id'])
            os.makedirs(destination_dir, exist_ok=True)
            with open(destination_path, "wb") as f:
                f.write(binary_data)
                print(f"✅ Saved to: {destination_path}")
        else:
            print(f"No New file arrived in source")
    else:
        # File does not exist, download it
        print(f"File not found, downloading: {pdf['full_path']}")
        binary_data = read_files_as_binary(pdf['id'])
        os.makedirs(destination_dir, exist_ok=True)
        with open(destination_path, "wb") as f:
            f.write(binary_data)
            print(f"✅ Saved to: {destination_path}")

# COMMAND ----------

# DBTITLE 1,First time read
# MAGIC %skip
# MAGIC from google.oauth2 import service_account
# MAGIC from googleapiclient.discovery import build
# MAGIC from googleapiclient.http import MediaIoBaseDownload
# MAGIC import os
# MAGIC import io
# MAGIC from datetime import datetime,timezone
# MAGIC
# MAGIC # --- 1. Configs ---
# MAGIC SERVICE_ACCOUNT_FILE = f"/Volumes/{catalog_name}/staging/p2p_files/metadata/service_account.json"
# MAGIC SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
# MAGIC
# MAGIC VOLUME_BASE_PATH = f"/Volumes/{catalog_name}/staging/p2p_files"  # REPLACE
# MAGIC
# MAGIC # --- 2. Auth ---
# MAGIC creds = service_account.Credentials.from_service_account_file(
# MAGIC     SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# MAGIC drive_service = build('drive', 'v3', credentials=creds)
# MAGIC
# MAGIC # --- 3. Helper: Get folder path by tracing parents ---
# MAGIC # def get_full_path(file_id, drive_service, path_cache={}):
# MAGIC #     if file_id in path_cache:
# MAGIC #         return path_cache[file_id]
# MAGIC     
# MAGIC #     file = drive_service.files().get(fileId=file_id, fields='id, name, parents, mimeType').execute()
# MAGIC #     if 'parents' in file:
# MAGIC #         parent_path = get_full_path(file['parents'][0], drive_service, path_cache)
# MAGIC #         full_path = os.path.join(parent_path, file['name'])
# MAGIC #     else:
# MAGIC #         full_path = file['name']  # root level
# MAGIC #     path_cache[file_id] = full_path
# MAGIC #     return full_path
# MAGIC
# MAGIC # --- 4. Recursively list all PDFs with folder path ---
# MAGIC def list_all_files(folder_id, drive_service, parent_path=""):
# MAGIC     query = f"'{folder_id}' in parents and trashed = false"
# MAGIC     files = drive_service.files().list(q=query, fields="files(id, name, mimeType,modifiedTime)").execute().get('files', [])
# MAGIC     results = []
# MAGIC     for f in files:
# MAGIC         if f['mimeType'] == 'application/pdf' or f['mimeType']=='text/csv':
# MAGIC             results.append({
# MAGIC                 "id": f['id'],
# MAGIC                 "name": f['name'],
# MAGIC                 "modifiedTime":f['modifiedTime'],
# MAGIC                 "full_path": os.path.join(parent_path, f['name'])
# MAGIC             })
# MAGIC         elif f['mimeType'] == 'application/vnd.google-apps.folder':
# MAGIC             new_path = os.path.join(parent_path, f['name'])
# MAGIC             results.extend(list_all_files(f['id'], drive_service, new_path))
# MAGIC     return results
# MAGIC
# MAGIC # --- 5. Download and Write to Volume ---
# MAGIC def read_files_as_binary(file_id):
# MAGIC     request = drive_service.files().get_media(fileId=file_id)
# MAGIC     buffer = io.BytesIO()
# MAGIC     downloader = MediaIoBaseDownload(buffer, request)
# MAGIC
# MAGIC     done = False
# MAGIC     while not done:
# MAGIC         _, done = downloader.next_chunk()
# MAGIC     buffer.seek(0)
# MAGIC     return buffer.read()
# MAGIC
# MAGIC # --- 6. Main Execution ---
# MAGIC pdf_files = list_all_files(f"{folder_id}", drive_service)  # or provide a folder ID
# MAGIC for pdf in pdf_files:
# MAGIC     destination_path = os.path.join(VOLUME_BASE_PATH, pdf['full_path'])
# MAGIC     print(f"Downloading: {pdf['full_path']}")
# MAGIC     binary_data = read_files_as_binary(pdf['id'])
# MAGIC         # # Build destination path in volume
# MAGIC     destination_path = os.path.join(VOLUME_BASE_PATH, pdf['full_path'])
# MAGIC     destination_dir = os.path.dirname(destination_path)
# MAGIC         # Create folders if not exist
# MAGIC     os.makedirs(destination_dir, exist_ok=True)
# MAGIC     with open(destination_path, "wb") as f:
# MAGIC         f.write(binary_data)
# MAGIC         print(f"✅ Saved to: {destination_path}")
# MAGIC     # modificationTime=[i.modificationTime for i in dbutils.fs.ls(f'{destination_path}')][0]
# MAGIC     # modificationTime_sink = datetime.fromtimestamp(modificationTime / 1000, tz=timezone.utc)
# MAGIC     # modificationTime_source = datetime.strptime(pdf['modifiedTime'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
# MAGIC     # if(modificationTime_source>modificationTime_sink):
# MAGIC     #     print(f"Downloading: {pdf['full_path']}")
# MAGIC     #     binary_data = read_files_as_binary(pdf['id'])
# MAGIC     #     # # Build destination path in volume
# MAGIC     #     destination_path = os.path.join(VOLUME_BASE_PATH, pdf['full_path'])
# MAGIC     #     destination_dir = os.path.dirname(destination_path)
# MAGIC     #     # Create folders if not exist
# MAGIC     #     os.makedirs(destination_dir, exist_ok=True)
# MAGIC     #     with open(destination_path, "wb") as f:
# MAGIC     #         f.write(binary_data)
# MAGIC     #         print(f"✅ Saved to: {destination_path}")
# MAGIC     # else:
# MAGIC     #     print(f"No New file arrived in source")
# MAGIC
