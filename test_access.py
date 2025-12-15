import json
import yaml
from google.oauth2 import service_account
from googleapiclient.discovery import build

with open("config.yaml") as f:
    config = yaml.safe_load(f)

creds = json.loads(config["google_service_account_json"])
print(f"\n🔑 Service Account Email:")
print(f"   {creds['client_email']}")
print(f"\n⚠️  Share your Google Drive folder with this email!\n")

credentials = service_account.Credentials.from_service_account_info(
    creds, scopes=['https://www.googleapis.com/auth/drive.readonly']
)
service = build('drive', 'v3', credentials=credentials)

folder_url = config["drive_folder_url"]
folder_id = folder_url.split('/folders/')[-1].split('?')[0] if '/folders/' in folder_url else folder_url

print(f"📁 Folder ID: {folder_id}\n")

try:
    # Try to get folder metadata
    folder_meta = service.files().get(fileId=folder_id, fields="name,mimeType").execute()
    print(f"✅ Folder accessible: {folder_meta['name']}\n")
except Exception as e:
    print(f"❌ Cannot access folder: {e}")
    print(f"\n👉 Make sure you shared the folder with the service account email above!")
    exit(1)

# List contents
results = service.files().list(
    q=f"'{folder_id}' in parents and trashed = false",
    fields="files(id, name, mimeType)"
).execute()

files = results.get('files', [])
print(f"📄 Items in folder: {len(files)}\n")

for f in files:
    emoji = "📁" if f['mimeType'] == 'application/vnd.google-apps.folder' else "🎵" if 'audio' in f['mimeType'] else "📄"
    print(f"   {emoji} {f['name']} ({f['mimeType']})")
