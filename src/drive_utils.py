"""
Google Drive utilities for listing and reading music files.
Supports recursive folder traversal and header-only downloads for ID3 tags.
"""

import io
import json
import logging
from typing import Generator, Dict, Any, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger(__name__)

# Supported audio formats
AUDIO_EXTENSIONS = {'.mp3', '.m4a', '.flac', '.wav', '.ogg', '.opus', '.aac', '.wma'}
AUDIO_MIMETYPES = {
    'audio/mpeg', 'audio/mp4', 'audio/x-m4a', 'audio/flac', 
    'audio/wav', 'audio/ogg', 'audio/opus', 'audio/aac',
    'audio/x-ms-wma', 'application/octet-stream'
}


def extract_folder_id(folder_url: str) -> str:
    """Extract folder ID from Google Drive URL."""
    # Handle various URL formats
    if '/folders/' in folder_url:
        folder_id = folder_url.split('/folders/')[-1].split('?')[0].split('/')[0]
    elif 'id=' in folder_url:
        folder_id = folder_url.split('id=')[-1].split('&')[0]
    else:
        # Assume it's already a folder ID
        folder_id = folder_url.strip()
    
    return folder_id


def create_drive_service(service_account_json: str):
    """Create Google Drive API service from service account JSON."""
    try:
        # Parse JSON (handles both raw JSON string and escaped JSON)
        if isinstance(service_account_json, str):
            creds_dict = json.loads(service_account_json)
        else:
            creds_dict = service_account_json
            
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Google Drive service created successfully")
        return service
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid service account JSON: {e}")
        raise ValueError(f"Failed to parse service account JSON: {e}")


def is_audio_file(file_info: Dict[str, Any]) -> bool:
    """Check if a file is an audio file based on extension and MIME type."""
    name = file_info.get('name', '').lower()
    mime_type = file_info.get('mimeType', '')
    
    # Check extension
    has_audio_ext = any(name.endswith(ext) for ext in AUDIO_EXTENSIONS)
    
    # Check MIME type
    has_audio_mime = mime_type in AUDIO_MIMETYPES or mime_type.startswith('audio/')
    
    return has_audio_ext or has_audio_mime


def list_audio_files_recursive(
    service, 
    folder_id: str, 
    path: str = ""
) -> Generator[Dict[str, Any], None, None]:
    """
    Recursively list all audio files in a Google Drive folder.
    
    Yields:
        Dict with keys: id, name, mimeType, size, path
    """
    page_token = None
    
    while True:
        try:
            # Query for files in this folder
            query = f"'{folder_id}' in parents and trashed = false"
            
            response = service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType, size)',
                pageToken=page_token,
                pageSize=1000
            ).execute()
            
            files = response.get('files', [])
            
            for file_info in files:
                file_path = f"{path}/{file_info['name']}" if path else file_info['name']
                
                # If it's a folder, recurse into it
                if file_info['mimeType'] == 'application/vnd.google-apps.folder':
                    logger.debug(f"Entering subfolder: {file_path}")
                    yield from list_audio_files_recursive(
                        service, 
                        file_info['id'], 
                        file_path
                    )
                    
                # If it's an audio file, yield it
                elif is_audio_file(file_info):
                    file_info['path'] = file_path
                    logger.debug(f"Found audio file: {file_path}")
                    yield file_info
                    
            page_token = response.get('nextPageToken')
            if not page_token:
                break
                
        except Exception as e:
            logger.error(f"Error listing files in folder {folder_id}: {e}")
            break


def download_file_header(service, file_id: str, bytes_to_read: int = 65536) -> Optional[bytes]:
    """
    Download only the first N bytes of a file (enough for ID3 tags).
    
    Args:
        service: Google Drive service
        file_id: File ID to download
        bytes_to_read: Number of bytes to read (default 64KB, enough for most ID3 headers)
        
    Returns:
        Bytes of file header, or None on error
    """
    try:
        request = service.files().get_media(fileId=file_id)
        
        # Add range header to only get first N bytes
        request.headers['Range'] = f'bytes=0-{bytes_to_read - 1}'
        
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        
        done = False
        while not done:
            _, done = downloader.next_chunk()
            
        buffer.seek(0)
        return buffer.read()
        
    except Exception as e:
        logger.warning(f"Failed to download header for file {file_id}: {e}")
        return None


def download_full_file(service, file_id: str) -> Optional[bytes]:
    """
    Download a complete file from Google Drive.
    Use sparingly - prefer download_file_header for ID3 extraction.
    
    Args:
        service: Google Drive service
        file_id: File ID to download
        
    Returns:
        File bytes, or None on error
    """
    try:
        request = service.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logger.debug(f"Download progress: {int(status.progress() * 100)}%")
                
        buffer.seek(0)
        return buffer.read()
        
    except Exception as e:
        logger.error(f"Failed to download file {file_id}: {e}")
        return None