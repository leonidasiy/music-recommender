# 🎵 Music Recommender

A privacy-first, automated music recommendation system that analyzes your Google Drive music library and sends personalized monthly recommendations via email.

## Features

- 📁 **Recursive Drive scanning** - Traverses all subfolders
- 🏷️ **ID3 tag extraction** - Reads metadata from MP3, M4A, FLAC, etc.
- 🎯 **Personalized scoring** - Based on your taste profile
- 🔗 **Dual links** - Spotify + YouTube fallback
- 🚫 **Remix exclusion** - Filters out remixes by default
- 🔒 **Privacy-first** - Credentials never leave your machine/secrets

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/YOUR_USERNAME/music-recommender.git
cd music-recommender
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

cp config.template.yaml config.yaml
# Edit config.yaml with your credentials

unalias python
python src/main.py

# Changes
# cache_utils.py: 
#     metadata_cache.json -> songs_metadata_cache.json

# main.py:         
#     drive_service = create_drive_service("credentials.json") -> 
#     service_account_json = config.get('google_service_account_json')
#     if not service_account_json:
#         raise ValueError("google_service_account_json not found in config")
#     drive_service = create_drive_service(service_account_json)

# recommender.py:
#     remove "eve" in KNOWN_ARTISTS
