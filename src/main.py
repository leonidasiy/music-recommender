#!/usr/bin/env python3
"""
Main entry point for the music recommendation pipeline.
Uses caching for efficiency.
"""

import sys
import logging
import yaml
from pathlib import Path
from typing import List

# Add src to path for imports
src_path = Path(__file__).parent
sys.path.insert(0, str(src_path))

from drive_utils import (
    create_drive_service, 
    extract_folder_id, 
    list_audio_files_recursive,
    download_file_header
)
from recommender import (
    create_spotify_client,
    extract_metadata_from_bytes,
    build_taste_profile,
    get_recommendations,
    TrackInfo,
    parse_filename,
    LibraryIndex
)
from email_utils import send_email, generate_html_email
from cache_utils import (
    MetadataCache, 
    TasteProfileCache,
    get_cache_path,
    get_profile_cache_path,
    get_rebuild_threshold
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('recommender.log')
    ]
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    paths_to_check = [
        config_path,
        Path(__file__).parent.parent / config_path,
        Path.cwd() / config_path
    ]
    
    for path in paths_to_check:
        path = Path(path)
        if path.exists():
            logger.info(f"Loading config from: {path}")
            with open(path, 'r') as f:
                return yaml.safe_load(f)
                
    raise FileNotFoundError(
        "Config file not found. Please create config.yaml from config.template.yaml"
    )


def process_audio_files_with_cache(
    drive_service,
    audio_files: list,
    cache: MetadataCache
) -> list:
    """
    Process audio files using cache for efficiency.
    Only downloads and parses files not already in cache.
    """
    tracks = []
    processed_count = 0
    cached_count = 0
    failed_count = 0
    
    total_files = len(audio_files)
    current_file_ids = {f['id'] for f in audio_files}
    
    # Remove deleted files from cache
    cache.remove_deleted_files(current_file_ids)
    
    logger.info(f"Processing {total_files} audio files...")
    logger.info(f"💡 Tip: Press Ctrl+C anytime - progress will be saved!")
    
    try:
        for i, file_info in enumerate(audio_files):
            file_id = file_info['id']
            file_name = file_info['name']
            file_path = file_info.get('path', file_name)
            file_size = file_info.get('size')
            
            # Convert size to int
            if file_size is not None:
                try:
                    file_size = int(file_size)
                except (ValueError, TypeError):
                    file_size = None
            
            # Progress logging
            if (i + 1) % 25 == 0 or i == 0:
                pct = ((i + 1) / total_files) * 100
                logger.info(f"📊 Progress: {i + 1}/{total_files} ({pct:.1f}%) "
                           f"[cached: {cached_count}, new: {processed_count}, skipped: {failed_count}]")
            
            # Check cache first
            cached_track = cache.get_cached_track(file_id, file_size)
            if cached_track:
                tracks.append(cached_track)
                cached_count += 1
                continue
            
            # Download and process
            try:
                header_bytes = download_file_header(drive_service, file_id)
                
                track = None
                if header_bytes:
                    track = extract_metadata_from_bytes(header_bytes, file_name)
                
                # Fallback to filename parsing
                if not track:
                    track = parse_filename(file_name)
                
                if track:
                    track.file_path = file_path
                    tracks.append(track)
                    cache.cache_track(file_id, file_name, file_path, file_size, track)
                    processed_count += 1
                else:
                    failed_count += 1
                    
            except Exception as e:
                logger.debug(f"Error processing {file_name}: {e}")
                failed_count += 1
                
    except KeyboardInterrupt:
        logger.info(f"\n⏸️  Interrupted. Saving progress...")
        cache.save()
        raise
    
    logger.info(f"\n{'='*50}")
    logger.info(f"✅ Processing complete:")
    logger.info(f"   📦 From cache: {cached_count}")
    logger.info(f"   🔄 Newly processed: {processed_count}")
    logger.info(f"   ⏭️  Skipped: {failed_count}")
    logger.info(f"   📁 Total tracks: {len(tracks)}")
    logger.info(f"{'='*50}")
    
    return tracks


def build_library_index(tracks: List[TrackInfo], track_ids: List[str]) -> LibraryIndex:
    """Build library index from tracks and Spotify IDs."""
    library_index = LibraryIndex()
    
    for track in tracks:
        library_index.add_track(track)
    
    for track_id in track_ids:
        library_index.add_spotify_track_id(track_id)
    
    return library_index


def get_or_build_taste_profile(
    tracks: list,
    spotify,
    profile_cache: TasteProfileCache
) -> tuple:
    """Get taste profile from cache or build it fresh."""
    
    # Show current vs cached track counts
    current_count = len(tracks)
    cached_count = profile_cache.get_cached_track_count()
    
    if cached_count > 0:
        diff = abs(current_count - cached_count)
        logger.info(f"📊 Library: {current_count} tracks (cached: {cached_count}, diff: {diff})")
    
    if profile_cache.is_valid_for_library(tracks):
        cached = profile_cache.get_cached_profile()
        if cached:
            genre_weights, artist_ids, track_ids = cached
            logger.info(f"📦 Using cached taste profile:")
            logger.info(f"   Genres: {len(genre_weights)}")
            logger.info(f"   Artists: {len(artist_ids)}")
            logger.info(f"   Tracks: {len(track_ids)}")
            
            top_genres = profile_cache.get_top_genres(5)
            if top_genres:
                logger.info(f"   Top genres: {', '.join(top_genres)}")
            
            # Build library index from cached data + current tracks
            library_index = build_library_index(tracks, track_ids)
            
            return genre_weights, artist_ids, track_ids, library_index
    
    logger.info("🔄 Building fresh taste profile...")
    
    genre_weights, artist_ids, track_ids, top_artists, library_index = build_taste_profile(tracks, spotify)
    
    profile_cache.cache_profile(
        tracks=tracks,
        genre_weights=genre_weights,
        artist_ids=artist_ids,
        track_ids=track_ids,
        top_artists=top_artists
    )
    
    return genre_weights, artist_ids, track_ids, library_index


def main():
    """Main pipeline execution."""
    logger.info("=" * 60)
    logger.info("Starting Music Recommendation Pipeline")
    logger.info("=" * 60)
    
    try:
        # Load configuration
        config = load_config()
        logger.info("Configuration loaded successfully")
        
        # Extract settings
        drive_url = config['drive_folder_url']
        spotify_client_id = config['spotify_client_id']
        spotify_client_secret = config['spotify_client_secret']
        email_config = config['email']
        settings = config.get('settings', {})
        weights = config.get('weights', {
            'tag_similarity': 0.60,
            'artist_affinity': 0.25,
            'popularity': 0.15
        })
        
        # Initialize caches with threshold from config
        metadata_cache = MetadataCache(get_cache_path(config))
        rebuild_threshold = get_rebuild_threshold(config)
        profile_cache = TasteProfileCache(
            cache_path=get_profile_cache_path(config),
            rebuild_threshold=rebuild_threshold
        )
        
        logger.info(f"Profile rebuild threshold: {rebuild_threshold} tracks")
        
        # Initialize Google Drive
        logger.info("Initializing Google Drive service...")
        service_account_json = config.get('google_service_account_json')
        if not service_account_json:
            raise ValueError("google_service_account_json not found in config")
        drive_service = create_drive_service(service_account_json)
        folder_id = extract_folder_id(drive_url)
        logger.info(f"Target folder ID: {folder_id}")
        
        # Initialize Spotify
        logger.info("Initializing Spotify client...")
        spotify = create_spotify_client(spotify_client_id, spotify_client_secret)
        
        # List audio files from Drive
        logger.info("Scanning Drive folder for audio files...")
        audio_files = list(list_audio_files_recursive(drive_service, folder_id))
        logger.info(f"Found {len(audio_files)} audio files")
        
        if not audio_files:
            logger.error("No audio files found in the specified folder")
            sys.exit(1)
        
        # Process files with caching
        tracks = process_audio_files_with_cache(drive_service, audio_files, metadata_cache)
        
        # Save metadata cache
        metadata_cache.save()
        
        if not tracks:
            logger.error("Could not extract metadata from any files")
            sys.exit(1)
        
        # Get or build taste profile
        user_genres, user_artist_ids, user_track_ids, library_index = get_or_build_taste_profile(
            tracks, spotify, profile_cache
        )
        
        # Log cache stats
        metadata_stats = metadata_cache.get_stats()
        logger.info(f"📊 Metadata cache: {metadata_stats['cache_hits']} hits, "
                   f"{metadata_stats['cache_misses']} misses, "
                   f"{metadata_stats['hit_rate']:.1%} hit rate")
        
        # Log library index stats
        index_stats = library_index.get_stats()
        logger.info(f"📚 Library index: {index_stats['spotify_ids']} Spotify IDs, "
                   f"{index_stats['unique_titles']} unique titles")
        
        # Generate recommendations
        logger.info("Generating personalized recommendations...")
        
        recommendations = get_recommendations(
            spotify=spotify,
            track_ids=user_track_ids,
            artist_ids=user_artist_ids,
            user_genres=user_genres,
            library_index=library_index,
            settings=settings,
            weights=weights
        )
        
        if not recommendations:
            logger.warning("No recommendations generated")
            sys.exit(1)
            
        logger.info(f"Generated {len(recommendations)} recommendations")
        
        # Log top recommendations
        logger.info("🎵 Top 5 recommendations:")
        for i, rec in enumerate(recommendations[:5], 1):
            logger.info(f"   {i}. {rec.artist} - {rec.title} (score: {rec.score:.2f})")
        
        # Prepare stats
        profile_stats = profile_cache.get_stats() or {}
        pipeline_stats = {
            'total_files': len(audio_files),
            'tracks_parsed': len(tracks),
            'genres_found': len(user_genres),
            'artists_found': len(user_artist_ids),
            'tracks_on_spotify': profile_stats.get('tracks_found_on_spotify', len(user_track_ids)),
            'cache_hits': metadata_stats['cache_hits'],
            'cache_misses': metadata_stats['cache_misses']
        }
        
        # Save HTML output locally
        out_dir = Path(__file__).parent.parent / 'out'
        out_dir.mkdir(exist_ok=True)
        
        html_output = generate_html_email(recommendations, pipeline_stats)
        output_file = out_dir / 'recommendations.html'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_output)
        logger.info(f"Saved HTML output to: {output_file}")
        
        # Send email
        logger.info("Sending recommendation email...")
        email_sent = send_email(
            recommendations=recommendations,
            stats=pipeline_stats,
            sender=email_config['sender'],
            to=email_config['to'],
            smtp_user=email_config['smtp_user'],
            smtp_password=email_config['smtp_app_password']
        )
        
        if email_sent:
            logger.info("=" * 60)
            logger.info("✅ Pipeline completed successfully!")
            logger.info("=" * 60)
        else:
            logger.warning("⚠️ Pipeline completed but email failed to send")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\n👋 Exiting. All progress has been saved!")
        sys.exit(0)
        
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
        
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        sys.exit(1)
        
    except Exception as e:
        logger.exception(f"Pipeline failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()