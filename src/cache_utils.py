"""
Caching utilities for metadata and taste profile.
Stores extracted track metadata and taste profile to avoid re-processing.
"""

import json
import logging
import signal
import atexit
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Set, Tuple

from recommender import TrackInfo

logger = logging.getLogger(__name__)

CACHE_VERSION = "1.0"
PROFILE_VERSION = "1.1"  # Bumped version for threshold logic
DEFAULT_CACHE_FILE = "songs_metadata_cache.json"
DEFAULT_PROFILE_FILE = "taste_profile_cache.json"
DEFAULT_REBUILD_THRESHOLD = 75  # Only rebuild if track count differs by this much

# Global reference for signal handler
_active_caches = []


def _save_on_exit():
    """Save all caches when program exits."""
    global _active_caches
    for cache in _active_caches:
        if cache is not None and hasattr(cache, 'save'):
            try:
                cache.save()
            except Exception as e:
                logger.error(f"Failed to save cache on exit: {e}")


def _signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    global _active_caches
    logger.info("\n⚠️  Interrupt received! Saving caches...")
    for cache in _active_caches:
        if cache is not None and hasattr(cache, 'save'):
            try:
                cache.save()
                logger.info(f"✅ Saved: {cache.cache_path}")
            except Exception as e:
                logger.error(f"Failed to save cache: {e}")
    raise KeyboardInterrupt


def _register_cache(cache):
    """Register a cache for auto-save on exit."""
    global _active_caches
    if cache not in _active_caches:
        _active_caches.append(cache)
        
        # Only register handlers once
        if len(_active_caches) == 1:
            atexit.register(_save_on_exit)
            signal.signal(signal.SIGINT, _signal_handler)
            signal.signal(signal.SIGTERM, _signal_handler)


# =============================================================================
# METADATA CACHE
# =============================================================================

class MetadataCache:
    """Cache for storing extracted music metadata."""
    
    def __init__(self, cache_path: str = DEFAULT_CACHE_FILE, auto_save_interval: int = 50):
        self.cache_path = Path(cache_path)
        self.auto_save_interval = auto_save_interval
        self.data = self._load_cache()
        self._hits = 0
        self._misses = 0
        self._new_entries = 0
        self._dirty = False
        
        _register_cache(self)
        
    def _load_cache(self) -> Dict[str, Any]:
        """Load cache from disk or create empty cache."""
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                if data.get('version') != CACHE_VERSION:
                    logger.warning("Cache version mismatch. Creating new cache.")
                    return self._empty_cache()
                    
                track_count = len(data.get('tracks', {}))
                logger.info(f"📦 Loaded metadata cache: {track_count} tracks")
                return data
                
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load cache: {e}. Creating new cache.")
                return self._empty_cache()
        else:
            logger.info("No existing metadata cache found.")
            return self._empty_cache()
            
    def _empty_cache(self) -> Dict[str, Any]:
        return {
            "version": CACHE_VERSION,
            "last_updated": None,
            "tracks": {}
        }
        
    def save(self, force: bool = False) -> None:
        if not self._dirty and not force:
            return
            
        self.data["last_updated"] = datetime.now().isoformat()
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        temp_path = self.cache_path.with_suffix('.tmp')
        
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            
            temp_path.replace(self.cache_path)
            
            track_count = len(self.data.get('tracks', {}))
            logger.info(f"💾 Saved metadata cache: {track_count} tracks")
            self._dirty = False
            self._new_entries = 0
            
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
            if temp_path.exists():
                temp_path.unlink()
        
    def _maybe_auto_save(self) -> None:
        if self._new_entries >= self.auto_save_interval:
            logger.info(f"Auto-saving metadata cache ({self._new_entries} new entries)...")
            self.save()
        
    def get_cached_track(self, file_id: str, file_size: Optional[int] = None) -> Optional[TrackInfo]:
        cached = self.data["tracks"].get(file_id)
        
        if not cached:
            self._misses += 1
            return None
            
        if file_size is not None and cached.get("file_size") != file_size:
            self._misses += 1
            return None
            
        metadata = cached.get("metadata", {})
        
        if not metadata.get("title") or not metadata.get("artist"):
            self._misses += 1
            return None
            
        self._hits += 1
        
        return TrackInfo(
            title=metadata.get("title", ""),
            artist=metadata.get("artist", ""),
            album=metadata.get("album"),
            genre=metadata.get("genre"),
            year=metadata.get("year"),
            file_path=cached.get("file_path")
        )
        
    def cache_track(
        self, 
        file_id: str, 
        file_name: str, 
        file_path: str,
        file_size: Optional[int],
        track: TrackInfo
    ) -> None:
        self.data["tracks"][file_id] = {
            "file_id": file_id,
            "file_name": file_name,
            "file_path": file_path,
            "file_size": file_size,
            "cached_at": datetime.now().isoformat(),
            "metadata": {
                "title": track.title,
                "artist": track.artist,
                "album": track.album,
                "genre": track.genre,
                "year": track.year
            }
        }
        
        self._dirty = True
        self._new_entries += 1
        self._maybe_auto_save()
        
    def get_all_cached_tracks(self) -> List[TrackInfo]:
        tracks = []
        for file_id, cached in self.data["tracks"].items():
            metadata = cached.get("metadata", {})
            if metadata.get("title") and metadata.get("artist"):
                track = TrackInfo(
                    title=metadata["title"],
                    artist=metadata["artist"],
                    album=metadata.get("album"),
                    genre=metadata.get("genre"),
                    year=metadata.get("year"),
                    file_path=cached.get("file_path")
                )
                tracks.append(track)
        return tracks
        
    def remove_deleted_files(self, current_file_ids: Set[str]) -> int:
        cached_ids = set(self.data["tracks"].keys())
        deleted_ids = cached_ids - current_file_ids
        
        for file_id in deleted_ids:
            del self.data["tracks"][file_id]
            
        if deleted_ids:
            logger.info(f"Removed {len(deleted_ids)} deleted files from cache")
            self._dirty = True
            
        return len(deleted_ids)
        
    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_cached": len(self.data["tracks"]),
            "cache_hits": self._hits,
            "cache_misses": self._misses,
            "hit_rate": self._hits / (self._hits + self._misses) if (self._hits + self._misses) > 0 else 0,
            "last_updated": self.data.get("last_updated"),
            "unsaved_entries": self._new_entries
        }
        
    def clear(self) -> None:
        self.data = self._empty_cache()
        self._dirty = True
        logger.info("Metadata cache cleared")


# =============================================================================
# TASTE PROFILE CACHE
# =============================================================================

class TasteProfileCache:
    """
    Cache for storing the built taste profile.
    
    The profile is invalidated when:
    - Track count differs by more than rebuild_threshold (default: 75)
    - Cache version changes
    
    Note: Small changes (< threshold) will use the cached profile,
    which is good enough for recommendations.
    """
    
    def __init__(
        self, 
        cache_path: str = DEFAULT_PROFILE_FILE,
        rebuild_threshold: int = DEFAULT_REBUILD_THRESHOLD
    ):
        self.cache_path = Path(cache_path)
        self.rebuild_threshold = rebuild_threshold
        self.data = self._load_cache()
        self._dirty = False
        
        _register_cache(self)
        
    def _load_cache(self) -> Dict[str, Any]:
        """Load cache from disk or create empty cache."""
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                if data.get('version') != PROFILE_VERSION:
                    logger.warning("Profile cache version mismatch. Will rebuild.")
                    return self._empty_cache()
                    
                logger.info(f"📦 Loaded taste profile cache")
                return data
                
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load profile cache: {e}")
                return self._empty_cache()
        else:
            logger.info("No existing taste profile cache found.")
            return self._empty_cache()
            
    def _empty_cache(self) -> Dict[str, Any]:
        return {
            "version": PROFILE_VERSION,
            "created_at": None,
            "library_fingerprint": None,
            "profile": None,
            "stats": None
        }
        
    def save(self, force: bool = False) -> None:
        """Save profile cache to disk."""
        if not self._dirty and not force:
            return
            
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        temp_path = self.cache_path.with_suffix('.tmp')
        
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            
            temp_path.replace(self.cache_path)
            logger.info(f"💾 Saved taste profile cache")
            self._dirty = False
            
        except Exception as e:
            logger.error(f"Failed to save profile cache: {e}")
            if temp_path.exists():
                temp_path.unlink()
    
    @staticmethod
    def _compute_library_fingerprint(tracks: List[TrackInfo]) -> Dict[str, Any]:
        """Compute a fingerprint of the track library."""
        track_strings = sorted([
            f"{t.artist.lower().strip()}|{t.title.lower().strip()}"
            for t in tracks
        ])
        
        combined = "\n".join(track_strings)
        hash_value = hashlib.sha256(combined.encode('utf-8')).hexdigest()[:16]
        
        return {
            "track_count": len(tracks),
            "hash": hash_value
        }
    
    def is_valid_for_library(self, tracks: List[TrackInfo]) -> bool:
        """
        Check if cached profile is still valid for the current library.
        
        Returns True if:
        - Cache exists
        - Track count difference is within threshold (default: 75)
        
        Note: We use a threshold-based approach because:
        - Small library changes don't significantly affect taste profile
        - Avoids expensive rebuilds for minor additions/deletions
        - The profile is still accurate enough for recommendations
        """
        if self.data.get("profile") is None:
            logger.info("No cached profile found")
            return False
            
        if self.data.get("library_fingerprint") is None:
            logger.info("No library fingerprint in cache")
            return False
            
        cached_fingerprint = self.data["library_fingerprint"]
        cached_count = cached_fingerprint.get("track_count", 0)
        current_count = len(tracks)
        
        # Calculate the difference
        count_diff = abs(current_count - cached_count)
        
        if count_diff >= self.rebuild_threshold:
            logger.info(f"Library changed significantly: {cached_count} → {current_count} tracks "
                       f"(diff: {count_diff} >= threshold: {self.rebuild_threshold})")
            return False
        
        if count_diff > 0:
            logger.info(f"Library changed slightly: {cached_count} → {current_count} tracks "
                       f"(diff: {count_diff} < threshold: {self.rebuild_threshold})")
            logger.info(f"✅ Using cached taste profile (within threshold)")
        else:
            logger.info(f"✅ Taste profile cache is valid ({current_count} tracks)")
            
        return True
        
    def get_cached_profile(self) -> Optional[Tuple[Dict[str, float], Set[str], List[str]]]:
        """Get cached taste profile."""
        profile = self.data.get("profile")
        
        if profile is None:
            return None
            
        genre_weights = profile.get("genre_weights", {})
        artist_ids = set(profile.get("artist_ids", []))
        track_ids = profile.get("track_ids", [])
        
        if not genre_weights and not artist_ids and not track_ids:
            return None
            
        return (genre_weights, artist_ids, track_ids)
        
    def cache_profile(
        self,
        tracks: List[TrackInfo],
        genre_weights: Dict[str, float],
        artist_ids: Set[str],
        track_ids: List[str],
        top_artists: Optional[List[str]] = None
    ) -> None:
        """Cache the taste profile."""
        fingerprint = self._compute_library_fingerprint(tracks)
        top_genres = sorted(genre_weights.keys(), key=lambda g: genre_weights[g], reverse=True)[:20]
        
        self.data = {
            "version": PROFILE_VERSION,
            "created_at": datetime.now().isoformat(),
            "library_fingerprint": fingerprint,
            "rebuild_threshold": self.rebuild_threshold,
            "profile": {
                "genre_weights": genre_weights,
                "artist_ids": list(artist_ids),
                "track_ids": track_ids,
                "top_artists": top_artists or [],
                "top_genres": top_genres
            },
            "stats": {
                "total_tracks_analyzed": len(tracks),
                "tracks_found_on_spotify": len(track_ids),
                "unique_artists": len(artist_ids),
                "unique_genres": len(genre_weights)
            }
        }
        
        self._dirty = True
        self.save()
        
    def get_stats(self) -> Optional[Dict[str, Any]]:
        """Get cached profile statistics."""
        return self.data.get("stats")
    
    def get_cached_track_count(self) -> int:
        """Get the track count from cached profile."""
        fingerprint = self.data.get("library_fingerprint")
        if fingerprint:
            return fingerprint.get("track_count", 0)
        return 0
        
    def get_top_genres(self, n: int = 10) -> List[str]:
        """Get top N genres from cached profile."""
        profile = self.data.get("profile")
        if profile:
            return profile.get("top_genres", [])[:n]
        return []
        
    def get_top_artists(self, n: int = 10) -> List[str]:
        """Get top N artists from cached profile."""
        profile = self.data.get("profile")
        if profile:
            return profile.get("top_artists", [])[:n]
        return []
        
    def clear(self) -> None:
        """Clear cached profile."""
        self.data = self._empty_cache()
        self._dirty = True
        logger.info("Taste profile cache cleared")
    
    def force_rebuild(self) -> None:
        """Force a rebuild on next run by clearing the fingerprint."""
        self.data["library_fingerprint"] = None
        self._dirty = True
        self.save()
        logger.info("Taste profile will rebuild on next run")


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_cache_path(config: Dict[str, Any]) -> str:
    """Get metadata cache file path from config or use default."""
    return config.get('settings', {}).get('cache_file', DEFAULT_CACHE_FILE)


def get_profile_cache_path(config: Dict[str, Any]) -> str:
    """Get profile cache file path from config or use default."""
    return config.get('settings', {}).get('profile_cache_file', DEFAULT_PROFILE_FILE)


def get_rebuild_threshold(config: Dict[str, Any]) -> int:
    """Get the rebuild threshold from config or use default."""
    return config.get('settings', {}).get('profile_rebuild_threshold', DEFAULT_REBUILD_THRESHOLD)