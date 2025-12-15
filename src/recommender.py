"""
Music recommendation engine using Spotify API.
Features improved artist/title extraction for Asian and instrumental filenames.
"""

import io
import re
import logging
import urllib.parse
import random
from typing import List, Dict, Any, Optional, Tuple, Set
from collections import Counter
from dataclasses import dataclass, field

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import musicbrainzngs
from mutagen import File as MutagenFile
from mutagen.id3 import ID3
from mutagen.mp4 import MP4
from mutagen.flac import FLAC

logger = logging.getLogger(__name__)

# Initialize MusicBrainz
musicbrainzngs.set_useragent(
    "MusicRecommender",
    "1.0",
    "https://github.com/your-username/music-recommender"
)

# =============================================================================
# KNOWN ARTISTS DATABASE
# =============================================================================

KNOWN_ARTISTS = {
    # Japanese
    "yoasobi", "kenshi yonezu", "米津玄師", "ado", "lisa", "aimer",
    "yorushika", "yama", "vaundy", "mrs green apple", "king gnu",
    "official髭男dism", "back number", "radwimps", "one ok rock",
    "bump of chicken", "mr.children", "arashi", "ayase", "ikura",
    "fujii kaze", "藤井風", "sumika", "creepy nuts", "aimyon", "あいみょん",
    "yuki matsui", "sungha jung", "kotaro oshio", "押尾コータロー",
    "masaaki kishibe", "岸部眞明", "osamuraisan", "eddie van der meer",
    
    # Chinese
    "jay chou", "周杰倫", "周杰伦", "jj lin", "林俊傑", "林俊杰",
    "eason chan", "陳奕迅", "陈奕迅", "mayday", "五月天",
    "王力宏", "wang leehom", "蔡依林", "jolin tsai",
    "邓紫棋", "g.e.m.", "薛之谦", "joker xue", "周深", "charlie zhou",
    "许嵩", "张杰", "jason zhang", "华晨宇", "hua chenyu",
    
    # Korean
    "bts", "blackpink", "iu", "아이유", "twice", "stray kids",
    "seventeen", "txt", "aespa", "newjeans", "le sserafim",
    
    # Instrumental / OST artists
    "joe hisaishi", "久石譲", "hans zimmer", "john williams",
    "yiruma", "ludovico einaudi", "hiroyuki sawano", "澤野弘之",
    "kevin penkin", "sawano hiroyuki", "evan call",
    "shirfine", "depapepe", "masaaki kishibe",
}

# Convert to lowercase set for matching
KNOWN_ARTISTS_LOWER = {a.lower() for a in KNOWN_ARTISTS}


def is_known_artist(text: str) -> bool:
    """Check if text contains a known artist name."""
    if not text:
        return False
    text_lower = text.lower().strip()
    
    # Exact match
    if text_lower in KNOWN_ARTISTS_LOWER:
        return True
    
    # Check if any known artist is contained in the text
    for artist in KNOWN_ARTISTS_LOWER:
        if artist in text_lower or text_lower in artist:
            return True
    
    return False


def extract_known_artist(text: str) -> Optional[str]:
    """Extract a known artist name from text."""
    if not text:
        return None
    text_lower = text.lower().strip()
    
    # Find the longest matching known artist
    best_match = None
    best_len = 0
    
    for artist in KNOWN_ARTISTS:
        artist_lower = artist.lower()
        if artist_lower in text_lower:
            if len(artist) > best_len:
                best_match = artist
                best_len = len(artist)
    
    return best_match


# =============================================================================
# FILENAME CLEANING
# =============================================================================

def clean_suffix(text: str) -> str:
    """Remove common video/music suffixes from text."""
    if not text:
        return ""
    
    suffixes = [
        r'\s*[-_]?\s*official\s*(music\s*)?(video|mv|audio)',
        r'\s*[-_]?\s*music\s*video',
        r'\s*[-_]?\s*lyric[s]?\s*(video)?',
        r'\s*[-_]?\s*\bmv\b',
        r'\s*[-_]?\s*\bpv\b',
        r'\s*[-_]?\s*官方(完整)?版?\s*(mv)?',
        r'\s*[-_]?\s*歌[词詞]版?',
        r'\s*[-_]?\s*完整版',
        r'\s*[-_]?\s*高清',
        r'\s*[-_]?\s*\b(hd|hq|4k)\b',
        r'\s*[-_]?\s*lyrics?\s*\(?[a-z/]+\)?',
        r'\s*[-_]?\s*\(?\s*[kK]an/[rR]om/[eE]ng\s*\)?',
        r'\s*[-_]?\s*\(?\s*[rR]om/[kK]an/[eE]ng\s*\)?',
        r'\s*\(\s*合唱[：:].+?\)',
        r'\s*[（\(]\s*[fF]eat\.?.+?[）\)]',
        r'\s*[（\(]\s*[fF]t\.?.+?[）\)]',
        r'\s*~[^~]+~',  # Remove ~text~
        r'\s*\([fF]ingerstyle\s*[gG]uitar\)',
        r'\s*[fF]ingerstyle\s*[gG]uitar',
        r'\s*~original\s*song~',
    ]
    
    result = text
    for pattern in suffixes:
        result = re.sub(pattern, '', result, flags=re.IGNORECASE)
    
    # Clean up multiple spaces and trailing punctuation
    result = re.sub(r'\s+', ' ', result)
    result = re.sub(r'[\s\-_:]+$', '', result)
    result = re.sub(r'^[\s\-_:]+', '', result)
    
    return result.strip()


def remove_parenthetical(text: str) -> str:
    """Remove parenthetical content but keep the main text."""
    if not text:
        return ""
    
    # Remove content in various brackets
    result = re.sub(r'\s*\([^)]*\)', '', text)
    result = re.sub(r'\s*\[[^\]]*\]', '', result)
    result = re.sub(r'\s*「[^」]*」', '', result)
    result = re.sub(r'\s*『[^』]*』', '', result)
    result = re.sub(r'\s*【[^】]*】', '', result)
    
    return result.strip()


# =============================================================================
# ARTIST/TITLE EXTRACTION
# =============================================================================

def extract_from_asian_brackets(text: str) -> Optional[Tuple[str, str]]:
    """
    Extract artist and title from Asian bracket patterns.
    Pattern: Artist「Title」 or Artist【Title】
    """
    if not text:
        return None
    
    patterns = [
        r'^(.+?)[\s]*【([^】]+)】',
        r'^(.+?)[\s]*「([^」]+)」',
        r'^(.+?)[\s]*『([^』]+)』',
    ]
    
    for pattern in patterns:
        match = re.match(pattern, text)
        if match:
            artist_part = match.group(1).strip()
            title_part = match.group(2).strip()
            
            # Clean up
            artist_part = re.sub(r'\s*MV\s*$', '', artist_part, flags=re.IGNORECASE)
            artist_part = re.sub(r'\s*[-_]\s*$', '', artist_part)
            title_part = clean_suffix(title_part)
            
            # Try to extract just the artist name
            extracted = extract_known_artist(artist_part)
            if extracted:
                artist_part = extracted
            
            if artist_part and title_part:
                return (artist_part, title_part)
    
    return None


def find_best_separator(text: str) -> Optional[Tuple[str, int]]:
    """
    Find the best separator in the text.
    Returns (separator, position) or None.
    
    Priority:
    1. Last " / " (slash with spaces) - often "Title / Artist"
    2. Last " - " (dash with spaces) - often "Artist - Title" or "Title - Artist"
    """
    if not text:
        return None
    
    # Look for " / " - often indicates "Title / Artist" at the end
    slash_pos = text.rfind(' / ')
    if slash_pos > 0 and slash_pos < len(text) - 3:
        return (' / ', slash_pos)
    
    # Look for " - " (with spaces)
    dash_pos = text.rfind(' - ')
    if dash_pos > 0 and dash_pos < len(text) - 3:
        return (' - ', dash_pos)
    
    # Look for " – " (en-dash with spaces)
    endash_pos = text.rfind(' – ')
    if endash_pos > 0:
        return (' – ', endash_pos)
    
    # Look for " — " (em-dash with spaces)
    emdash_pos = text.rfind(' — ')
    if emdash_pos > 0:
        return (' — ', emdash_pos)
    
    return None


def determine_artist_title(part1: str, part2: str, separator: str) -> Tuple[str, str]:
    """
    Determine which part is artist and which is title.
    
    Logic:
    1. If one part is a known artist, that's the artist
    2. For " / " separator, usually "Title / Artist" (artist at end)
    3. For " - " separator, check if part2 looks like an artist name
    4. Default to part1=artist, part2=title
    """
    part1_clean = clean_suffix(part1)
    part2_clean = clean_suffix(part2)
    
    # Check for known artists
    part1_is_artist = is_known_artist(part1_clean)
    part2_is_artist = is_known_artist(part2_clean)
    
    if part1_is_artist and not part2_is_artist:
        artist = extract_known_artist(part1_clean) or part1_clean
        return (artist, part2_clean)
    
    if part2_is_artist and not part1_is_artist:
        artist = extract_known_artist(part2_clean) or part2_clean
        return (artist, part1_clean)
    
    # For " / " separator at the end, artist is usually at the end
    if separator == ' / ':
        return (part2_clean, part1_clean)
    
    # For " - " separator, check if part2 looks like a simple name (likely artist)
    # Artists are usually shorter and don't have colons or complex punctuation
    part2_looks_like_artist = (
        len(part2_clean) < 30 and
        ':' not in part2_clean and
        '(' not in part2_clean and
        '「' not in part2_clean and
        '【' not in part2_clean
    )
    
    part1_looks_like_artist = (
        len(part1_clean) < 30 and
        ':' not in part1_clean and
        '(' not in part1_clean and
        '「' not in part1_clean and
        '【' not in part1_clean
    )
    
    # If part2 looks more like an artist (simpler), swap
    if part2_looks_like_artist and not part1_looks_like_artist:
        return (part2_clean, part1_clean)
    
    # Check if part2 is predominantly a name (capitalized words or CJK)
    if re.match(r'^[A-Z][a-z]+(\s+[A-Z][a-z]+)*$', part2_clean):
        # part2 is a proper name like "Sungha Jung"
        return (part2_clean, part1_clean)
    
    # Default: part1 is artist, part2 is title
    return (part1_clean, part2_clean)


def parse_filename_smart(filename: str, file_path: str = None) -> Tuple[str, str]:
    """
    Smart parsing of filename to extract artist and title.
    
    Returns:
        Tuple of (artist, title). Artist may be "Unknown" if not determinable.
    """
    if not filename:
        return ("Unknown", filename or "Unknown")
    
    # Remove file extension
    name = re.sub(r'\.(mp3|m4a|flac|wav|ogg|opus|aac|wma)$', '', filename, flags=re.I)
    original_name = name
    
    # Try Asian bracket patterns first
    bracket_result = extract_from_asian_brackets(name)
    if bracket_result:
        artist, title = bracket_result
        if artist and title:
            return (artist, title)
    
    # Clean the name for further processing
    name_cleaned = clean_suffix(name)
    
    # Find separator
    sep_result = find_best_separator(name_cleaned)
    
    if sep_result:
        separator, pos = sep_result
        part1 = name_cleaned[:pos].strip()
        part2 = name_cleaned[pos + len(separator):].strip()
        
        if part1 and part2:
            artist, title = determine_artist_title(part1, part2, separator)
            
            # Clean up the results
            artist = clean_suffix(artist)
            title = clean_suffix(title)
            
            # Remove parenthetical from title if it makes it cleaner
            title_clean = remove_parenthetical(title)
            if title_clean and len(title_clean) > 2:
                title = title_clean
            
            if artist and title:
                return (artist, title)
    
    # Try to extract artist from folder path
    if file_path:
        parts = file_path.replace('\\', '/').split('/')
        for part in reversed(parts[:-1]):  # Exclude filename
            if part and part.lower() not in ('music', 'songs', 'chinese songs', 
                                              'japanese songs', 'korean songs',
                                              'english songs', 'anime songs',
                                              'instrumental music & osts',
                                              'fingerstyle guitar songs', 'osts'):
                artist = extract_known_artist(part)
                if artist:
                    title = clean_suffix(name_cleaned)
                    return (artist, title)
    
    # Try to find known artist anywhere in the name
    known = extract_known_artist(name_cleaned)
    if known:
        # Remove the artist name from the title
        title = name_cleaned
        for variant in [known, known.lower(), known.upper()]:
            title = title.replace(variant, '')
        title = clean_suffix(title)
        title = re.sub(r'^[\s\-_/]+', '', title)
        title = re.sub(r'[\s\-_/]+$', '', title)
        
        if title:
            return (known, title)
    
    # Can't determine artist - use full name as title
    title = clean_suffix(name_cleaned)
    if not title:
        title = original_name
    
    return ("Unknown", title)


def parse_filename(filename: str, file_path: str = None) -> Optional['TrackInfo']:
    """
    Parse artist and title from filename.
    Always returns a TrackInfo (never None) - uses Unknown for artist if needed.
    """
    artist, title = parse_filename_smart(filename, file_path)
    
    # Ensure we have a title
    if not title or len(title.strip()) < 1:
        title = re.sub(r'\.(mp3|m4a|flac|wav|ogg|opus|aac|wma)$', '', filename, flags=re.I)
    
    return TrackInfo(
        title=title.strip(),
        artist=artist.strip(),
        file_path=file_path or filename
    )


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TrackInfo:
    """Represents extracted track metadata."""
    title: str
    artist: str
    album: Optional[str] = None
    genre: Optional[str] = None
    year: Optional[int] = None
    file_path: Optional[str] = None
    spotify_id: Optional[str] = None
    
    def __hash__(self):
        return hash((self.title.lower(), self.artist.lower()))
    
    def __eq__(self, other):
        if not isinstance(other, TrackInfo):
            return False
        return (self.title.lower() == other.title.lower() and 
                self.artist.lower() == other.artist.lower())


@dataclass  
class Recommendation:
    """Represents a recommended track with score and links."""
    title: str
    artist: str
    album: str
    score: float
    popularity: int
    spotify_url: Optional[str]
    youtube_url: str
    genres: List[str]


@dataclass
class LibraryIndex:
    """Index of user's library for efficient duplicate detection."""
    spotify_track_ids: Set[str] = field(default_factory=set)
    normalized_tracks: Set[str] = field(default_factory=set)
    artist_titles: Set[Tuple[str, str]] = field(default_factory=set)
    song_titles: Set[str] = field(default_factory=set)
    title_variations: Dict[str, Set[str]] = field(default_factory=dict)
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """Normalize text for matching."""
        if not text:
            return ""
        
        text = clean_suffix(text)
        text = text.lower().strip()
        
        # Remove brackets and their contents
        text = re.sub(r'[【】「」『』\[\]()（）]', ' ', text)
        text = re.sub(r'[^\w\s\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    @staticmethod
    def normalize_artist(artist: str) -> str:
        """Normalize artist name."""
        if not artist:
            return ""
        
        artist = artist.strip().lower()
        
        # Don't normalize "unknown" away - we still want to match on title
        if artist in ('unknown', 'unknown artist'):
            return ""
        
        artist = re.sub(r'^the\s+', '', artist)
        artist = re.sub(r'[^\w\s\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', ' ', artist)
        artist = re.sub(r'\s+', ' ', artist).strip()
        
        return artist
    
    def add_track(self, track: TrackInfo, spotify_id: Optional[str] = None) -> None:
        """Add a track to the index."""
        if spotify_id:
            self.spotify_track_ids.add(spotify_id)
        if track.spotify_id:
            self.spotify_track_ids.add(track.spotify_id)
        
        norm_artist = self.normalize_artist(track.artist)
        norm_title = self.normalize_text(track.title)
        
        if norm_title:
            if norm_artist:
                key = f"{norm_artist}|||{norm_title}"
                self.normalized_tracks.add(key)
                self.artist_titles.add((norm_artist, norm_title))
            
            # Always add title-only for matching
            self.normalized_tracks.add(f"|||{norm_title}")
            self.song_titles.add(norm_title)
            
            if norm_title not in self.title_variations:
                self.title_variations[norm_title] = set()
            if norm_artist:
                self.title_variations[norm_title].add(norm_artist)
        
        # Add raw lowercase
        self.artist_titles.add((track.artist.lower(), track.title.lower()))
    
    def add_spotify_track_id(self, track_id: str) -> None:
        """Add a Spotify track ID to the index."""
        if track_id:
            self.spotify_track_ids.add(track_id)
    
    def contains(self, 
                 spotify_id: Optional[str] = None,
                 title: Optional[str] = None, 
                 artist: Optional[str] = None) -> bool:
        """Check if a track is in the library."""
        if spotify_id and spotify_id in self.spotify_track_ids:
            return True
            
        if not title:
            return False
        
        norm_title = self.normalize_text(title)
        norm_artist = self.normalize_artist(artist) if artist else ""
        
        # Exact match with artist
        if norm_artist and norm_title:
            key = f"{norm_artist}|||{norm_title}"
            if key in self.normalized_tracks:
                return True
        
        # Title-only match
        if f"|||{norm_title}" in self.normalized_tracks:
            return True
        
        # Check song titles
        if norm_title in self.song_titles:
            return True
        
        # Fuzzy title match
        if norm_title:
            for existing_title in self.song_titles:
                if norm_title in existing_title or existing_title in norm_title:
                    return True
                if len(norm_title) > 5 and len(existing_title) > 5:
                    words1 = set(norm_title.split())
                    words2 = set(existing_title.split())
                    if words1 and words2:
                        overlap = len(words1 & words2)
                        if overlap / max(len(words1), len(words2)) >= 0.6:
                            return True
        
        return False
    
    def get_stats(self) -> Dict[str, int]:
        """Get index statistics."""
        return {
            "spotify_ids": len(self.spotify_track_ids),
            "normalized_tracks": len(self.normalized_tracks),
            "artist_title_pairs": len(self.artist_titles),
            "unique_titles": len(self.song_titles)
        }


# =============================================================================
# SPOTIFY FUNCTIONS
# =============================================================================

def create_spotify_client(client_id: str, client_secret: str) -> spotipy.Spotify:
    """Create authenticated Spotify client."""
    try:
        auth_manager = SpotifyClientCredentials(
            client_id=client_id,
            client_secret=client_secret
        )
        client = spotipy.Spotify(auth_manager=auth_manager)
        client.search(q="test", type="track", limit=1)
        logger.info("Spotify client authenticated successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to create Spotify client: {e}")
        raise


def extract_metadata_from_bytes(file_bytes: bytes, filename: str, file_path: str = None) -> Optional[TrackInfo]:
    """
    Extract ID3/metadata tags from file bytes.
    Always returns a TrackInfo (falls back to filename parsing).
    """
    try:
        file_obj = io.BytesIO(file_bytes)
        audio = MutagenFile(file_obj)
        
        if audio is None:
            file_obj.seek(0)
            try:
                audio = ID3(file_obj)
            except:
                pass
                
        if audio is not None:
            title = None
            artist = None
            album = None
            genre = None
            year = None
            
            if hasattr(audio, 'tags') and audio.tags:
                tags = audio.tags
                
                if hasattr(tags, 'get'):
                    title = _get_tag_value(tags, ['TIT2', 'title', '\xa9nam'])
                    artist = _get_tag_value(tags, ['TPE1', 'artist', '\xa9ART'])
                    album = _get_tag_value(tags, ['TALB', 'album', '\xa9alb'])
                    genre = _get_tag_value(tags, ['TCON', 'genre', '\xa9gen'])
                    year_str = _get_tag_value(tags, ['TDRC', 'date', '\xa9day', 'TYER'])
                    if year_str:
                        try:
                            year = int(str(year_str)[:4])
                        except:
                            pass
            
            if isinstance(audio, MP4):
                tags = audio.tags or {}
                title = _get_first(tags.get('\xa9nam'))
                artist = _get_first(tags.get('\xa9ART'))
                album = _get_first(tags.get('\xa9alb'))
                genre = _get_first(tags.get('\xa9gen'))
                
            if isinstance(audio, FLAC):
                title = _get_first(audio.get('title'))
                artist = _get_first(audio.get('artist'))
                album = _get_first(audio.get('album'))
                genre = _get_first(audio.get('genre'))
            
            # If we got valid metadata with both title and artist
            if title and artist:
                return TrackInfo(
                    title=str(title).strip(),
                    artist=str(artist).strip(),
                    album=str(album).strip() if album else None,
                    genre=str(genre).strip() if genre else None,
                    year=year,
                    file_path=file_path or filename
                )
                
    except Exception as e:
        logger.debug(f"Mutagen extraction failed for {filename}: {e}")
    
    # Fallback to filename parsing - this always returns a TrackInfo
    return parse_filename(filename, file_path)


def _get_tag_value(tags, keys: List[str]) -> Optional[str]:
    """Try multiple tag keys and return first found value."""
    for key in keys:
        try:
            value = tags.get(key)
            if value:
                if hasattr(value, 'text'):
                    return str(value.text[0]) if value.text else None
                elif isinstance(value, list):
                    return str(value[0]) if value else None
                else:
                    return str(value)
        except:
            continue
    return None


def _get_first(value) -> Optional[str]:
    """Get first element if list, else return as-is."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def search_spotify_track(
    spotify: spotipy.Spotify, 
    track: TrackInfo,
    market: str = None
) -> Optional[Dict[str, Any]]:
    """Search Spotify for a track and return match."""
    # Skip search if artist is unknown and title is very generic
    artist = track.artist
    title = track.title
    
    # Build search queries
    queries = []
    
    if artist.lower() not in ('unknown', 'unknown artist'):
        queries.append(f'track:"{title}" artist:"{artist}"')
        queries.append(f'{artist} {title}')
    
    queries.append(f'{title}')  # Title-only search
    
    search_kwargs = {"type": "track", "limit": 5}
    if market:
        search_kwargs["market"] = market
    
    for query in queries:
        try:
            results = spotify.search(q=query, **search_kwargs)
            tracks = results.get('tracks', {}).get('items', [])
            
            for item in tracks:
                spotify_title = item['name'].lower()
                spotify_artist = item['artists'][0]['name'].lower() if item['artists'] else ''
                
                # Title match
                title_match = (
                    title.lower() in spotify_title or 
                    spotify_title in title.lower() or
                    _similar(title.lower(), spotify_title) > 0.5
                )
                
                if title_match:
                    # If we have a known artist, verify it matches
                    if artist.lower() not in ('unknown', 'unknown artist'):
                        artist_match = (
                            artist.lower() in spotify_artist or
                            spotify_artist in artist.lower() or
                            _similar(artist.lower(), spotify_artist) > 0.5
                        )
                        if artist_match:
                            return item
                    else:
                        # No artist to verify, accept on title match
                        return item
                        
        except Exception as e:
            logger.debug(f"Spotify search failed for '{query}': {e}")
            continue
            
    return None


def _similar(a: str, b: str) -> float:
    """Simple similarity ratio between two strings."""
    if not a or not b:
        return 0.0
    a_set = set(a.lower().split())
    b_set = set(b.lower().split())
    if not a_set or not b_set:
        return 0.0
    intersection = len(a_set & b_set)
    union = len(a_set | b_set)
    return intersection / union if union > 0 else 0.0


def get_artist_genres(spotify: spotipy.Spotify, artist_id: str) -> List[str]:
    """Get genres for an artist from Spotify."""
    try:
        artist = spotify.artist(artist_id)
        return artist.get('genres', [])
    except:
        return []


def get_musicbrainz_tags(artist: str, title: str) -> List[str]:
    """Get tags/genres from MusicBrainz for a track."""
    tags = []
    
    # Skip if artist is unknown
    if artist.lower() in ('unknown', 'unknown artist'):
        return tags
    
    try:
        result = musicbrainzngs.search_recordings(
            recording=title,
            artist=artist,
            limit=3
        )
        
        recordings = result.get('recording-list', [])
        
        for recording in recordings:
            if 'tag-list' in recording:
                for tag in recording['tag-list']:
                    if 'name' in tag:
                        tags.append(tag['name'].lower())
                        
            if 'artist-credit' in recording:
                for credit in recording['artist-credit']:
                    if isinstance(credit, dict) and 'artist' in credit:
                        artist_info = credit['artist']
                        if 'tag-list' in artist_info:
                            for tag in artist_info['tag-list']:
                                if 'name' in tag:
                                    tags.append(tag['name'].lower())
                                    
    except Exception as e:
        logger.debug(f"MusicBrainz lookup failed for {artist} - {title}: {e}")
        
    return list(set(tags))


def build_taste_profile(
    tracks: List[TrackInfo], 
    spotify: spotipy.Spotify,
    market: str = None
) -> Tuple[Dict[str, float], Set[str], List[str], List[str], LibraryIndex]:
    """Build a taste profile from user's music library."""
    genre_counter = Counter()
    artist_ids = set()
    track_ids = []
    artist_names = Counter()
    artist_id_to_name = {}
    
    library_index = LibraryIndex()
    
    total = len(tracks)
    logger.info(f"Building taste profile from {total} tracks...")
    
    tracks_with_unknown = 0
    tracks_found_on_spotify = 0
    
    for i, track in enumerate(tracks):
        if (i + 1) % 50 == 0:
            logger.info(f"  Analyzing: {i + 1}/{total} tracks...")
        
        # Count artists (but don't count "Unknown")
        if track.artist.lower() not in ('unknown', 'unknown artist'):
            artist_names[track.artist.lower()] += 1
        else:
            tracks_with_unknown += 1
        
        # Always add to library index
        library_index.add_track(track)
        
        # Try to find on Spotify
        spotify_track = search_spotify_track(spotify, track, market=market)
        
        if spotify_track:
            tracks_found_on_spotify += 1
            track_id = spotify_track['id']
            track_ids.append(track_id)
            
            library_index.add_spotify_track_id(track_id)
            track.spotify_id = track_id
            
            if spotify_track['artists']:
                artist = spotify_track['artists'][0]
                artist_id = artist['id']
                artist_name = artist['name']
                artist_ids.add(artist_id)
                artist_id_to_name[artist_id] = artist_name
                
                genres = get_artist_genres(spotify, artist_id)
                for genre in genres:
                    genre_counter[genre] += 1
        
        # Try MusicBrainz for additional tags
        if track.artist.lower() not in ('unknown', 'unknown artist'):
            mb_tags = get_musicbrainz_tags(track.artist, track.title)
            for tag in mb_tags:
                genre_counter[tag] += 0.5
            
    total_weight = sum(genre_counter.values()) or 1
    genre_weights = {genre: count / total_weight for genre, count in genre_counter.items()}
    
    # Get top artists - EXCLUDE "Unknown"
    top_artist_ids = sorted(
        artist_ids, 
        key=lambda aid: artist_names.get(artist_id_to_name.get(aid, '').lower(), 0), 
        reverse=True
    )
    
    top_artist_names = []
    for aid in top_artist_ids[:30]:
        name = artist_id_to_name.get(aid, '')
        if name and name.lower() not in ('unknown', 'unknown artist'):
            top_artist_names.append(name)
        if len(top_artist_names) >= 20:
            break
    
    index_stats = library_index.get_stats()
    
    logger.info(f"✅ Taste profile built:")
    logger.info(f"   Total tracks processed: {total}")
    logger.info(f"   Tracks with unknown artist: {tracks_with_unknown}")
    logger.info(f"   Tracks found on Spotify: {tracks_found_on_spotify}")
    logger.info(f"   Genres: {len(genre_weights)}")
    logger.info(f"   Artists: {len(artist_ids)}")
    if top_artist_names:
        logger.info(f"   Top artists: {', '.join(top_artist_names[:5])}...")
    
    return genre_weights, artist_ids, track_ids, top_artist_names, library_index


def should_exclude_track(track_name: str, settings: Dict[str, Any]) -> Tuple[bool, str]:
    """Check if a track should be excluded based on settings."""
    name_lower = track_name.lower()
    
    if settings.get('exclude_remixes', True):
        if re.search(r'\b(remix|refix|bootleg|mashup|vip)\b', name_lower):
            return True, "remix"
    
    if settings.get('exclude_covers', True):
        if re.search(r'\bcover\b', name_lower):
            return True, "cover"
    
    if settings.get('exclude_live', True):
        if re.search(r'\blive\b', name_lower):
            return True, "live"
    
    if settings.get('exclude_karaoke', True):
        if re.search(r'\b(karaoke|backing\s*track)\b', name_lower):
            return True, "karaoke"
    
    if settings.get('exclude_instrumentals', False):
        if re.search(r'\binstrumental\b', name_lower):
            return True, "instrumental"
    
    return False, ""


def generate_youtube_url(artist: str, title: str) -> str:
    """Generate a YouTube search URL for a track."""
    if artist.lower() in ('unknown', 'unknown artist'):
        query = title
    else:
        query = f"{artist} {title}"
    encoded_query = urllib.parse.quote(query)
    return f"https://www.youtube.com/results?search_query={encoded_query}"


def get_related_artists(spotify: spotipy.Spotify, artist_id: str) -> List[Dict[str, Any]]:
    """Get related artists for a given artist."""
    try:
        result = spotify.artist_related_artists(artist_id)
        return result.get('artists', [])
    except:
        return []


def get_artist_top_tracks(
    spotify: spotipy.Spotify, 
    artist_id: str, 
    market: str = 'JP'
) -> List[Dict[str, Any]]:
    """Get top tracks for an artist in specified market."""
    try:
        result = spotify.artist_top_tracks(artist_id, country=market)
        return result.get('tracks', [])
    except:
        return []


def search_tracks_by_genre(
    spotify: spotipy.Spotify, 
    genre: str, 
    limit: int = 20,
    market: str = None
) -> List[Dict[str, Any]]:
    """Search for tracks by genre."""
    try:
        query = f'genre:"{genre}"'
        search_kwargs = {"q": query, "type": "track", "limit": limit}
        if market:
            search_kwargs["market"] = market
        result = spotify.search(**search_kwargs)
        return result.get('tracks', {}).get('items', [])
    except:
        return []


def calculate_score(
    candidate: Dict[str, Any],
    user_genres: Dict[str, float],
    user_artist_ids: Set[str],
    all_popularities: List[int],
    weights: Dict[str, float]
) -> float:
    """Calculate recommendation score for a candidate track."""
    candidate_genres = set(candidate.get('_genres', []))
    
    # Tag similarity
    if user_genres and candidate_genres:
        matched_weight = sum(user_genres.get(g, 0) for g in candidate_genres)
        tag_similarity = min(matched_weight * 2, 1.0)
    else:
        tag_similarity = 0.3
    
    # Artist affinity
    artist_affinity = 0.0
    if candidate.get('artists'):
        candidate_artist_id = candidate['artists'][0]['id']
        if candidate_artist_id in user_artist_ids:
            artist_affinity = 1.0
        else:
            artist_affinity = 0.2
    
    # Popularity z-score
    popularity = candidate.get('popularity', 50)
    if all_popularities:
        mean_pop = sum(all_popularities) / len(all_popularities)
        std_pop = (sum((p - mean_pop) ** 2 for p in all_popularities) / len(all_popularities)) ** 0.5
        if std_pop > 0:
            z_score = (popularity - mean_pop) / std_pop
            popularity_z = (z_score + 3) / 6
            popularity_z = max(0, min(1, popularity_z))
        else:
            popularity_z = 0.5
    else:
        popularity_z = popularity / 100
    
    w = weights
    score = (
        w.get('tag_similarity', 0.60) * tag_similarity +
        w.get('artist_affinity', 0.25) * artist_affinity +
        w.get('popularity', 0.15) * popularity_z
    )
    
    return score


def get_recommendations(
    spotify: spotipy.Spotify,
    track_ids: List[str],
    artist_ids: Set[str],
    user_genres: Dict[str, float],
    library_index: LibraryIndex,
    settings: Dict[str, Any],
    weights: Dict[str, float]
) -> List[Recommendation]:
    """Generate recommendations using Spotify endpoints."""
    recommendations = []
    seen_tracks = set()
    candidates = []
    skipped_in_library = 0
    skipped_excluded = 0
    
    artist_id_list = list(artist_ids)
    
    if not artist_id_list:
        logger.error("No artist IDs available for recommendations")
        raise ValueError("No valid artists found.")
    
    market = settings.get('market', 'JP') or 'JP'
    
    logger.info(f"Generating recommendations from {len(artist_id_list)} artists...")
    logger.info(f"  Market: {market}")
    
    index_stats = library_index.get_stats()
    logger.info(f"  Library index: {index_stats['spotify_ids']} Spotify IDs, "
                f"{index_stats['unique_titles']} unique titles")
    
    # Strategy 1: Related artists and their top tracks
    logger.info("  → Finding related artists...")
    
    sample_artists = random.sample(artist_id_list, min(15, len(artist_id_list)))
    related_artist_ids = set()
    
    for artist_id in sample_artists:
        related = get_related_artists(spotify, artist_id)
        for artist in related[:5]:
            related_artist_ids.add(artist['id'])
    
    logger.info(f"  → Found {len(related_artist_ids)} related artists")
    logger.info("  → Fetching top tracks from related artists...")
    
    for artist_id in list(related_artist_ids)[:25]:
        top_tracks = get_artist_top_tracks(spotify, artist_id, market=market)
        
        for track in top_tracks[:5]:
            if not track.get('artists'):
                continue
            track_key = (track['name'].lower(), track['artists'][0]['name'].lower())
            if track_key not in seen_tracks:
                seen_tracks.add(track_key)
                artist_genres = get_artist_genres(spotify, track['artists'][0]['id'])
                track['_genres'] = artist_genres
                candidates.append(track)
    
    logger.info(f"  → Collected {len(candidates)} tracks from related artists")
    
    # Strategy 2: Genre-based search
    top_genres = sorted(user_genres.keys(), key=lambda g: user_genres[g], reverse=True)[:10]
    
    if top_genres:
        logger.info(f"  → Searching by your top genres: {', '.join(top_genres[:5])}...")
        
        for genre in top_genres:
            genre_tracks = search_tracks_by_genre(spotify, genre, limit=15, market=market)
            
            for track in genre_tracks:
                if not track.get('artists'):
                    continue
                track_key = (track['name'].lower(), track['artists'][0]['name'].lower())
                if track_key not in seen_tracks:
                    seen_tracks.add(track_key)
                    track['_genres'] = [genre]
                    candidates.append(track)
        
        logger.info(f"  → Total after genre search: {len(candidates)}")
    
    # Strategy 3: More from your top artists
    logger.info("  → Getting more tracks from your top artists...")
    
    for artist_id in sample_artists[:10]:
        top_tracks = get_artist_top_tracks(spotify, artist_id, market=market)
        artist_genres = get_artist_genres(spotify, artist_id)
        
        for track in top_tracks:
            if not track.get('artists'):
                continue
            track_key = (track['name'].lower(), track['artists'][0]['name'].lower())
            if track_key not in seen_tracks:
                seen_tracks.add(track_key)
                track['_genres'] = artist_genres
                candidates.append(track)
    
    logger.info(f"  → Final candidate pool: {len(candidates)} tracks")
    
    if not candidates:
        logger.warning("No candidate tracks found")
        return []
    
    # Scoring and filtering
    logger.info("  → Scoring and filtering candidates...")
    
    all_popularities = [t.get('popularity', 50) for t in candidates]
    scored_candidates = []
    
    for candidate in candidates:
        track_name = candidate.get('name', '')
        artist_name = candidate['artists'][0]['name'] if candidate.get('artists') else ''
        track_id = candidate.get('id', '')
        
        # Check if in library
        if library_index.contains(
            spotify_id=track_id,
            title=track_name,
            artist=artist_name
        ):
            skipped_in_library += 1
            continue
        
        # Check exclusions
        should_exclude, reason = should_exclude_track(track_name, settings)
        if should_exclude:
            skipped_excluded += 1
            continue
        
        # Skip low popularity
        min_popularity = settings.get('min_popularity', 10)
        if candidate.get('popularity', 0) < min_popularity:
            continue
        
        # Calculate score
        score = calculate_score(
            candidate, 
            user_genres, 
            artist_ids, 
            all_popularities,
            weights
        )
        
        scored_candidates.append((score, candidate))
    
    logger.info(f"  → Skipped {skipped_in_library} tracks already in your library")
    logger.info(f"  → Skipped {skipped_excluded} excluded tracks")
    
    # Sort and take top N
    scored_candidates.sort(key=lambda x: x[0], reverse=True)
    max_recs = settings.get('max_recommendations', 30)
    
    for score, candidate in scored_candidates[:max_recs]:
        artist_name = candidate['artists'][0]['name'] if candidate.get('artists') else 'Unknown'
        track_name = candidate.get('name', 'Unknown')
        album_name = candidate.get('album', {}).get('name', 'Unknown')
        
        spotify_url = candidate.get('external_urls', {}).get('spotify')
        youtube_url = generate_youtube_url(artist_name, track_name)
        
        rec = Recommendation(
            title=track_name,
            artist=artist_name,
            album=album_name,
            score=score,
            popularity=candidate.get('popularity', 0),
            spotify_url=spotify_url,
            youtube_url=youtube_url,
            genres=candidate.get('_genres', [])
        )
        recommendations.append(rec)
    
    logger.info(f"✅ Generated {len(recommendations)} recommendations")
    
    return recommendations