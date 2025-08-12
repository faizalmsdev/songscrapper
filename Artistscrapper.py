import json
import threading
import time
import os
import re
import subprocess
import sys
import requests
import hashlib
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gzip
import brotli

# === CONFIGURATION ===
class Config:
    # Spotify settings
    ARTIST_ID = ""  # Will be set by user input
    TARGET_API_URL = "https://api-partner.spotify.com/pathfinder/v2/query"
    
    # Scrolling settings
    SCROLL_PAUSE_TIME = 2
    AUTO_SCROLL_ENABLED = True
    SCROLL_PIXELS = 800
    
    # Download settings
    AUDIO_QUALITY = '192K'
    MAX_RETRIES = 3
    DOWNLOAD_DELAY = 1  # Seconds between downloads
    
    # Metadata settings
    DOWNLOAD_COVER_ART = True
    COVER_ART_SIZE = 640  # Preferred size (640x640, 300x300, or 64x64)
    
    # Error handling settings
    SKIP_INVALID_TRACKS = True
    MIN_TRACK_NAME_LENGTH = 1
    MIN_ARTIST_NAME_LENGTH = 1
    
    # Consolidation settings
    CONSOLIDATED_FOLDER = "consolidated_music"
    ENABLE_SMART_DEDUPLICATION = True
    
    # Test folder for captured data
    TEST_FOLDER = "test"

# === GLOBAL VARIABLES ===
captured_data = []
all_artist_tracks = []
seen_requests = set()
stop_capture = False
auto_scroll_active = False

# === SMART SONG MANAGER CLASS ===
class SmartSongManager:
    def __init__(self, consolidated_folder: str = "consolidated_music"):
        self.consolidated_folder = Path(consolidated_folder)
        self.songs_folder = self.consolidated_folder / "songs"
        self.metadata_folder = self.consolidated_folder / "metadata"
        
        # Create directories if they don't exist
        self.songs_folder.mkdir(parents=True, exist_ok=True)
        self.metadata_folder.mkdir(parents=True, exist_ok=True)
        
        # Load existing databases
        self.existing_songs = {}  # song_id -> song_info
        self.existing_playlists = {}  # playlist_id -> playlist_info
        self.existing_artists = {}  # artist_uri -> artist_info
        self.uri_to_song_id = {}  # track_uri -> song_id
        self.name_artist_to_song_id = {}  # normalized_name_artist -> song_id
        
        self.load_existing_databases()
    
    def load_existing_databases(self):
        """Load existing songs, playlists, and artists databases"""
        # Load songs database
        songs_db_path = self.metadata_folder / 'songs_database.json'
        if songs_db_path.exists():
            try:
                with open(songs_db_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    existing_songs = data.get('songs', {})
                    
                    for song_id, song_info in existing_songs.items():
                        self.existing_songs[song_id] = song_info
                        
                        # Build lookup tables
                        metadata = song_info.get('metadata', {})
                        track_uri = metadata.get('track_uri', '')
                        if track_uri:
                            self.uri_to_song_id[track_uri] = song_id
                        
                        # Create name+artist lookup
                        track_name = metadata.get('track_name', '').lower().strip()
                        artists = metadata.get('artists_string', '').lower().strip()
                        if track_name and artists:
                            key = f"{track_name}|{artists}"
                            self.name_artist_to_song_id[key] = song_id
                
                print(f"📚 Loaded {len(self.existing_songs)} existing songs from database")
                
            except Exception as e:
                print(f"⚠️  Warning: Could not load existing songs database: {e}")
        
        # Load playlists database
        playlists_db_path = self.metadata_folder / 'playlists_database.json'
        if playlists_db_path.exists():
            try:
                with open(playlists_db_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.existing_playlists = data.get('playlists', {})
                
                print(f"📚 Loaded {len(self.existing_playlists)} existing playlists from database")
                
            except Exception as e:
                print(f"⚠️  Warning: Could not load existing playlists database: {e}")
        
        # Load artists database
        artists_db_path = self.metadata_folder / 'artists_database.json'
        if artists_db_path.exists():
            try:
                with open(artists_db_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.existing_artists = data.get('artists', {})
                
                print(f"📚 Loaded {len(self.existing_artists)} existing artists from database")
                
            except Exception as e:
                print(f"⚠️  Warning: Could not load existing artists database: {e}")
        else:
            print("🆕 No existing artists database found - starting fresh")
    
    def generate_song_id(self, track_name: str, artists: str) -> str:
        """Generate a unique ID for a song based on track name and artists"""
        clean_string = f"{track_name}_{artists}".lower()
        clean_string = re.sub(r'[^a-z0-9_]', '', clean_string)
        hash_object = hashlib.md5(clean_string.encode())
        return f"song_{hash_object.hexdigest()[:12]}"
    
    def generate_playlist_id(self, playlist_name: str) -> str:
        """Generate a unique ID for a playlist"""
        clean_string = playlist_name.lower()
        clean_string = re.sub(r'[^a-z0-9_]', '', clean_string)
        hash_object = hashlib.md5(clean_string.encode())
        return f"playlist_{hash_object.hexdigest()[:12]}"
    
    def find_existing_song(self, track_info: dict) -> Optional[Tuple[str, dict]]:
        """
        Find existing song in database
        Returns: (song_id, song_info) if found, None otherwise
        """
        track_uri = track_info.get('track_uri', '')
        track_name = track_info.get('track_name', '').lower().strip()
        artists = track_info.get('artists_string', '').lower().strip()
        
        # First check by URI (most reliable)
        if track_uri and track_uri in self.uri_to_song_id:
            song_id = self.uri_to_song_id[track_uri]
            return song_id, self.existing_songs[song_id]
        
        # Then check by name + artists
        if track_name and artists:
            key = f"{track_name}|{artists}"
            if key in self.name_artist_to_song_id:
                song_id = self.name_artist_to_song_id[key]
                return song_id, self.existing_songs[song_id]
        
        return None
    
    def add_playlist_to_song(self, song_id: str, playlist_id: str):
        """Add playlist ID to existing song without replacing other playlists"""
        if song_id in self.existing_songs:
            current_playlists = self.existing_songs[song_id].get('playlists', [])
            if playlist_id not in current_playlists:
                current_playlists.append(playlist_id)
                self.existing_songs[song_id]['playlists'] = current_playlists
                print(f"   ✅ Added playlist {playlist_id} to existing song {song_id}")
                return True
            else:
                print(f"   ℹ️  Song {song_id} already has playlist {playlist_id}")
                return False
        return False
    
    def store_artist_info(self, artist_uri: str, artist_name: str, playlist_id: str):
        """Store artist information in artists database"""
        if artist_uri in self.existing_artists:
            # Update existing artist
            if playlist_id not in self.existing_artists[artist_uri].get('playlist_ids', []):
                self.existing_artists[artist_uri]['playlist_ids'].append(playlist_id)
                self.existing_artists[artist_uri]['last_updated'] = datetime.now().isoformat()
        else:
            # Create new artist entry
            self.existing_artists[artist_uri] = {
                'name': artist_name,
                'uri': artist_uri,
                'playlist_ids': [playlist_id],
                'created_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat()
            }

# === UTILITY FUNCTIONS ===
def install_required_packages():
    """Install required packages if not available"""
    try:
        import yt_dlp
        print("✅ yt-dlp is available")
    except ImportError:
        print("📦 Installing yt-dlp...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yt-dlp"])
        print("✅ yt-dlp installed successfully")
    
    try:
        import requests
        print("✅ requests is available")
    except ImportError:
        print("📦 Installing requests...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
        print("✅ requests installed successfully")

def check_prerequisites():
    """Check if required tools are available"""
    print("🔧 Checking prerequisites...")
    
    # Check ffmpeg
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("   ✅ ffmpeg found")
        else:
            print("   ❌ ffmpeg not working properly")
            return False
    except FileNotFoundError:
        print("   ❌ ffmpeg not found - please install ffmpeg")
        print("      Download from: https://ffmpeg.org/download.html")
        return False
    
    install_required_packages()
    return True

def sanitize_filename(filename):
    """Remove invalid characters from filename with enhanced error handling"""
    try:
        if not filename or not str(filename).strip():
            return "unknown_file"
        
        filename = str(filename).strip()
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        filename = re.sub(r'[^\w\s-]', '', filename)
        filename = re.sub(r'[-\s]+', '-', filename)
        result = filename.strip('-')[:100]
        
        return result if result else "unknown_file"
    except Exception as e:
        print(f"   ⚠️  Error sanitizing filename '{filename}': {e}")
        return "unknown_file"

def safe_get(data, *keys, default="Unknown"):
    """Safely navigate nested dictionaries with fallback"""
    try:
        result = data
        for key in keys:
            if isinstance(result, dict) and key in result:
                result = result[key]
            else:
                return default
        return result if result is not None and str(result).strip() else default
    except:
        return default

def download_song(track_name: str, artists_string: str, song_id: str, output_folder: Path) -> bool:
    """Download a song using yt-dlp"""
    try:
        import yt_dlp
        
        # Create search query
        search_query = f"{track_name} {artists_string}"
        
        # Configure yt-dlp options
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': str(output_folder / f'{song_id}.%(ext)s'),
            'extractaudio': True,
            'audioformat': 'mp3',
            'audioquality': Config.AUDIO_QUALITY,
            'quiet': True,
            'no_warnings': True
        }
        
        print(f"   🔍 Searching for: {search_query}")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Search for the song
            info = ydl.extract_info(f"ytsearch1:{search_query}", download=True)
            
            if info and 'entries' in info and len(info['entries']) > 0:
                entry = info['entries'][0]
                print(f"   ✅ Downloaded: {entry.get('title', 'Unknown')}")
                return True
            else:
                print(f"   ❌ No results found for: {search_query}")
                return False
                
    except Exception as e:
        print(f"   ❌ Download failed for {track_name}: {e}")
        return False

# === SPOTIFY CAPTURE FUNCTIONS ===
def decode_response_body(response):
    """Decode response body handling different compression formats"""
    try:
        body = response.body
        if not body:
            return ""
        
        encoding = response.headers.get('content-encoding', '').lower()
        
        if encoding == 'gzip':
            body = gzip.decompress(body)
        elif encoding == 'br':
            body = brotli.decompress(body)
        elif encoding == 'deflate':
            import zlib
            body = zlib.decompress(body)
        
        try:
            return body.decode('utf-8')
        except UnicodeDecodeError:
            return body.decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"[!] Error decoding response body: {e}")
        return ""

def parse_json_response(body_text):
    """Try to parse response as JSON"""
    try:
        return json.loads(body_text)
    except json.JSONDecodeError:
        return body_text

def is_artist_discography_response(parsed_response):
    """Check if the response contains artist discography data"""
    try:
        if isinstance(parsed_response, dict):
            data = parsed_response.get('data', {})
            album_union = data.get('albumUnion', {})
            return album_union.get('__typename') == 'Album'
        return False
    except:
        return False

def extract_tracks_from_response(parsed_response):
    """Extract the tracks array from artist discography response"""
    try:
        if isinstance(parsed_response, dict):
            data = parsed_response.get('data', {})
            album_union = data.get('albumUnion', {})
            tracks_v2 = album_union.get('tracksV2', {})
            items = tracks_v2.get('items', [])
            return items
    except:
        pass
    return []

def request_interceptor(request):
    """Intercept HTTP requests to capture Spotify API calls"""
    global captured_data, all_artist_tracks, seen_requests, stop_capture
    
    try:
        if stop_capture:
            return
        
        if Config.TARGET_API_URL in request.url:
            request_hash = hashlib.md5(f"{request.url}{request.body}".encode()).hexdigest()
            
            if request_hash not in seen_requests:
                seen_requests.add(request_hash)
                captured_data.append({
                    'url': request.url,
                    'method': request.method,
                    'headers': dict(request.headers),
                    'body': request.body.decode('utf-8') if request.body else None,
                    'timestamp': datetime.now().isoformat(),
                    'hash': request_hash
                })
                
                print(f"[+] Captured request #{len(captured_data)} - {request.method} {request.url}")
                
    except Exception as e:
        print(f"[!] Error in request interceptor: {e}")

def response_interceptor(request, response):
    """Intercept HTTP responses to capture Spotify API data"""
    global captured_data, all_artist_tracks, stop_capture
    
    try:
        if stop_capture:
            return
        
        if Config.TARGET_API_URL in request.url and response.status_code == 200:
            body_text = decode_response_body(response)
            
            if body_text:
                parsed_response = parse_json_response(body_text)
                
                # Save raw response to test folder
                test_folder = Path(Config.TEST_FOLDER)
                test_folder.mkdir(exist_ok=True)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"artist_discography_response_{timestamp}.json"
                
                with open(test_folder / filename, 'w', encoding='utf-8') as f:
                    if isinstance(parsed_response, dict):
                        json.dump(parsed_response, f, indent=2, ensure_ascii=False)
                    else:
                        f.write(body_text)
                
                print(f"[+] Saved raw response to {filename}")
                
                # Check if this is artist discography data
                if is_artist_discography_response(parsed_response):
                    tracks = extract_tracks_from_response(parsed_response)
                    print(f"[+] Found {len(tracks)} tracks in response")
                    
                    for track_item in tracks:
                        track = track_item.get('track', {})
                        if track:
                            all_artist_tracks.append(track)
                            print(f"   └─ {safe_get(track, 'name')} by {', '.join([artist.get('profile', {}).get('name', 'Unknown') for artist in safe_get(track, 'artists', 'items', default=[])])}")
                
    except Exception as e:
        print(f"[!] Error in response interceptor: {e}")

def auto_scroll(driver):
    """Auto-scroll the page to load all artist tracks"""
    global stop_capture, auto_scroll_active
    auto_scroll_active = True
    scroll_count = 0
    
    print("🔄 Starting auto-scroll...")
    
    try:
        time.sleep(3)
        
        while not stop_capture and Config.AUTO_SCROLL_ENABLED:
            # Get current page height
            last_height = driver.execute_script("return document.body.scrollHeight")
            
            # Scroll down
            driver.execute_script(f"window.scrollBy(0, {Config.SCROLL_PIXELS});")
            scroll_count += 1
            
            print(f"   📜 Scroll #{scroll_count} - Found {len(all_artist_tracks)} tracks so far")
            
            # Wait for new content to load
            time.sleep(Config.SCROLL_PAUSE_TIME)
            
            # Check if page height changed (new content loaded)
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                print("   ✅ Reached end of page")
                break
                
            if scroll_count > 100:  # Safety limit
                print("   ⚠️  Reached scroll limit")
                break
                
    except Exception as e:
        print(f"[!] Error during auto-scroll: {e}")
    
    finally:
        auto_scroll_active = False
        print(f"🏁 Auto-scroll completed. Total tracks found: {len(all_artist_tracks)}")

def get_artist_id_from_user():
    """Get artist ID from user input"""
    print("🎵 Spotify Artist Discography Scraper")
    print("=" * 50)
    print("This tool will scrape all songs from an artist's discography")
    print()
    print("Example URL: https://open.spotify.com/artist/4zCH9qm4R2DADamUHMCa6O/discography/all")
    print("Artist ID from URL: 4zCH9qm4R2DADamUHMCa6O")
    print()
    
    while True:
        artist_input = input("Enter Spotify Artist ID (or full URL): ").strip()
        
        if not artist_input:
            print("❌ Please provide an artist ID or URL")
            continue
        
        # Extract artist ID from URL if full URL is provided
        if "open.spotify.com/artist/" in artist_input:
            try:
                artist_id = artist_input.split('/artist/')[1].split('/')[0].split('?')[0]
                print(f"✅ Extracted Artist ID: {artist_id}")
                return artist_id
            except:
                print("❌ Could not extract artist ID from URL. Please check the format.")
                continue
        else:
            # Assume it's already an artist ID
            if len(artist_input) == 22 and artist_input.isalnum():
                return artist_input
            else:
                print("❌ Invalid artist ID format. Should be 22 characters long.")
                continue

def process_artist_tracks(artist_name: str):
    """Process captured artist tracks and save to database"""
    global all_artist_tracks
    
    if not all_artist_tracks:
        print("❌ No tracks found to process")
        return
    
    print(f"\n🎵 Processing {len(all_artist_tracks)} tracks for artist: {artist_name}")
    
    song_manager = SmartSongManager()
    
    # Create artist playlist entry
    playlist_id = song_manager.generate_playlist_id(f"{artist_name} - Discography")
    playlist_name = f"{artist_name} - Discography"
    
    processed_tracks = []
    song_ids = []
    new_songs_to_download = []
    existing_songs_updated = 0
    
    # Get main artist info for storage
    main_artist_uri = ""
    if all_artist_tracks:
        first_track = all_artist_tracks[0]
        artists_data = safe_get(first_track, 'artists', 'items', default=[])
        for artist in artists_data:
            if safe_get(artist, 'profile', 'name') == artist_name:
                main_artist_uri = safe_get(artist, 'uri', default='')
                break
    
    for track_data in all_artist_tracks:
        try:
            # Extract track information
            track_name = safe_get(track_data, 'name', default='Unknown Track')
            track_uri = safe_get(track_data, 'uri', default='')
            duration_ms = safe_get(track_data, 'duration', 'totalMilliseconds', default=0)
            
            # Extract artists information
            artists_data = safe_get(track_data, 'artists', 'items', default=[])
            artists_info = []
            artists_names = []
            
            for artist in artists_data:
                artist_name_individual = safe_get(artist, 'profile', 'name', default='Unknown Artist')
                artist_uri = safe_get(artist, 'uri', default='')
                
                artists_info.append({
                    'name': artist_name_individual,
                    'uri': artist_uri
                })
                artists_names.append(artist_name_individual)
                
                # Store artist info in artists database
                song_manager.store_artist_info(artist_uri, artist_name_individual, playlist_id)
            
            artists_string = ', '.join(artists_names)
            
            # Create track metadata
            track_info = {
                'track_name': track_name,
                'artists_string': artists_string,
                'artists_info': artists_info,
                'track_uri': track_uri,
                'duration_ms': duration_ms,
                'album_name': 'Artist Discography',
                'track_number': len(processed_tracks) + 1
            }
            
            # Generate song ID
            song_id = song_manager.generate_song_id(track_name, artists_string)
            
            # Check if song already exists
            existing_song = song_manager.find_existing_song(track_info)
            
            if existing_song:
                # Song exists, add playlist ID to it
                existing_song_id, existing_song_info = existing_song
                if song_manager.add_playlist_to_song(existing_song_id, playlist_id):
                    existing_songs_updated += 1
                song_ids.append(existing_song_id)
                print(f"   🔄 Updated existing song: {track_name} by {artists_string}")
            else:
                # New song, create entry and mark for download
                song_entry = {
                    'metadata': track_info,
                    'playlists': [playlist_id],
                    'download_info': {
                        'status': 'pending',
                        'file_path': None,
                        'file_size': None,
                        'quality': Config.AUDIO_QUALITY,
                        'downloaded_at': None
                    },
                    'added_at': datetime.now().isoformat()
                }
                
                song_manager.existing_songs[song_id] = song_entry
                new_songs_to_download.append((song_id, track_name, artists_string))
                song_ids.append(song_id)
                print(f"   ✅ New song added: {track_name} by {artists_string}")
            
            processed_tracks.append(track_info)
            
        except Exception as e:
            print(f"   ❌ Error processing track: {e}")
            continue
    
    # Create playlist entry
    playlist_entry = {
        'name': playlist_name,
        'description': f'All tracks from {artist_name} discography',
        'song_ids': song_ids,
        'total_tracks': len(song_ids),
        'created_at': datetime.now().isoformat(),
        'source': 'spotify_artist_discography',
        'source_id': Config.ARTIST_ID
    }
    
    song_manager.existing_playlists[playlist_id] = playlist_entry
    
    # Store main artist info
    if main_artist_uri:
        song_manager.store_artist_info(main_artist_uri, artist_name, playlist_id)
    
    # Save databases
    save_databases(song_manager)
    
    print(f"\n📊 Processing Summary:")
    print(f"   ✅ Total tracks processed: {len(processed_tracks)}")
    print(f"   🔄 Existing songs updated: {existing_songs_updated}")
    print(f"   🆕 New songs to download: {len(new_songs_to_download)}")
    print(f"   📋 Created playlist: {playlist_name}")
    print(f"   🆔 Playlist ID: {playlist_id}")
    
    # Download new songs
    if new_songs_to_download:
        print(f"\n🎵 Starting downloads for {len(new_songs_to_download)} new songs...")
        
        for song_id, track_name, artists_string in new_songs_to_download:
            try:
                print(f"\n📥 Downloading: {track_name} by {artists_string}")
                
                if download_song(track_name, artists_string, song_id, song_manager.songs_folder):
                    # Update download status
                    song_manager.existing_songs[song_id]['download_info'].update({
                        'status': 'completed',
                        'file_path': str(song_manager.songs_folder / f"{song_id}.mp3"),
                        'downloaded_at': datetime.now().isoformat()
                    })
                    print(f"   ✅ Successfully downloaded: {track_name}")
                else:
                    # Mark as failed
                    song_manager.existing_songs[song_id]['download_info']['status'] = 'failed'
                    print(f"   ❌ Failed to download: {track_name}")
                
                # Small delay between downloads
                time.sleep(Config.DOWNLOAD_DELAY)
                
            except Exception as e:
                print(f"   ❌ Download error for {track_name}: {e}")
                song_manager.existing_songs[song_id]['download_info']['status'] = 'failed'
        
        # Save updated databases after downloads
        save_databases(song_manager)
        print(f"\n💾 Updated databases with download status")

def save_databases(song_manager: SmartSongManager):
    """Save songs, playlists, and artists databases"""
    try:
        # Save songs database
        songs_db = {
            'songs': song_manager.existing_songs,
            'total_songs': len(song_manager.existing_songs),
            'last_updated': datetime.now().isoformat()
        }
        
        songs_db_path = song_manager.metadata_folder / 'songs_database.json'
        with open(songs_db_path, 'w', encoding='utf-8') as f:
            json.dump(songs_db, f, indent=2, ensure_ascii=False)
        
        # Save playlists database
        playlists_db = {
            'playlists': song_manager.existing_playlists,
            'total_playlists': len(song_manager.existing_playlists),
            'last_updated': datetime.now().isoformat()
        }
        
        playlists_db_path = song_manager.metadata_folder / 'playlists_database.json'
        with open(playlists_db_path, 'w', encoding='utf-8') as f:
            json.dump(playlists_db, f, indent=2, ensure_ascii=False)
        
        # Save artists database
        artists_db = {
            'artists': song_manager.existing_artists,
            'total_artists': len(song_manager.existing_artists),
            'last_updated': datetime.now().isoformat()
        }
        
        artists_db_path = song_manager.metadata_folder / 'artists_database.json'
        with open(artists_db_path, 'w', encoding='utf-8') as f:
            json.dump(artists_db, f, indent=2, ensure_ascii=False)
        
        # Save song-playlist mapping
        song_playlist_mapping = {}
        for song_id, song_info in song_manager.existing_songs.items():
            playlists = song_info.get('playlists', [])
            song_playlist_mapping[song_id] = playlists
        
        mapping_db = {
            'mapping': song_playlist_mapping,
            'last_updated': datetime.now().isoformat()
        }
        
        mapping_db_path = song_manager.metadata_folder / 'song_playlist_mapping.json'
        with open(mapping_db_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_db, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Saved databases:")
        print(f"   📚 Songs: {len(song_manager.existing_songs)}")
        print(f"   📋 Playlists: {len(song_manager.existing_playlists)}")
        print(f"   🎤 Artists: {len(song_manager.existing_artists)}")
        
    except Exception as e:
        print(f"❌ Error saving databases: {e}")

def main():
    """Main function to run the artist discography scraper"""
    global stop_capture, all_artist_tracks, captured_data
    
    print("🎵 Spotify Artist Discography Scraper")
    print("=" * 50)
    
    # Check prerequisites
    if not check_prerequisites():
        print("❌ Prerequisites not met. Please install required tools.")
        return
    
    # Get artist ID from user
    artist_id = get_artist_id_from_user()
    Config.ARTIST_ID = artist_id
    
    # Construct artist discography URL
    artist_url = f"https://open.spotify.com/artist/{artist_id}/discography/all"
    
    print(f"🔗 Artist URL: {artist_url}")
    print("\n📋 Instructions:")
    print("1. A browser will open with the artist discography page")
    print("2. The script will automatically scroll and capture track data")
    print("3. Wait for the message 'Capture completed' before closing")
    print("4. Press Enter to continue...")
    input()
    
    # Setup browser
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=options)
    driver.request_interceptor = request_interceptor
    driver.response_interceptor = response_interceptor
    
    try:
        print("🌐 Opening browser...")
        driver.get(artist_url)
        
        print("⏳ Waiting for page to load...")
        time.sleep(5)
        
        # Start auto-scrolling in a separate thread
        scroll_thread = threading.Thread(target=auto_scroll, args=(driver,))
        scroll_thread.start()
        
        print("\n⌨️  Press Enter when you want to stop capture and process the data...")
        input()
        
        stop_capture = True
        scroll_thread.join()
        
        print(f"\n📊 Capture Summary:")
        print(f"   🌐 API Requests: {len(captured_data)}")
        print(f"   🎵 Tracks Found: {len(all_artist_tracks)}")
        
        if all_artist_tracks:
            # Get artist name from first track
            first_track = all_artist_tracks[0]
            artists_data = safe_get(first_track, 'artists', 'items', default=[])
            if artists_data:
                artist_name = safe_get(artists_data[0], 'profile', 'name', default='Unknown Artist')
            else:
                artist_name = f"Artist_{artist_id}"
            
            print(f"🎤 Artist: {artist_name}")
            
            # Process tracks
            process_artist_tracks(artist_name)
        else:
            print("❌ No tracks were found. Make sure the page loaded correctly.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        print("🔄 Closing browser...")
        driver.quit()
        print("✅ Browser closed")

# Run the main function
if __name__ == "__main__":
    main()
