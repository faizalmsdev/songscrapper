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
    SPOTIFY_URL = ""  # Will be set by user input
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
    
    # Multi-playlist settings
    BATCH_PROCESSING_FILE = "playlist_batch.json"
    PAUSE_BETWEEN_PLAYLISTS = True

# === GLOBAL VARIABLES ===
captured_data = []
all_playlist_items = []
seen_requests = set()
stop_capture = False
auto_scroll_active = False
download_paused = False
batch_download_cancelled = False

# === BATCH PROCESSING CLASSES ===
class PlaylistBatch:
    def __init__(self, batch_file: str = None):
        self.batch_file = batch_file or Config.BATCH_PROCESSING_FILE
        self.playlists = []  # List of {'name': str, 'url': str, 'status': str}
        self.current_playlist_index = 0
        self.load_batch_file()
    
    def load_batch_file(self):
        """Load existing batch file or create new one"""
        if os.path.exists(self.batch_file):
            try:
                with open(self.batch_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.playlists = data.get('playlists', [])
                    self.current_playlist_index = data.get('current_index', 0)
                print(f"📋 Loaded {len(self.playlists)} playlists from batch file")
            except Exception as e:
                print(f"⚠️  Error loading batch file: {e}")
                self.playlists = []
        else:
            print("🆕 Creating new batch processing file")
    
    def add_playlist(self, name: str, url: str):
        """Add a playlist to the batch"""
        playlist_entry = {
            'name': sanitize_filename(name),
            'url': url.strip(),
            'status': 'pending',
            'added_at': datetime.now().isoformat(),
            'processed_at': None,
            'tracks_count': 0,
            'success_count': 0,
            'error': None
        }
        self.playlists.append(playlist_entry)
    
    def save_batch_file(self):
        """Save batch file with current status"""
        batch_data = {
            'playlists': self.playlists,
            'current_index': self.current_playlist_index,
            'total_playlists': len(self.playlists),
            'last_updated': datetime.now().isoformat(),
            'batch_created': datetime.now().isoformat() if not os.path.exists(self.batch_file) else None
        }
        
        with open(self.batch_file, 'w', encoding='utf-8') as f:
            json.dump(batch_data, f, indent=2, ensure_ascii=False)
    
    def get_current_playlist(self):
        """Get current playlist to process"""
        if self.current_playlist_index < len(self.playlists):
            return self.playlists[self.current_playlist_index]
        return None
    
    def mark_playlist_completed(self, tracks_count: int, success_count: int, error: str = None):
        """Mark current playlist as completed"""
        if self.current_playlist_index < len(self.playlists):
            self.playlists[self.current_playlist_index].update({
                'status': 'completed' if error is None else 'failed',
                'processed_at': datetime.now().isoformat(),
                'tracks_count': tracks_count,
                'success_count': success_count,
                'error': error
            })
            self.current_playlist_index += 1
            self.save_batch_file()
    
    def get_batch_summary(self):
        """Get batch processing summary"""
        completed = len([p for p in self.playlists if p['status'] == 'completed'])
        failed = len([p for p in self.playlists if p['status'] == 'failed'])
        pending = len([p for p in self.playlists if p['status'] == 'pending'])
        
        return {
            'total': len(self.playlists),
            'completed': completed,
            'failed': failed,
            'pending': pending,
            'current_index': self.current_playlist_index
        }

# === PAUSE/RESUME CONTROL ===
class DownloadController:
    def __init__(self):
        self.paused = False
        self.cancelled = False
        self.pause_lock = threading.Lock()
    
    def pause(self):
        """Pause the download process"""
        with self.pause_lock:
            self.paused = True
            print("⏸️  Download process paused")
    
    def resume(self):
        """Resume the download process"""
        with self.pause_lock:
            self.paused = False
            print("▶️  Download process resumed")
    
    def cancel(self):
        """Cancel the entire batch process"""
        with self.pause_lock:
            self.cancelled = True
            print("🛑 Batch download process cancelled")
    
    def check_pause(self):
        """Check if process should be paused and wait if necessary"""
        while self.paused and not self.cancelled:
            time.sleep(0.5)
        return not self.cancelled
    
    def is_cancelled(self):
        """Check if process is cancelled"""
        return self.cancelled

# === SMART DEDUPLICATION CLASS ===
class SmartSongManager:
    def __init__(self, consolidated_folder: str = "consolidated_music"):
        self.consolidated_folder = Path(consolidated_folder)
        self.songs_folder = self.consolidated_folder / "songs"
        self.metadata_folder = self.consolidated_folder / "metadata"
        
        # Create directories if they don't exist
        self.songs_folder.mkdir(parents=True, exist_ok=True)
        self.metadata_folder.mkdir(parents=True, exist_ok=True)
        
        # Load existing song database
        self.existing_songs = {}  # song_id -> song_info
        self.uri_to_song_id = {}  # track_uri -> song_id
        self.name_artist_to_song_id = {}  # normalized_name_artist -> song_id
        
        self.load_existing_database()
    
    def load_existing_database(self):
        """Load existing songs database for duplicate checking"""
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
        else:
            print("🆕 No existing songs database found - starting fresh")
    
    def generate_song_id(self, track_name: str, artists: str) -> str:
        """Generate a unique ID for a song based on track name and artists"""
        clean_string = f"{track_name}_{artists}".lower()
        clean_string = re.sub(r'[^a-z0-9_]', '', clean_string)
        hash_object = hashlib.md5(clean_string.encode())
        return f"song_{hash_object.hexdigest()[:12]}"
    
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
    
    def get_consolidated_song_path(self, song_id: str, extension: str = ".mp3") -> Path:
        """Get the path where the consolidated song should be stored"""
        return self.songs_folder / f"{song_id}{extension}"

# === ERROR HANDLING UTILITIES ===
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

def validate_track_data(track_info):
    """Validate if track data is sufficient for processing"""
    track_name = track_info.get('track_name', '').strip()
    artists_string = track_info.get('artists_string', '').strip()
    
    if not track_name or len(track_name) < Config.MIN_TRACK_NAME_LENGTH:
        return False, "Track name is empty or too short"
    
    if not artists_string or len(artists_string) < Config.MIN_ARTIST_NAME_LENGTH:
        return False, "Artist name is empty or too short"
    
    if track_name.lower() in ['unknown track', 'unknown', '']:
        return False, "Track name is placeholder value"
    
    if artists_string.lower() in ['unknown artist', 'unknown', '']:
        return False, "Artist name is placeholder value"
    
    return True, "Valid"

def log_skipped_track(track_info, reason, log_file):
    """Log information about skipped tracks"""
    try:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"SKIPPED TRACK:\n")
            f.write(f"  Reason: {reason}\n")
            f.write(f"  Track Name: '{track_info.get('track_name', 'N/A')}'\n")
            f.write(f"  Artists: '{track_info.get('artists_string', 'N/A')}'\n")
            f.write(f"  Album: '{track_info.get('album_name', 'N/A')}'\n")
            f.write(f"  URI: '{track_info.get('track_uri', 'N/A')}'\n")
            f.write(f"  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("-" * 50 + "\n")
    except Exception as e:
        print(f"   ⚠️  Failed to log skipped track: {e}")

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

def download_cover_art(cover_url, output_path):
    """Download cover art image with enhanced error handling"""
    try:
        if not cover_url or not str(cover_url).strip():
            return False
            
        response = requests.get(cover_url, timeout=10)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        return True
    except Exception as e:
        print(f"   ⚠️  Failed to download cover art: {e}")
        return False

def get_best_cover_art_url(cover_sources, preferred_size=640):
    """Get the best cover art URL from sources with error handling"""
    try:
        if not cover_sources or not isinstance(cover_sources, list):
            return None
        
        # Try to find preferred size
        for source in cover_sources:
            if isinstance(source, dict) and source.get('width') == preferred_size:
                url = source.get('url')
                if url:
                    return url
        
        # If preferred size not found, get the largest available
        valid_sources = [s for s in cover_sources if isinstance(s, dict) and s.get('width') and s.get('url')]
        if valid_sources:
            largest = max(valid_sources, key=lambda x: x.get('width', 0))
            return largest.get('url')
        
        return None
    except Exception as e:
        print(f"   ⚠️  Error getting cover art URL: {e}")
        return None

# === BATCH SETUP FUNCTIONS ===
def setup_batch_processing():
    """Setup batch processing by collecting multiple playlist URLs and names"""
    print("🎵 Multi-Playlist Batch Setup")
    print("=" * 50)
    
    batch = PlaylistBatch()
    
    # Check if there's an existing batch file
    if batch.playlists:
        print(f"📋 Found existing batch with {len(batch.playlists)} playlists:")
        for i, playlist in enumerate(batch.playlists, 1):
            status_emoji = {"pending": "⏳", "completed": "✅", "failed": "❌"}
            emoji = status_emoji.get(playlist['status'], "❓")
            print(f"   {emoji} {i}. {playlist['name']} - {playlist['status']}")
        
        print(f"\nCurrent position: {batch.current_playlist_index + 1}/{len(batch.playlists)}")
        
        choice = input("\nWhat would you like to do?\n"
                      "1. Continue from where you left off\n"
                      "2. Add more playlists to existing batch\n"
                      "3. Start fresh (clear existing batch)\n"
                      "4. View batch details\n"
                      "Choice (1-4): ").strip()
        
        if choice == "1":
            return batch
        elif choice == "3":
            batch.playlists = []
            batch.current_playlist_index = 0
            print("🗑️  Cleared existing batch")
        elif choice == "4":
            display_batch_details(batch)
            return setup_batch_processing()  # Recursive call to show menu again
        # If choice == "2" or anything else, continue to add more playlists
    
    print("\n📝 Add Playlists to Batch")
    print("Enter playlist information (press Enter with empty URL to finish)")
    
    while True:
        print(f"\n📋 Current batch size: {len(batch.playlists)}")
        
        url = input("Spotify playlist URL: ").strip()
        if not url:
            break
        
        # Auto-extract playlist name from URL or ask user
        playlist_name = input("Playlist name (or press Enter to auto-generate): ").strip()
        
        if not playlist_name:
            try:
                # Try to extract from URL
                playlist_id = url.split('/')[-1].split('?')[0]
                playlist_name = f"playlist_{playlist_id}"
            except:
                playlist_name = f"playlist_{len(batch.playlists) + 1}"
        
        batch.add_playlist(playlist_name, url)
        print(f"✅ Added: {playlist_name}")
    
    if not batch.playlists:
        print("❌ No playlists added to batch")
        return None
    
    # Save batch file
    batch.save_batch_file()
    
    print(f"\n📋 Batch Summary:")
    print(f"   Total playlists: {len(batch.playlists)}")
    print(f"   Batch file: {batch.batch_file}")
    
    return batch

def display_batch_details(batch: PlaylistBatch):
    """Display detailed information about the batch"""
    print(f"\n📋 Batch Details:")
    print(f"   File: {batch.batch_file}")
    print(f"   Total playlists: {len(batch.playlists)}")
    print(f"   Current position: {batch.current_playlist_index + 1}/{len(batch.playlists)}")
    
    summary = batch.get_batch_summary()
    print(f"   Completed: {summary['completed']}")
    print(f"   Failed: {summary['failed']}")
    print(f"   Pending: {summary['pending']}")
    
    print(f"\n📜 Playlist List:")
    for i, playlist in enumerate(batch.playlists, 1):
        status_emoji = {"pending": "⏳", "completed": "✅", "failed": "❌"}
        emoji = status_emoji.get(playlist['status'], "❓")
        
        print(f"   {emoji} {i}. {playlist['name']}")
        print(f"      URL: {playlist['url']}")
        print(f"      Status: {playlist['status']}")
        if playlist.get('processed_at'):
            print(f"      Processed: {playlist['processed_at']}")
            print(f"      Tracks: {playlist.get('tracks_count', 0)}")
            print(f"      Success: {playlist.get('success_count', 0)}")
        if playlist.get('error'):
            print(f"      Error: {playlist['error']}")

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

def is_playlist_items_response(parsed_response):
    """Check if the response contains playlist items data"""
    try:
        if isinstance(parsed_response, dict):
            data = parsed_response.get('data', {})
            playlist_v2 = data.get('playlistV2', {})
            content = playlist_v2.get('content', {})
            return content.get('__typename') == 'PlaylistItemsPage'
        return False
    except:
        return False

def extract_items_from_response(parsed_response):
    """Extract the items array from playlist response"""
    try:
        if isinstance(parsed_response, dict):
            data = parsed_response.get('data', {})
            playlist_v2 = data.get('playlistV2', {})
            content = playlist_v2.get('content', {})
            items = content.get('items', [])
            return items
    except:
        pass
    return []

def extract_pagination_info(parsed_response):
    """Extract pagination information from the response"""
    try:
        if isinstance(parsed_response, dict):
            data = parsed_response.get('data', {})
            playlist_v2 = data.get('playlistV2', {})
            content = playlist_v2.get('content', {})
            paging_info = content.get('pagingInfo', {})
            items = content.get('items', [])
            
            return {
                'limit': paging_info.get('limit', 0),
                'offset': paging_info.get('offset', 0),
                'totalCount': paging_info.get('totalCount', 0),
                'items_in_response': len(items)
            }
    except:
        pass
    return None

def auto_scroll(driver):
    """Auto-scroll the page to load all playlist items"""
    global stop_capture, auto_scroll_active
    auto_scroll_active = True
    scroll_count = 0
    
    print("🔄 Starting auto-scroll...")
    
    try:
        time.sleep(3)
        
        while not stop_capture and Config.AUTO_SCROLL_ENABLED:
            try:
                current_scroll = driver.execute_script("return window.pageYOffset;")
                page_height = driver.execute_script("return document.body.scrollHeight;")
                window_height = driver.execute_script("return window.innerHeight;")
                
                driver.execute_script(f"window.scrollBy(0, {Config.SCROLL_PIXELS});")
                scroll_count += 1
                
                print(f"🔽 Scroll #{scroll_count} - Position: {current_scroll}px")
                
                time.sleep(Config.SCROLL_PAUSE_TIME)
                
                new_scroll = driver.execute_script("return window.pageYOffset;")
                if new_scroll == current_scroll or new_scroll + window_height >= page_height:
                    print("📍 Reached bottom of page, continuing to monitor...")
                    time.sleep(Config.SCROLL_PAUSE_TIME * 2)
                
            except Exception as e:
                print(f"[!] Error during scrolling: {e}")
                time.sleep(Config.SCROLL_PAUSE_TIME)
                
    except Exception as e:
        print(f"[!] Error in auto-scroll thread: {e}")
    
    auto_scroll_active = False

def capture_requests(driver):
    """Capture playlist requests from Spotify"""
    global stop_capture, all_playlist_items
    playlist_items_count = 0
    
    while not stop_capture:
        for request in driver.requests:
            if (request.response and 
                request.id not in seen_requests and 
                Config.TARGET_API_URL in request.url):
                
                seen_requests.add(request.id)
                
                try:
                    response_body = decode_response_body(request.response)
                    parsed_response = parse_json_response(response_body)
                    
                    if is_playlist_items_response(parsed_response):
                        playlist_items_count += 1
                        pagination_info = extract_pagination_info(parsed_response)
                        items_in_response = extract_items_from_response(parsed_response)
                        
                        print(f"🎯 Captured Playlist Items Request #{playlist_items_count}")
                        print(f"   URL: {request.url}")
                        print(f"   Status: {request.response.status_code}")
                        
                        if pagination_info:
                            print(f"   📄 Pagination: Offset {pagination_info['offset']}, "
                                  f"Limit {pagination_info['limit']}, "
                                  f"Items: {pagination_info['items_in_response']}, "
                                  f"Total: {pagination_info['totalCount']}")
                        
                        print(f"   🎵 Items extracted: {len(items_in_response)}")
                        
                        if items_in_response:
                            all_playlist_items.extend(items_in_response)
                            print(f"   📚 Total items collected: {len(all_playlist_items)}")
                        
                except Exception as e:
                    print(f"[!] Error processing request: {e}")
        
        time.sleep(0.5)

def listen_for_commands():
    """Listen for user commands during capture"""
    global stop_capture, Config
    while True:
        print("\nCommands:")
        print("  'stop' - Stop capturing and proceed to processing")
        print("  'scroll on' - Enable auto-scrolling")
        print("  'scroll off' - Disable auto-scrolling")
        print("  'status' - Show current status")
        print("  'items' - Show total items collected")
        
        user_input = input(">>> ").strip().lower()
        
        if user_input == "stop":
            stop_capture = True
            break
        elif user_input == "scroll on":
            Config.AUTO_SCROLL_ENABLED = True
            print("✅ Auto-scrolling enabled")
        elif user_input == "scroll off":
            Config.AUTO_SCROLL_ENABLED = False
            print("🛑 Auto-scrolling disabled")
        elif user_input == "status":
            print(f"📊 Status:")
            print(f"   Total items collected: {len(all_playlist_items)}")
            print(f"   Auto-scroll: {'ON' if Config.AUTO_SCROLL_ENABLED else 'OFF'}")
            print(f"   Auto-scroll active: {'YES' if auto_scroll_active else 'NO'}")
        elif user_input == "items":
            print(f"📚 Total items collected: {len(all_playlist_items)}")
            if all_playlist_items:
                print(f"   Latest item example keys: {list(all_playlist_items[-1].keys()) if all_playlist_items[-1] else 'None'}")

def listen_for_download_commands(controller: DownloadController):
    """Listen for commands during download process"""
    print("\n🎮 Download Control Commands:")
    print("  'pause' - Pause download process")
    print("  'resume' - Resume download process") 
    print("  'cancel' - Cancel entire batch download")
    print("  'status' - Show current download status")
    
    while not controller.is_cancelled():
        try:
            user_input = input("Download>>> ").strip().lower()
            
            if user_input == "pause":
                controller.pause()
            elif user_input == "resume":
                controller.resume()
            elif user_input == "cancel":
                controller.cancel()
                break
            elif user_input == "status":
                status = "⏸️  PAUSED" if controller.paused else "▶️  RUNNING"
                print(f"📊 Download Status: {status}")
            elif user_input == "help":
                print("Available commands: pause, resume, cancel, status, help")
        except EOFError:
            # Handle Ctrl+C or EOF
            break
        except Exception as e:
            print(f"Error processing command: {e}")
        
        time.sleep(0.1)

# === ENHANCED TRACK EXTRACTION FUNCTIONS ===
def extract_enhanced_track_info(items, cover_art_folder, song_manager=None):
    """Extract comprehensive track information with smart deduplication"""
    tracks_info = []
    skipped_count = 0
    error_count = 0
    existing_found_count = 0
    
    print(f"🎵 Processing {len(items)} items with enhanced metadata and smart deduplication...")
    
    # Create skipped tracks log file
    skipped_log_file = os.path.join(os.path.dirname(cover_art_folder), "skipped_tracks.log")
    
    for i, item in enumerate(items, 1):
        try:
            # Safety check for item structure
            if not isinstance(item, dict):
                skipped_count += 1
                print(f"   ⏭️  [{i}] Skipped: Invalid item structure")
                continue
            
            item_v2 = safe_get(item, 'itemV2', default={})
            
            # Check if it's a track
            if safe_get(item_v2, '__typename') != 'TrackResponseWrapper':
                skipped_count += 1
                continue
                
            track_data = safe_get(item_v2, 'data', default={})
            
            # Basic track info with safe extraction
            track_name = safe_get(track_data, 'name', default='').strip()
            track_uri = safe_get(track_data, 'uri', default='')
            
            # Artists info with safe extraction
            artists_data = safe_get(track_data, 'artists', 'items', default=[])
            artist_names = []
            artist_uris = []
            
            if isinstance(artists_data, list):
                for artist in artists_data:
                    if isinstance(artist, dict):
                        artist_name = safe_get(artist, 'profile', 'name', default='').strip()
                        if artist_name and artist_name not in artist_names:
                            artist_names.append(artist_name)
                            artist_uris.append(safe_get(artist, 'uri', default=''))
            
            # Create artists string
            artists_string = ', '.join(artist_names) if artist_names else 'Unknown Artist'
            
            # Album info with safe extraction
            album_data = safe_get(track_data, 'albumOfTrack', default={})
            album_name = safe_get(album_data, 'name', default='Unknown Album').strip()
            album_uri = safe_get(album_data, 'uri', default='')
            
            # Create preliminary track info for validation
            preliminary_track_info = {
                'track_name': track_name,
                'artists_string': artists_string,
                'album_name': album_name,
                'track_uri': track_uri
            }
            
            # Validate track data
            is_valid, validation_reason = validate_track_data(preliminary_track_info)
            
            if not is_valid and Config.SKIP_INVALID_TRACKS:
                skipped_count += 1
                print(f"   ⏭️  [{i}] Skipped: {validation_reason}")
                print(f"      Track: '{track_name}' by '{artists_string}'")
                log_skipped_track(preliminary_track_info, validation_reason, skipped_log_file)
                continue
            
            # Cover art info with safe extraction
            cover_sources = safe_get(album_data, 'coverArt', 'sources', default=[])
            cover_url = get_best_cover_art_url(cover_sources, Config.COVER_ART_SIZE)
            cover_filename = None
            
            # Check if song already exists in consolidated database
            existing_song_info = None
            song_id = None
            skip_download = False
            
            if song_manager and Config.ENABLE_SMART_DEDUPLICATION:
                existing_result = song_manager.find_existing_song(preliminary_track_info)
                if existing_result:
                    song_id, existing_song_info = existing_result
                    skip_download = True
                    existing_found_count += 1
                    print(f"   🔄 [{i}] Found existing song: '{track_name}' by '{artists_string}'")
                    print(f"      Using existing song_id: {song_id}")
            
            # Generate song_id if not found in existing database
            if not song_id:
                song_id = song_manager.generate_song_id(track_name, artists_string) if song_manager else f"song_{i:06d}"
            
            # Download cover art if available and not skipping
            if cover_url and Config.DOWNLOAD_COVER_ART and not skip_download:
                try:
                    safe_track_name = sanitize_filename(f"{track_name}_{artist_names[0] if artist_names else 'unknown'}")
                    cover_filename = f"{safe_track_name}_cover.jpg"
                    cover_path = os.path.join(cover_art_folder, cover_filename)
                    
                    if download_cover_art(cover_url, cover_path):
                        print(f"   🖼️  Downloaded cover art: {cover_filename}")
                    else:
                        cover_filename = None
                except Exception as e:
                    print(f"   ⚠️  Cover art download failed: {e}")
                    cover_filename = None
            elif existing_song_info:
                # Use existing cover art filename if available
                cover_filename = existing_song_info.get('metadata', {}).get('cover_art_filename')
            
            # Track duration with safe extraction
            duration_ms = safe_get(track_data, 'trackDuration', 'totalMilliseconds', default=0)
            try:
                duration_ms = int(duration_ms) if duration_ms else 0
            except (ValueError, TypeError):
                duration_ms = 0
            
            duration_seconds = duration_ms / 1000 if duration_ms else 0
            
            # Additional metadata with safe extraction
            track_number = safe_get(track_data, 'trackNumber', default=0)
            disc_number = safe_get(track_data, 'discNumber', default=1)
            playcount = safe_get(track_data, 'playcount', default='0')
            content_rating = safe_get(track_data, 'contentRating', 'label', default='NONE')
            
            # Added info with safe extraction
            added_at = safe_get(item, 'addedAt', 'isoString', default='')
            added_by_data = safe_get(item, 'addedBy', 'data', default={})
            added_by_name = safe_get(added_by_data, 'name', default='Unknown')
            added_by_username = safe_get(added_by_data, 'username', default='')
            
            # Added by avatar
            added_by_avatar_sources = safe_get(added_by_data, 'avatar', 'sources', default=[])
            added_by_avatar_url = get_best_cover_art_url(added_by_avatar_sources, 300)
            
            # Format added date safely
            added_at_formatted = ''
            if added_at:
                try:
                    added_at_formatted = datetime.fromisoformat(added_at.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    print(f"   ⚠️  Date formatting failed: {e}")
                    added_at_formatted = added_at
            
            # Create final track info
            track_info = {
                # Basic info
                'track_name': track_name,
                'track_uri': track_uri,
                'artists': artist_names,
                'artist_uris': artist_uris,
                'artists_string': artists_string,
                
                # Album info
                'album_name': album_name,
                'album_uri': album_uri,
                
                # Cover art
                'cover_art_url': cover_url,
                'cover_art_filename': cover_filename,
                'cover_art_sources': cover_sources,
                
                # Duration and track info
                'duration_ms': duration_ms,
                'duration_seconds': duration_seconds,
                'duration_formatted': f"{int(duration_seconds//60)}:{int(duration_seconds%60):02d}" if duration_seconds else "0:00",
                'track_number': track_number,
                'disc_number': disc_number,
                
                # Metadata
                'playcount': playcount,
                'content_rating': content_rating,
                
                # Added info
                'added_at': added_at,
                'added_at_formatted': added_at_formatted,
                'added_by_name': added_by_name,
                'added_by_username': added_by_username,
                'added_by_avatar_url': added_by_avatar_url,
                
                # Processing info
                'processed_at': datetime.now().isoformat(),
                
                # Smart deduplication info
                'song_id': song_id,
                'skip_download': skip_download,
                'existing_song_found': existing_song_info is not None
            }
            
            tracks_info.append(track_info)
            
            # Show progress every 50 items or for special cases
            if i % 50 == 0 or not is_valid or skip_download:
                print(f"✅ Processed {i}/{len(items)} items... (Valid tracks: {len(tracks_info)}, Existing: {existing_found_count})")
                
        except Exception as e:
            error_count += 1
            print(f"⚠️  Error processing item {i}: {e}")
            
            # Log the error with available information
            try:
                error_info = {
                    'track_name': 'ERROR_PROCESSING',
                    'artists_string': 'ERROR_PROCESSING',
                    'album_name': 'ERROR_PROCESSING',
                    'track_uri': '',
                    'error': str(e)
                }
                log_skipped_track(error_info, f"Processing error: {str(e)}", skipped_log_file)
            except:
                pass
            
            continue
    
    print(f"✅ Successfully extracted {len(tracks_info)} valid tracks with metadata")
    print(f"🔄 Found {existing_found_count} existing songs (will skip download)")
    if skipped_count > 0:
        print(f"⏭️  Skipped {skipped_count} invalid/problematic items")
    if error_count > 0:
        print(f"⚠️  {error_count} items had processing errors")
    
    if skipped_count > 0 or error_count > 0:
        print(f"📋 Detailed skip log saved to: {skipped_log_file}")
    
    return tracks_info

# === SMART DOWNLOAD FUNCTIONS ===
def search_and_download_audio_smart(track_info, output_folder, song_manager=None, controller=None):
    """Search for and download audio with smart deduplication and pause support"""
    import yt_dlp
    
    try:
        # Check for pause/cancel before starting
        if controller and not controller.check_pause():
            return {
                'track_name': track_info.get('track_name', 'Unknown'),
                'artists': track_info.get('artists_string', 'Unknown'),
                'search_query': '',
                'status': 'cancelled',
                'error': 'Download cancelled by user',
                'filename': None,
                'video_title': None,
                'metadata': track_info,
                'song_id': track_info.get('song_id', 'unknown_song')
            }
        
        track_name = track_info.get('track_name', 'Unknown')
        artists_str = track_info.get('artists_string', 'Unknown')
        song_id = track_info.get('song_id', 'unknown_song')
        skip_download = track_info.get('skip_download', False)
        
        # If we should skip download (song already exists), return success with existing info
        if skip_download and song_manager:
            existing_song_path = song_manager.get_consolidated_song_path(song_id)
            if existing_song_path.exists():
                return {
                    'track_name': track_name,
                    'artists': artists_str, 
                    'search_query': f"{track_name} {artists_str}".strip(),
                    'status': 'existing',
                    'error': None,
                    'filename': existing_song_path.name,
                    'video_title': 'Using existing file',
                    'metadata': track_info,
                    'song_id': song_id,
                    'consolidated_path': str(existing_song_path)
                }
        
        # Validate track info before attempting download
        is_valid, reason = validate_track_data(track_info)
        if not is_valid:
            return {
                'track_name': track_name,
                'artists': artists_str,
                'search_query': '',
                'status': 'skipped',
                'error': f'Invalid track data: {reason}',
                'filename': None,
                'video_title': None,
                'metadata': track_info,
                'song_id': song_id
            }
        
        search_query = f"{track_name} {artists_str}".strip()
        safe_filename = sanitize_filename(f"{track_name} - {artists_str}")
        
        # Ensure we have a valid filename
        if not safe_filename or safe_filename == "unknown_file":
            return {
                'track_name': track_name,
                'artists': artists_str,
                'search_query': search_query,
                'status': 'failed',
                'error': 'Could not create valid filename',
                'filename': None,
                'video_title': None,
                'metadata': track_info,
                'song_id': song_id
            }
        
        # Use song_id for filename if available, otherwise use safe_filename
        if song_id and song_id != 'unknown_song':
            final_filename_base = song_id
        else:
            final_filename_base = safe_filename
        
        # Download to temporary location first
        temp_output_path = os.path.join(output_folder, f"temp_{final_filename_base}.%(ext)s")
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'extractaudio': True,
            'audioformat': 'mp3',
            'audioquality': Config.AUDIO_QUALITY,
            'outtmpl': temp_output_path,
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'default_search': 'ytsearch1:',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
        }
        
        result = {
            'track_name': track_name,
            'artists': artists_str,
            'search_query': search_query,
            'status': 'failed',
            'error': None,
            'filename': None,
            'video_title': None,
            'metadata': track_info,
            'song_id': song_id
        }
        
        for attempt in range(Config.MAX_RETRIES):
            # Check for pause/cancel before each attempt
            if controller and not controller.check_pause():
                result['status'] = 'cancelled'
                result['error'] = 'Download cancelled by user'
                return result
            
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    search_results = ydl.extract_info(
                        f"ytsearch1:{search_query}",
                        download=False
                    )
                    
                    if not search_results or 'entries' not in search_results or not search_results['entries']:
                        result['error'] = 'No search results found'
                        continue
                    
                    video_info = search_results['entries'][0]
                    result['video_title'] = video_info.get('title', 'Unknown')
                    
                    # Check for pause/cancel before actual download
                    if controller and not controller.check_pause():
                        result['status'] = 'cancelled'
                        result['error'] = 'Download cancelled by user'
                        return result
                    
                    ydl.download([video_info['webpage_url']])
                    
                    # Find the downloaded file
                    expected_temp_filename = f"temp_{final_filename_base}.mp3"
                    temp_full_path = os.path.join(output_folder, expected_temp_filename)
                    
                    downloaded_file = None
                    if os.path.exists(temp_full_path):
                        downloaded_file = temp_full_path
                    else:
                        # Search for any file starting with temp_
                        for file in os.listdir(output_folder):
                            if file.startswith(f"temp_{final_filename_base}") and file.endswith('.mp3'):
                                downloaded_file = os.path.join(output_folder, file)
                                break
                    
                    if downloaded_file and os.path.exists(downloaded_file):
                        # Move to final location
                        final_filename = f"{final_filename_base}.mp3"
                        final_path = os.path.join(output_folder, final_filename)
                        
                        # If final file already exists, remove it first
                        if os.path.exists(final_path):
                            os.remove(final_path)
                        
                        shutil.move(downloaded_file, final_path)
                        
                        # Also copy to consolidated location if song_manager is available
                        if song_manager and Config.ENABLE_SMART_DEDUPLICATION:
                            consolidated_path = song_manager.get_consolidated_song_path(song_id)
                            consolidated_path.parent.mkdir(parents=True, exist_ok=True)
                            
                            if not consolidated_path.exists():
                                shutil.copy2(final_path, consolidated_path)
                                result['consolidated_path'] = str(consolidated_path)
                        
                        result['status'] = 'success'
                        result['filename'] = final_filename
                        return result
                    
            except Exception as e:
                result['error'] = str(e)
                if attempt < Config.MAX_RETRIES - 1:
                    print(f"   ⚠️  Attempt {attempt + 1} failed: {e}, retrying...")
                    time.sleep(2)
                continue
        
        return result
        
    except Exception as e:
        return {
            'track_name': track_info.get('track_name', 'Unknown'),
            'artists': track_info.get('artists_string', 'Unknown'),
            'search_query': '',
            'status': 'error',
            'error': f'Unexpected error: {str(e)}',
            'filename': None,
            'video_title': None,
            'metadata': track_info,
            'song_id': track_info.get('song_id', 'unknown_song')
        }

# === CONSOLIDATION FUNCTIONS ===
class PlaylistConsolidator:
    def __init__(self, song_manager: SmartSongManager, playlist_name: str):
        self.song_manager = song_manager
        self.playlist_name = playlist_name
        self.playlist_songs = []  # List of song_ids in this playlist
        self.playlist_metadata = {}
        
    def add_song_to_playlist(self, song_id: str, track_info: dict, download_result: dict):
        """Add a song to this playlist's tracking"""
        # Update or create song in consolidated database
        consolidated_path = self.song_manager.get_consolidated_song_path(song_id)
        
        # Create comprehensive song info
        song_info = {
            'song_id': song_id,
            'filename': consolidated_path.name,
            'original_filename': download_result.get('filename', ''),
            'file_path': str(consolidated_path),
            'metadata': track_info,
            'playlists': [self.playlist_name],
            'added_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'download_info': {
                'video_title': download_result.get('video_title', ''),
                'search_query': download_result.get('search_query', ''),
                'download_status': download_result.get('status', ''),
                'downloaded_at': datetime.now().isoformat()
            }
        }
        
        # Check if song already exists in manager
        if song_id in self.song_manager.existing_songs:
            existing_song = self.song_manager.existing_songs[song_id]
            # Add this playlist to existing song if not already there
            if self.playlist_name not in existing_song.get('playlists', []):
                existing_song['playlists'].append(self.playlist_name)
                existing_song['last_updated'] = datetime.now().isoformat()
        else:
            # Add new song to manager
            self.song_manager.existing_songs[song_id] = song_info
            
            # Update lookup tables
            track_uri = track_info.get('track_uri', '')
            if track_uri:
                self.song_manager.uri_to_song_id[track_uri] = song_id
            
            track_name = track_info.get('track_name', '').lower().strip()
            artists = track_info.get('artists_string', '').lower().strip()
            if track_name and artists:
                key = f"{track_name}|{artists}"
                self.song_manager.name_artist_to_song_id[key] = song_id
        
        # Add to this playlist's song list
        if song_id not in self.playlist_songs:
            self.playlist_songs.append(song_id)
    
    def set_playlist_metadata(self, download_info: dict, source_url: str):
        """Set playlist metadata"""
        self.playlist_metadata = {
            'name': self.playlist_name,
            'total_tracks': download_info.get('total_tracks', len(self.playlist_songs)),
            'successful_downloads': download_info.get('successful_downloads', 0),
            'source_url': source_url,
            'timestamp': download_info.get('timestamp', datetime.now().isoformat()),
            'songs': self.playlist_songs.copy(),
            'unique_song_count': len(self.playlist_songs),
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat()
        }
    
    def save_consolidated_metadata(self):
        """Save consolidated metadata files"""
        print("\n💾 Saving consolidated metadata...")
        
        # 1. Update songs database
        songs_db_path = self.song_manager.metadata_folder / 'songs_database.json'
        
        # Load existing data
        existing_songs_db = {'songs': {}, 'stats': {}}
        if songs_db_path.exists():
            try:
                with open(songs_db_path, 'r', encoding='utf-8') as f:
                    existing_songs_db = json.load(f)
            except Exception as e:
                print(f"   ⚠️  Warning loading existing songs database: {e}")
        
        # Merge with current songs
        all_songs = existing_songs_db.get('songs', {}).copy()
        all_songs.update(self.song_manager.existing_songs)
        
        # Save songs database
        songs_db = {
            'songs': all_songs,
            'stats': {
                'total_unique_songs': len(all_songs),
                'generated_at': datetime.now().isoformat(),
                'last_playlist_processed': self.playlist_name
            }
        }
        
        with open(songs_db_path, 'w', encoding='utf-8') as f:
            json.dump(songs_db, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ Updated songs database with {len(all_songs)} total songs")
        
        # 2. Update playlists database
        playlists_db_path = self.song_manager.metadata_folder / 'playlists_database.json'
        
        # Load existing playlists data
        existing_playlists_db = {'playlists': {}, 'stats': {}}
        if playlists_db_path.exists():
            try:
                with open(playlists_db_path, 'r', encoding='utf-8') as f:
                    existing_playlists_db = json.load(f)
            except Exception as e:
                print(f"   ⚠️  Warning loading existing playlists database: {e}")
        
        # Update with current playlist
        all_playlists = existing_playlists_db.get('playlists', {}).copy()
        all_playlists[self.playlist_name] = self.playlist_metadata
        
        # Save playlists database
        playlists_db = {
            'playlists': all_playlists,
            'stats': {
                'total_playlists': len(all_playlists),
                'generated_at': datetime.now().isoformat(),
                'last_updated_playlist': self.playlist_name
            }
        }
        
        with open(playlists_db_path, 'w', encoding='utf-8') as f:
            json.dump(playlists_db, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ Updated playlists database with {len(all_playlists)} total playlists")
        
        # 3. Update song-playlist mapping
        mapping_db_path = self.song_manager.metadata_folder / 'song_playlist_mapping.json'
        
        # Load existing mapping data
        existing_mapping_db = {'song_to_playlists': {}, 'stats': {}}
        if mapping_db_path.exists():
            try:
                with open(mapping_db_path, 'r', encoding='utf-8') as f:
                    existing_mapping_db = json.load(f)
            except Exception as e:
                print(f"   ⚠️  Warning loading existing mapping database: {e}")
        
        # Build complete mapping
        all_mappings = existing_mapping_db.get('song_to_playlists', {}).copy()
        
        # Update mappings for all songs
        for song_id, song_info in all_songs.items():
            playlists = song_info.get('playlists', [])
            if playlists:
                all_mappings[song_id] = playlists
        
        # Save mapping database
        mapping_db = {
            'song_to_playlists': all_mappings,
            'stats': {
                'total_mappings': len(all_mappings),
                'generated_at': datetime.now().isoformat(),
                'last_updated_playlist': self.playlist_name
            }
        }
        
        with open(mapping_db_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_db, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ Updated mapping database with {len(all_mappings)} total mappings")
        print("✅ All consolidated metadata saved successfully!")

# === SINGLE PLAYLIST PROCESSING ===
def process_single_playlist(playlist_info: dict, song_manager: SmartSongManager, batch_download_approved: bool, controller: DownloadController):
    """Process a single playlist from the batch"""
    global all_playlist_items, captured_data, seen_requests, stop_capture, auto_scroll_active
    
    # Reset global variables for this playlist
    all_playlist_items = []
    captured_data = []
    seen_requests = set()
    stop_capture = False
    auto_scroll_active = False
    
    playlist_name = playlist_info['name']
    playlist_url = playlist_info['url']
    
    print(f"\n🎵 Processing Playlist: {playlist_name}")
    print(f"🌐 URL: {playlist_url}")
    print("=" * 80)
    
    # Check if cancelled before starting
    if controller.is_cancelled():
        return {'error': 'Batch process cancelled', 'tracks_count': 0, 'success_count': 0}
    
    # Create output folders for this playlist
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_folder = f"playlist_{sanitize_filename(playlist_name)}_{timestamp}"
    songs_folder = os.path.join(base_folder, "songs")
    cover_art_folder = os.path.join(base_folder, "cover_art")
    os.makedirs(songs_folder, exist_ok=True)
    os.makedirs(cover_art_folder, exist_ok=True)
    
    print(f"📁 Playlist folder: {base_folder}")
    
    try:
        # === PHASE 1: CAPTURE PLAYLIST DATA ===
        print("\n" + "="*60)
        print("PHASE 1: Capturing Spotify Playlist Data")
        print("="*60)
        
        # Setup browser
        print("🔄 Launching browser...")
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        driver = webdriver.Chrome(options=options)
        driver.requests.clear()
        driver.get(playlist_url)
        
        print(f"🌐 Opened playlist: {playlist_url}")
        print(f"🎯 Monitoring for PlaylistItemsPage requests to: {Config.TARGET_API_URL}")
        print("🟢 The script will automatically scroll and capture playlist items.")
        
        # Start capture and scroll threads
        capture_thread = threading.Thread(target=capture_requests, args=(driver,))
        capture_thread.daemon = True
        capture_thread.start()
        
        if Config.AUTO_SCROLL_ENABLED:
            scroll_thread = threading.Thread(target=auto_scroll, args=(driver,))
            scroll_thread.daemon = True
            scroll_thread.start()
        
        # Start command listener for this playlist only
        command_thread = threading.Thread(target=listen_for_commands)
        command_thread.daemon = True
        command_thread.start()
        
        # Wait for capture to complete
        while not stop_capture and not controller.is_cancelled():
            time.sleep(1)
        
        # Wait a bit for threads to finish
        time.sleep(2)
        driver.quit()
        
        if controller.is_cancelled():
            return {'error': 'Batch process cancelled', 'tracks_count': 0, 'success_count': 0}
        
        if not all_playlist_items:
            return {'error': 'No playlist items captured', 'tracks_count': 0, 'success_count': 0}
        
        print(f"✅ Captured {len(all_playlist_items)} playlist items")
        
        # === PHASE 2: EXTRACT ENHANCED TRACK INFORMATION ===
        print("\n" + "="*60)
        print("PHASE 2: Extracting Track Information with Smart Deduplication")
        print("="*60)
        
        tracks = extract_enhanced_track_info(all_playlist_items, cover_art_folder, song_manager)
        
        if not tracks:
            return {'error': 'No valid tracks extracted', 'tracks_count': 0, 'success_count': 0}
        
        # Count existing vs new tracks
        existing_tracks = [t for t in tracks if t.get('skip_download', False)]
        new_tracks = [t for t in tracks if not t.get('skip_download', False)]
        
        print(f"\n📊 Track Analysis for {playlist_name}:")
        print(f"   🔄 Existing songs found: {len(existing_tracks)}")
        print(f"   🆕 New songs to download: {len(new_tracks)}")
        print(f"   📚 Total valid tracks: {len(tracks)}")
        
        # Save enhanced track information
        tracks_file = os.path.join(base_folder, "enhanced_tracks_metadata.json")
        tracks_data = {
            'extraction_info': {
                'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'playlist_name': playlist_name,
                'total_tracks': len(tracks),
                'existing_songs_found': len(existing_tracks),
                'new_songs_to_download': len(new_tracks),
                'source_url': playlist_url,
                'cover_art_downloaded': Config.DOWNLOAD_COVER_ART,
                'cover_art_folder': cover_art_folder,
                'smart_deduplication_enabled': Config.ENABLE_SMART_DEDUPLICATION,
                'consolidated_folder': Config.CONSOLIDATED_FOLDER
            },
            'tracks': tracks
        }
        
        with open(tracks_file, 'w', encoding='utf-8') as f:
            json.dump(tracks_data, f, indent=2, ensure_ascii=False)
        
        print(f"📄 Enhanced track metadata saved to: {tracks_file}")
        
        # === PHASE 3: SMART DOWNLOAD WITH DEDUPLICATION ===
        print("\n" + "="*60)
        print("PHASE 3: Smart Download with Deduplication")
        print("="*60)
        
        if len(existing_tracks) > 0:
            print(f"🔄 {len(existing_tracks)} songs already exist and will be reused")
            print("   (No download required for these tracks)")
        
        # Initialize playlist consolidator
        consolidator = PlaylistConsolidator(song_manager, playlist_name)
        
        # Process all tracks (existing and new)
        successful_downloads = 0
        failed_downloads = 0
        skipped_downloads = 0
        existing_reused = 0
        cancelled_downloads = 0
        download_log = []
        
        log_file = os.path.join(base_folder, "download_log.txt")
        
        for i, track in enumerate(tracks, 1):
            # Check for cancellation
            if controller.is_cancelled():
                print(f"\n🛑 Download cancelled by user at track {i}/{len(tracks)}")
                break
            
            # Check for pause
            if not controller.check_pause():
                print(f"\n🛑 Download cancelled by user at track {i}/{len(tracks)}")
                break
            
            try:
                # Display track info
                track_name = track.get('track_name', 'Unknown Track')
                artists_string = track.get('artists_string', 'Unknown Artist')
                album_name = track.get('album_name', 'Unknown Album')
                duration_formatted = track.get('duration_formatted', '0:00')
                song_id = track.get('song_id', 'unknown_song')
                skip_download = track.get('skip_download', False)
                
                print(f"\n🎵 [{i}/{len(tracks)}] {track_name} - {artists_string}")
                print(f"   📀 Album: {album_name}")
                print(f"   🆔 Song ID: {song_id}")
                
                if duration_formatted and duration_formatted != '0:00':
                    print(f"   ⏱️  Duration: {duration_formatted}")
                
                if skip_download:
                    print(f"   🔄 Using existing song (skipping download)")
                    # Create a result for existing song
                    result = {
                        'track_name': track_name,
                        'artists': artists_string,
                        'search_query': f"{track_name} {artists_string}",
                        'status': 'existing',
                        'error': None,
                        'filename': f"{song_id}.mp3",
                        'video_title': 'Using existing file',
                        'metadata': track,
                        'song_id': song_id
                    }
                    existing_reused += 1
                else:
                    # Attempt download for new songs
                    if batch_download_approved:  # Only download if user agreed for batch
                        result = search_and_download_audio_smart(track, songs_folder, song_manager, controller)
                    else:
                        # Skip download but still process metadata
                        result = {
                            'track_name': track_name,
                            'artists': artists_string,
                            'search_query': f"{track_name} {artists_string}",
                            'status': 'skipped',
                            'error': 'Download skipped by user for batch',
                            'filename': None,
                            'video_title': None,
                            'metadata': track,
                            'song_id': song_id
                        }
                
                download_log.append(result)
                
                # Add song to consolidator regardless of download status
                consolidator.add_song_to_playlist(song_id, track, result)
                
                # Update counters
                if result['status'] == 'success':
                    successful_downloads += 1
                    print(f"   ✅ Downloaded: {result['filename']}")
                    print(f"   🎬 From video: {result['video_title']}")
                elif result['status'] == 'existing':
                    existing_reused += 1
                    print(f"   ✅ Using existing: {result['filename']}")
                elif result['status'] == 'skipped':
                    skipped_downloads += 1
                    print(f"   ⏭️  Skipped: {result['error']}")
                elif result['status'] == 'cancelled':
                    cancelled_downloads += 1
                    print(f"   🛑 Cancelled: {result['error']}")
                    break
                else:
                    failed_downloads += 1
                    print(f"   ❌ Failed: {result['error']}")
                
                # Log result
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"{i}. {track_name} - {artists_string}\n")
                    f.write(f"   Album: {album_name}\n")
                    f.write(f"   Song ID: {song_id}\n")
                    f.write(f"   Duration: {duration_formatted}\n")
                    f.write(f"   Status: {result['status']}\n")
                    f.write(f"   Video: {result.get('video_title', 'N/A')}\n")
                    f.write(f"   Error: {result.get('error', 'None')}\n\n")
                
                if not skip_download and batch_download_approved and result['status'] != 'cancelled':
                    time.sleep(Config.DOWNLOAD_DELAY)
                
            except KeyboardInterrupt:
                print("\n⏹️  Process interrupted by user")
                break
            except Exception as e:
                print(f"   ❌ Unexpected error: {e}")
                failed_downloads += 1
        
        # === PHASE 4: CONSOLIDATION AND METADATA GENERATION ===
        print("\n" + "="*60)
        print("PHASE 4: Consolidation and Metadata Generation")
        print("="*60)
        
        # Set playlist metadata in consolidator
        download_info_summary = {
            'total_tracks': len(tracks),
            'successful_downloads': successful_downloads,
            'existing_reused': existing_reused,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        consolidator.set_playlist_metadata(download_info_summary, playlist_url)
        
        # Save consolidated metadata
        consolidator.save_consolidated_metadata()
        
        # Save final summary for this playlist
        summary_file = os.path.join(base_folder, "playlist_download_summary.json")
        summary_data = {
            'playlist_info': {
                'name': playlist_name,
                'url': playlist_url,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_valid_tracks': len(tracks),
                'successful_downloads': successful_downloads,
                'existing_reused': existing_reused,
                'failed_downloads': failed_downloads,
                'skipped_downloads': skipped_downloads,
                'cancelled_downloads': cancelled_downloads,
                'success_rate': f"{((successful_downloads + existing_reused)/len(tracks)*100):.1f}%" if tracks else "0%",
                'songs_folder': songs_folder,
                'cover_art_folder': cover_art_folder,
                'error_handling_enabled': Config.SKIP_INVALID_TRACKS
            },
            'download_results': download_log
        }
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n📊 Playlist '{playlist_name}' Results:")
        print(f"   ✅ New downloads: {successful_downloads}")
        print(f"   🔄 Existing reused: {existing_reused}")
        print(f"   ❌ Failed: {failed_downloads}")
        print(f"   ⏭️  Skipped: {skipped_downloads}")
        if cancelled_downloads > 0:
            print(f"   🛑 Cancelled: {cancelled_downloads}")
        
        total_success = successful_downloads + existing_reused
        if len(tracks) > 0:
            efficiency = (total_success / len(tracks)) * 100
            print(f"   📈 Efficiency: {efficiency:.1f}%")
        
        return {
            'tracks_count': len(tracks),
            'success_count': total_success,
            'failed_count': failed_downloads,
            'cancelled_count': cancelled_downloads,
            'base_folder': base_folder
        }
        
    except Exception as e:
        error_msg = f"Error processing playlist '{playlist_name}': {str(e)}"
        print(f"❌ {error_msg}")
        return {'error': error_msg, 'tracks_count': 0, 'success_count': 0}

# === MAIN EXECUTION ===
def main():
    print("🎵 Enhanced Multi-Playlist Spotify Downloader with Smart Deduplication")
    print("=" * 90)
    print("⚠️  LEGAL NOTICE: Only download content you have rights to access.")
    print("   Respect copyright laws and platform terms of service.")
    print("=" * 90)
    
    # Check prerequisites
    if not check_prerequisites():
        print("❌ Prerequisites not met. Exiting.")
        return
    
    # Setup batch processing
    print("\n🔧 Choose Processing Mode:")
    print("1. Single Playlist (Original Mode)")
    print("2. Multi-Playlist Batch Processing (New Mode)")
    
    mode_choice = input("Choose mode (1-2): ").strip()
    
    if mode_choice == "1":
        # Original single playlist mode
        run_single_playlist_mode()
    elif mode_choice == "2":
        # New multi-playlist batch mode
        run_batch_processing_mode()
    else:
        print("❌ Invalid choice. Exiting.")
        return

def run_single_playlist_mode():
    """Run the original single playlist processing mode"""
    global all_playlist_items, captured_data, seen_requests, stop_capture, auto_scroll_active
    
    # Reset global variables
    all_playlist_items = []
    captured_data = []
    seen_requests = set()
    stop_capture = False
    auto_scroll_active = False
    
    # Get Spotify playlist URL
    Config.SPOTIFY_URL = input("\nEnter Spotify playlist URL: ").strip()
    if not Config.SPOTIFY_URL:
        print("❌ No URL provided. Exiting.")
        return
    
    # Get playlist name from user or generate from URL
    playlist_name_input = input("Enter playlist name (or press Enter to auto-generate): ").strip()
    if playlist_name_input:
        playlist_name = sanitize_filename(playlist_name_input)
    else:
        # Try to extract playlist name from URL or use timestamp
        import urllib.parse
        try:
            playlist_id = Config.SPOTIFY_URL.split('/')[-1].split('?')[0]
            playlist_name = f"playlist_{playlist_id}"
        except:
            playlist_name = f"playlist_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Initialize smart song manager
    print(f"\n🧠 Initializing smart deduplication system...")
    song_manager = SmartSongManager(Config.CONSOLIDATED_FOLDER)
    
    # Create output folders
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_folder = f"spotify_download_{timestamp}"
    songs_folder = os.path.join(base_folder, "songs")
    cover_art_folder = os.path.join(base_folder, "cover_art")
    os.makedirs(songs_folder, exist_ok=True)
    os.makedirs(cover_art_folder, exist_ok=True)
    
    print(f"📁 Temporary download folder: {base_folder}")
    print(f"🎵 Songs will be saved in: {songs_folder}")
    print(f"🖼️  Cover art will be saved in: {cover_art_folder}")
    print(f"🗃️  Consolidated music folder: {Config.CONSOLIDATED_FOLDER}")
    
    # Initialize download controller (for single playlist, always approved)
    controller = DownloadController()
    
    # Create a fake playlist info for single mode
    playlist_info = {
        'name': playlist_name,
        'url': Config.SPOTIFY_URL,
        'status': 'pending'
    }
    
    # Process the single playlist with download approval
    print("\nSingle playlist mode - proceeding with download capability enabled")
    result = process_single_playlist(playlist_info, song_manager, True, controller)
    
    if result.get('error'):
        print(f"❌ Playlist processing failed: {result['error']}")
    else:
        print(f"\n🎉 Single playlist processing completed!")
        print(f"📊 Final Results:")
        print(f"   📚 Total tracks: {result.get('tracks_count', 0)}")
        print(f"   ✅ Successful: {result.get('success_count', 0)}")
        if result.get('failed_count', 0) > 0:
            print(f"   ❌ Failed: {result.get('failed_count', 0)}")
        if result.get('cancelled_count', 0) > 0:
            print(f"   🛑 Cancelled: {result.get('cancelled_count', 0)}")

def run_batch_processing_mode():
    """Run the new multi-playlist batch processing mode"""
    # Setup batch processing
    batch = setup_batch_processing()
    if not batch or not batch.playlists:
        print("❌ No playlists to process. Exiting.")
        return
    
    # Initialize smart song manager
    print(f"\n🧠 Initializing smart deduplication system...")
    song_manager = SmartSongManager(Config.CONSOLIDATED_FOLDER)
    
    # Ask user about batch download approval ONCE
    total_playlists = len(batch.playlists)
    pending_playlists = len([p for p in batch.playlists if p['status'] == 'pending'])
    
    print(f"\n📋 Batch Summary:")
    print(f"   Total playlists: {total_playlists}")
    print(f"   Pending processing: {pending_playlists}")
    
    if pending_playlists == 0:
        print("✅ All playlists already processed!")
        return
    
    # Single approval for entire batch
    print(f"\n🔄 Ready to process {pending_playlists} playlists")
    print("This will:")
    print("   1. Capture playlist data from Spotify")
    print("   2. Extract track information with smart deduplication")
    print("   3. Optionally download new songs (you'll be asked once)")
    
    # Ask for download approval once for entire batch
    download_approval = input(f"\nDo you want to download new songs for ALL playlists? (y/N): ").strip().lower() == 'y'
    
    if download_approval:
        print("✅ Download approved for entire batch")
    else:
        print("📊 Will process metadata only (no downloads)")
    
    # Initialize download controller
    controller = DownloadController()
    
    # Start download control thread
    control_thread = threading.Thread(target=listen_for_download_commands, args=(controller,))
    control_thread.daemon = True
    control_thread.start()
    
    print(f"\n🚀 Starting batch processing...")
    print("💡 You can use 'pause', 'resume', 'cancel', 'status' commands during processing")
    
    # Process each playlist
    batch_results = {
        'total_playlists': total_playlists,
        'processed': 0,
        'successful': 0,
        'failed': 0,
        'cancelled': 0,
        'total_tracks': 0,
        'total_successful_downloads': 0,
        'start_time': datetime.now().isoformat(),
        'playlist_results': []
    }
    
    for i in range(batch.current_playlist_index, len(batch.playlists)):
        if controller.is_cancelled():
            print(f"\n🛑 Batch processing cancelled by user")
            batch_results['cancelled'] = 1
            break
        
        playlist = batch.playlists[i]
        
        if playlist['status'] != 'pending':
            print(f"\n⏭️  Skipping already processed playlist: {playlist['name']}")
            continue
        
        print(f"\n" + "="*90)
        print(f"🎯 PROCESSING PLAYLIST {i+1}/{len(batch.playlists)}")
        print(f"📝 Name: {playlist['name']}")
        print("="*90)
        
        # Check for pause between playlists
        if not controller.check_pause():
            print(f"🛑 Batch processing cancelled")
            batch_results['cancelled'] = 1
            break
        
        if Config.PAUSE_BETWEEN_PLAYLISTS and i > batch.current_playlist_index:
            print(f"\n⏸️  Pausing between playlists...")
            print(f"Press Enter to continue with next playlist, or type 'skip' to skip this playlist: ", end='')
            user_input = input().strip().lower()
            if user_input == 'skip':
                print(f"⏭️  Skipping playlist: {playlist['name']}")
                batch.mark_playlist_completed(0, 0, "Skipped by user")
                continue
        
        # Process the playlist
        result = process_single_playlist(playlist, song_manager, download_approval, controller)
        
        batch_results['processed'] += 1
        batch_results['playlist_results'].append({
            'name': playlist['name'],
            'url': playlist['url'],
            'result': result
        })
        
        if result.get('error'):
            print(f"❌ Playlist '{playlist['name']}' failed: {result['error']}")
            batch.mark_playlist_completed(0, 0, result['error'])
            batch_results['failed'] += 1
        elif result.get('cancelled_count', 0) > 0 or controller.is_cancelled():
            print(f"🛑 Playlist '{playlist['name']}' cancelled")
            batch.mark_playlist_completed(
                result.get('tracks_count', 0), 
                result.get('success_count', 0), 
                "Cancelled by user"
            )
            batch_results['cancelled'] += 1
            break
        else:
            print(f"✅ Playlist '{playlist['name']}' completed successfully")
            batch.mark_playlist_completed(
                result.get('tracks_count', 0), 
                result.get('success_count', 0)
            )
            batch_results['successful'] += 1
            batch_results['total_tracks'] += result.get('tracks_count', 0)
            batch_results['total_successful_downloads'] += result.get('success_count', 0)
    
    # Final batch summary
    batch_results['end_time'] = datetime.now().isoformat()
    
    print(f"\n" + "="*90)
    print("🏁 BATCH PROCESSING COMPLETE")
    print("="*90)
    
    print(f"📊 FINAL BATCH RESULTS:")
    print(f"   🎯 Total playlists: {batch_results['total_playlists']}")
    print(f"   ✅ Successfully processed: {batch_results['successful']}")
    print(f"   ❌ Failed: {batch_results['failed']}")
    print(f"   🛑 Cancelled: {batch_results['cancelled']}")
    print(f"   📚 Total tracks processed: {batch_results['total_tracks']}")
    print(f"   🎵 Total successful downloads/reuses: {batch_results['total_successful_downloads']}")
    
    if batch_results['total_tracks'] > 0:
        overall_efficiency = (batch_results['total_successful_downloads'] / batch_results['total_tracks']) * 100
        print(f"   📈 Overall efficiency: {overall_efficiency:.1f}%")
    
    # Save batch results
    batch_results_file = f"batch_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(batch_results_file, 'w', encoding='utf-8') as f:
        json.dump(batch_results, f, indent=2, ensure_ascii=False)
    
    print(f"\n📄 Batch results saved to: {batch_results_file}")
    print(f"📋 Batch configuration saved to: {batch.batch_file}")
    print(f"🗃️  Consolidated music library: {song_manager.consolidated_folder}")
    
    if batch_results['successful'] > 0:
        print(f"\n🎉 Successfully processed {batch_results['successful']} playlists!")
        print(f"🎵 Your consolidated music library has been updated with new songs!")

if __name__ == "__main__":
    main()
