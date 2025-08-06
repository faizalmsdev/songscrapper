import json
import threading
import time
import os
import re
import subprocess
import sys
import requests
from datetime import datetime
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
    
    # YouTube bot prevention settings - UPDATED
    USE_COOKIES_FROM_BROWSER = False  # CHANGED: Disable browser cookies
    BROWSER_FOR_COOKIES = "chrome"  # Options: chrome, firefox, edge, safari
    USE_PROXY = False
    PROXY_URL = ""
    RANDOM_USER_AGENT = True
    EXTRA_DELAY_ON_ERROR = 10
    ALLOW_YOUTUBE_CAPTCHA = True
# === GLOBAL VARIABLES ===
captured_data = []
all_playlist_items = []
seen_requests = set()
stop_capture = False
auto_scroll_active = False

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
    
    # Check if essential fields are present and valid
    if not track_name or len(track_name) < Config.MIN_TRACK_NAME_LENGTH:
        return False, "Track name is empty or too short"
    
    if not artists_string or len(artists_string) < Config.MIN_ARTIST_NAME_LENGTH:
        return False, "Artist name is empty or too short"
    
    if track_name.lower() in ['unknown track', 'unknown', '']:
        return False, "Track name is placeholder value"
    
    if artists_string.lower() in ['unknown artist', 'unknown', '']:
        return False, "Artist name is placeholder value"
    
    return True, "Valid"

def get_random_user_agent():
    """Return a random user agent to avoid detection"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0"
    ]
    import random
    return random.choice(user_agents)

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
        
        # Ensure we have a valid filename
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

def get_enhanced_ydl_opts(output_path):
    """Get enhanced yt-dlp options with proper cookie handling"""
    opts = {
        'format': 'bestaudio/best',
        'extractaudio': True,
        'audioformat': 'mp3',
        'audioquality': Config.AUDIO_QUALITY,
        'outtmpl': output_path,
        'noplaylist': True,
        'quiet': False,
        'no_warnings': False,
        'default_search': 'ytsearch1:',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        # Enhanced bot prevention options
        'extractor_retries': 5,
        'fragment_retries': 5,
        'retries': 5,
        'sleep_interval': 3,
        'max_sleep_interval': 15,
        'sleep_interval_requests': 3,
        'sleep_interval_subtitles': 3,
        # Additional anti-bot measures
        'http_chunk_size': 10485760,  # 10MB chunks
        'ratelimit': 1000000,  # 1MB/s rate limit to appear more human-like
    }
    
    # Add random user agent
    if Config.RANDOM_USER_AGENT:
        opts['http_headers'] = {
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-us,en;q=0.5',
            'Accept-Encoding': 'gzip,deflate',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
            'Keep-Alive': '300',
            'Connection': 'keep-alive',
        }
    
    # FIXED: Prioritize cookies.txt file over browser cookies
    cookies_file = "cookies.txt"
    if os.path.exists(cookies_file):
        opts['cookiefile'] = cookies_file
        print(f"   🍪 Using cookies.txt file")
    elif Config.USE_COOKIES_FROM_BROWSER:
        try:
            opts['cookiesfrombrowser'] = (Config.BROWSER_FOR_COOKIES,)
            print(f"   🍪 Using cookies from {Config.BROWSER_FOR_COOKIES} browser")
        except Exception as e:
            print(f"   ⚠️  Could not load browser cookies: {e}")
    else:
        print(f"   ⚠️  No cookies available - may encounter bot detection")
    
    # Add proxy if configured
    if Config.USE_PROXY and Config.PROXY_URL:
        opts['proxy'] = Config.PROXY_URL
        print(f"   🌐 Using proxy: {Config.PROXY_URL}")
    
    return opts
def handle_youtube_captcha():
    """Handle YouTube CAPTCHA by opening browser"""
    if Config.ALLOW_YOUTUBE_CAPTCHA:
        print("\n🤖 YouTube may require CAPTCHA verification.")
        print("   Opening YouTube in browser for manual verification...")
        
        try:
            import webbrowser
            webbrowser.open("https://www.youtube.com")
            print("   ✅ YouTube opened in browser")
            print("   👆 Please solve any CAPTCHA if prompted, then press Enter to continue")
            input("   Press Enter when ready...")
            return True
        except Exception as e:
            print(f"   ⚠️  Could not open browser: {e}")
            return False
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

# === ENHANCED TRACK EXTRACTION FUNCTIONS ===
def extract_enhanced_track_info(items, cover_art_folder):
    """Extract comprehensive track information with robust error handling"""
    tracks_info = []
    skipped_count = 0
    error_count = 0
    
    print(f"🎵 Processing {len(items)} items with enhanced metadata and error handling...")
    
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
            
            # Download cover art if available
            if cover_url and Config.DOWNLOAD_COVER_ART:
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
            }
            
            tracks_info.append(track_info)
            
            # Show progress every 50 items or for problematic items
            if i % 50 == 0 or not is_valid:
                print(f"✅ Processed {i}/{len(items)} items... (Valid tracks: {len(tracks_info)})")
                
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
    if skipped_count > 0:
        print(f"⏭️  Skipped {skipped_count} invalid/problematic items")
    if error_count > 0:
        print(f"⚠️  {error_count} items had processing errors")
    
    if skipped_count > 0 or error_count > 0:
        print(f"📋 Detailed skip log saved to: {skipped_log_file}")
    
    return tracks_info

# === ENHANCED DOWNLOAD FUNCTIONS ===
def search_and_download_audio(track_info, output_folder):
    """Search for and download audio with enhanced bot prevention"""
    import yt_dlp
    
    try:
        track_name = track_info.get('track_name', 'Unknown')
        artists_str = track_info.get('artists_string', 'Unknown')
        
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
                'metadata': track_info
            }
        
        search_query = f"{track_name} {artists_str}".strip()
        safe_filename = sanitize_filename(f"{track_name} - {artists_str}")
        
        if not safe_filename or safe_filename == "unknown_file":
            return {
                'track_name': track_name,
                'artists': artists_str,
                'search_query': search_query,
                'status': 'failed',
                'error': 'Could not create valid filename',
                'filename': None,
                'video_title': None,
                'metadata': track_info
            }
        
        output_path = os.path.join(output_folder, f"{safe_filename}.%(ext)s")
        
        result = {
            'track_name': track_name,
            'artists': artists_str,
            'search_query': search_query,
            'status': 'failed',
            'error': None,
            'filename': None,
            'video_title': None,
            'metadata': track_info
        }
        
        for attempt in range(Config.MAX_RETRIES):
            try:
                # Get fresh yt-dlp options for each attempt
                ydl_opts = get_enhanced_ydl_opts(output_path)
                
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
                    
                    ydl.download([video_info['webpage_url']])
                    
                    expected_filename = f"{safe_filename}.mp3"
                    full_path = os.path.join(output_folder, expected_filename)
                    
                    if os.path.exists(full_path):
                        result['status'] = 'success'
                        result['filename'] = expected_filename
                        return result
                    else:
                        for file in os.listdir(output_folder):
                            if file.startswith(safe_filename) and file.endswith('.mp3'):
                                result['status'] = 'success'
                                result['filename'] = file
                                return result
                    
            except Exception as e:
                error_msg = str(e)
                result['error'] = error_msg
                
                # Check if it's a bot detection error
                if "Sign in to confirm you're not a bot" in error_msg:
                    print(f"   🤖 Bot detection triggered on attempt {attempt + 1}")
                    if attempt < Config.MAX_RETRIES - 1:
                        print(f"   ⏸️  Waiting {Config.EXTRA_DELAY_ON_ERROR} seconds before retry...")
                        time.sleep(Config.EXTRA_DELAY_ON_ERROR)
                        
                        # Try to handle CAPTCHA
                        if Config.ALLOW_YOUTUBE_CAPTCHA and attempt == 0:
                            handle_youtube_captcha()
                    continue
                
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
            'metadata': track_info
        }

def create_cookies_txt_guide():
    """Display guide for creating cookies.txt file"""
    print("\n" + "="*70)
    print("COOKIES.TXT SETUP GUIDE")
    print("="*70)
    print("To fix YouTube bot detection, you need to provide cookies.")
    print("Here are your options:")
    print()
    print("OPTION 1 - Automatic (Recommended):")
    print("1. Make sure Chrome/Firefox is installed")
    print("2. Visit YouTube.com in your browser and sign in")
    print("3. The script will automatically use your browser cookies")
    print()
    print("OPTION 2 - Manual cookies.txt:")
    print("1. Install 'Get cookies.txt LOCALLY' Chrome extension")
    print("2. Visit YouTube.com and sign in")
    print("3. Click the extension and export cookies for youtube.com")
    print("4. Save the file as 'cookies.txt' in the same folder as this script")
    print()
    print("OPTION 3 - Using yt-dlp command:")
    print("Run this command first to create cookies.txt:")
    print("yt-dlp --cookies-from-browser chrome --cookies cookies.txt --skip-download 'https://www.youtube.com/watch?v=dQw4w9WgXcQ'")
    print()
    print("After setting up cookies, run the script again.")
    print("="*70)
# === MAIN EXECUTION ===
def main():
    print("🎵 Enhanced Spotify Playlist Downloader with Robust Error Handling")
    print("=" * 70)
    print("⚠️  LEGAL NOTICE: Only download content you have rights to access.")
    print("   Respect copyright laws and platform terms of service.")
    print("=" * 70)
    
    # Check for cookies setup (ADD THIS)
    if not os.path.exists("cookies.txt"):
        print("🍪 No cookies.txt found. For best results against YouTube bot detection:")
        print("   The script will try to use your browser cookies automatically.")
        print("   If you encounter bot detection errors, you may need to set up cookies.txt")
        
        setup_cookies = input("\nWould you like to see the cookies setup guide? (y/N): ").strip().lower()
        if setup_cookies == 'y':
            create_cookies_txt_guide()
            return
    else:
        print("✅ cookies.txt found - using for YouTube authentication")
    
    # Check prerequisites
    if not check_prerequisites():
        print("❌ Prerequisites not met. Exiting.")
        return
    
    # Get Spotify playlist URL
    Config.SPOTIFY_URL = input("\nEnter Spotify playlist URL: ").strip()
    if not Config.SPOTIFY_URL:
        print("❌ No URL provided. Exiting.")
        return
    
    # Create output folders
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_folder = f"spotify_download_{timestamp}"
    songs_folder = os.path.join(base_folder, "songs")
    cover_art_folder = os.path.join(base_folder, "cover_art")
    os.makedirs(songs_folder, exist_ok=True)
    os.makedirs(cover_art_folder, exist_ok=True)
    
    print(f"📁 Output folder: {base_folder}")
    print(f"🎵 Songs will be saved in: {songs_folder}")
    print(f"🖼️  Cover art will be saved in: {cover_art_folder}")
    
    # === PHASE 1: CAPTURE PLAYLIST DATA ===
    print("\n" + "="*70)
    print("PHASE 1: Capturing Spotify Playlist Data")
    print("="*70)
    
    # Setup browser - Using the same settings as the working version
    print("🔄 Launching browser...")
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(options=options)
    driver.requests.clear()
    driver.get(Config.SPOTIFY_URL)
    
    print(f"🌐 Opened playlist: {Config.SPOTIFY_URL}")
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
    
    # Start command listener
    command_thread = threading.Thread(target=listen_for_commands)
    command_thread.daemon = True
    command_thread.start()
    
    # Wait for capture to complete
    while not stop_capture:
        time.sleep(1)
    
    # Wait a bit for threads to finish
    time.sleep(2)
    driver.quit()
    
    if not all_playlist_items:
        print("❌ No playlist items captured. Exiting.")
        return
    
    print(f"✅ Captured {len(all_playlist_items)} playlist items")
    
    # === PHASE 2: EXTRACT ENHANCED TRACK INFORMATION ===
    print("\n" + "="*70)
    print("PHASE 2: Extracting Enhanced Track Information & Metadata")
    print("="*70)
    
    tracks = extract_enhanced_track_info(all_playlist_items, cover_art_folder)
    
    if not tracks:
        print("❌ No valid tracks extracted. Exiting.")
        return
    
    # Save enhanced track information
    tracks_file = os.path.join(base_folder, "enhanced_tracks_metadata.json")
    tracks_data = {
        'extraction_info': {
            'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_tracks': len(tracks),
            'source_url': Config.SPOTIFY_URL,
            'cover_art_downloaded': Config.DOWNLOAD_COVER_ART,
            'cover_art_folder': cover_art_folder,
            'error_handling_enabled': Config.SKIP_INVALID_TRACKS
        },
        'tracks': tracks
    }
    
    with open(tracks_file, 'w', encoding='utf-8') as f:
        json.dump(tracks_data, f, indent=2, ensure_ascii=False)
    
    print(f"📄 Enhanced track metadata saved to: {tracks_file}")
    
    # === PHASE 3: DOWNLOAD AUDIO ===
    print("\n" + "="*70)
    print("PHASE 3: Downloading Audio Files")
    print("="*70)
    
    print(f"🎵 Found {len(tracks)} valid tracks to download")
    response = input("Do you want to proceed with downloading? (y/N): ").strip().lower()
    
    if response != 'y':
        print("❌ Download cancelled")
        print(f"📄 Enhanced metadata saved in: {tracks_file}")
        return
    
    # Download tracks
    successful_downloads = 0
    failed_downloads = 0
    skipped_downloads = 0
    download_log = []
    
    log_file = os.path.join(base_folder, "download_log.txt")
    
    for i, track in enumerate(tracks, 1):
        try:
            # Display track info with safe handling of empty fields
            track_name = track.get('track_name', 'Unknown Track')
            artists_string = track.get('artists_string', 'Unknown Artist')
            album_name = track.get('album_name', 'Unknown Album')
            duration_formatted = track.get('duration_formatted', '0:00')
            added_at_formatted = track.get('added_at_formatted', '')
            added_by_name = track.get('added_by_name', 'Unknown')
            
            print(f"\n🎵 [{i}/{len(tracks)}] {track_name} - {artists_string}")
            print(f"   📀 Album: {album_name}")
            
            if duration_formatted and duration_formatted != '0:00':
                print(f"   ⏱️  Duration: {duration_formatted}")
            else:
                print(f"   ⏱️  Duration: Unknown")
            
            if added_at_formatted:
                print(f"   📅 Added: {added_at_formatted} by {added_by_name}")
            else:
                print(f"   📅 Added: Unknown date by {added_by_name}")
            
            # Attempt download
            result = search_and_download_audio(track, songs_folder)
            download_log.append(result)
            
            if result['status'] == 'success':
                successful_downloads += 1
                print(f"   ✅ Downloaded: {result['filename']}")
                print(f"   🎬 From video: {result['video_title']}")
            elif result['status'] == 'skipped':
                skipped_downloads += 1
                print(f"   ⏭️  Skipped: {result['error']}")
            else:
                failed_downloads += 1
                print(f"   ❌ Failed: {result['error']}")
            
            # Log result with safe handling
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"{i}. {track_name} - {artists_string}\n")
                f.write(f"   Album: {album_name}\n")
                f.write(f"   Duration: {duration_formatted}\n")
                f.write(f"   Added: {added_at_formatted} by {added_by_name}\n")
                f.write(f"   Status: {result['status']}\n")
                f.write(f"   Video: {result.get('video_title', 'N/A')}\n")
                f.write(f"   Error: {result.get('error', 'None')}\n\n")
            
            time.sleep(Config.DOWNLOAD_DELAY)
            
        except KeyboardInterrupt:
            print("\n⏹️  Download interrupted by user")
            break
        except Exception as e:
            print(f"   ❌ Unexpected error during download: {e}")
            failed_downloads += 1
            
            # Log the unexpected error
            try:
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"{i}. ERROR PROCESSING TRACK\n")
                    f.write(f"   Error: Unexpected error - {str(e)}\n\n")
            except:
                pass
    
    # === FINAL SUMMARY ===
    print("\n" + "="*70)
    print("DOWNLOAD COMPLETE - ENHANCED SUMMARY WITH ERROR HANDLING")
    print("="*70)
    
    total_processed = successful_downloads + failed_downloads + skipped_downloads
    
    print(f"📊 RESULTS:")
    print(f"   Total valid tracks: {len(tracks)}")
    print(f"   ✅ Successful downloads: {successful_downloads}")
    print(f"   ❌ Failed downloads: {failed_downloads}")
    print(f"   ⏭️  Skipped downloads: {skipped_downloads}")
    if len(tracks) > 0:
        print(f"   📈 Success rate: {(successful_downloads/len(tracks)*100):.1f}%")
    
    cover_art_count = 0
    try:
        cover_art_count = len([f for f in os.listdir(cover_art_folder) if f.endswith('.jpg')])
    except:
        pass
    
    print(f"\n📁 FILES CREATED:")
    print(f"   🎵 Songs folder: {songs_folder}")
    print(f"   🖼️  Cover art folder: {cover_art_folder} ({cover_art_count} images)")
    print(f"   📄 Enhanced metadata: {tracks_file}")
    print(f"   📋 Download log: {log_file}")
    
    # Check for skipped tracks log
    skipped_log_file = os.path.join(base_folder, "skipped_tracks.log")
    if os.path.exists(skipped_log_file):
        print(f"   ⏭️  Skipped tracks log: {skipped_log_file}")
    
    # Save final summary
    summary_file = os.path.join(base_folder, "enhanced_download_summary.json")
    summary_data = {
        'download_info': {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'source_url': Config.SPOTIFY_URL,
            'total_valid_tracks': len(tracks),
            'successful_downloads': successful_downloads,
            'failed_downloads': failed_downloads,
            'skipped_downloads': skipped_downloads,
            'success_rate': f"{(successful_downloads/len(tracks)*100):.1f}%" if tracks else "0%",
            'songs_folder': songs_folder,
            'cover_art_folder': cover_art_folder,
            'cover_art_downloaded': cover_art_count,
            'error_handling_enabled': Config.SKIP_INVALID_TRACKS
        },
        'download_results': download_log
    }
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    
    print(f"   📊 Enhanced summary: {summary_file}")
    
    if successful_downloads > 0:
        print(f"\n🎉 Successfully downloaded {successful_downloads} songs with metadata!")
        print(f"🎵 Your music is ready in: {songs_folder}")
        print(f"🖼️  Cover art available in: {cover_art_folder}")
        
        if skipped_downloads > 0:
            print(f"⏭️  {skipped_downloads} tracks were skipped due to invalid data")
            print(f"📋 Check skipped tracks log for details: {skipped_log_file}")
    else:
        print(f"\n😔 No songs were successfully downloaded.")
        print(f"📋 Check the log files for details:")
        print(f"   Download log: {log_file}")
        if os.path.exists(skipped_log_file):
            print(f"   Skipped tracks: {skipped_log_file}")

if __name__ == "__main__":
    main()
