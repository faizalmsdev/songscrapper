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

# === GLOBAL VARIABLES ===
captured_data = []
all_playlist_items = []
seen_requests = set()
stop_capture = False
auto_scroll_active = False

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
    """Remove invalid characters from filename"""
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    filename = re.sub(r'[^\w\s-]', '', filename)
    filename = re.sub(r'[-\s]+', '-', filename)
    return filename.strip('-')[:100]

def download_cover_art(cover_url, output_path):
    """Download cover art image"""
    try:
        response = requests.get(cover_url, timeout=10)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        return True
    except Exception as e:
        print(f"   ⚠️  Failed to download cover art: {e}")
        return False

def get_best_cover_art_url(cover_sources, preferred_size=640):
    """Get the best cover art URL from sources"""
    if not cover_sources:
        return None
    
    # Try to find preferred size
    for source in cover_sources:
        if source.get('width') == preferred_size:
            return source.get('url')
    
    # If preferred size not found, get the largest available
    largest = max(cover_sources, key=lambda x: x.get('width', 0))
    return largest.get('url')

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
                        items_in_response = extract_items_from_response(parsed_response)
                        
                        print(f"🎯 Captured Playlist Items Request #{playlist_items_count}")
                        print(f"   🎵 Items extracted: {len(items_in_response)}")
                        
                        if items_in_response:
                            all_playlist_items.extend(items_in_response)
                            print(f"   📚 Total items collected: {len(all_playlist_items)}")
                        
                except Exception as e:
                    print(f"[!] Error processing request: {e}")
        
        time.sleep(0.5)

# === ENHANCED TRACK EXTRACTION FUNCTIONS ===
def extract_enhanced_track_info(items, cover_art_folder):
    """Extract comprehensive track information including metadata"""
    tracks_info = []
    skipped_count = 0
    
    print(f"🎵 Processing {len(items)} items with enhanced metadata...")
    
    for i, item in enumerate(items, 1):
        try:
            item_v2 = item.get('itemV2', {})
            
            if item_v2.get('__typename') != 'TrackResponseWrapper':
                skipped_count += 1
                continue
                
            track_data = item_v2.get('data', {})
            
            # Basic track info
            track_name = track_data.get('name', 'Unknown Track')
            track_uri = track_data.get('uri', '')
            
            # Artists info
            artists_data = track_data.get('artists', {}).get('items', [])
            artist_names = []
            artist_uris = []
            
            for artist in artists_data:
                artist_name = artist.get('profile', {}).get('name', 'Unknown Artist')
                if artist_name not in artist_names:
                    artist_names.append(artist_name)
                    artist_uris.append(artist.get('uri', ''))
            
            # Album info
            album_data = track_data.get('albumOfTrack', {})
            album_name = album_data.get('name', 'Unknown Album')
            album_uri = album_data.get('uri', '')
            
            # Cover art info
            cover_sources = album_data.get('coverArt', {}).get('sources', [])
            cover_url = get_best_cover_art_url(cover_sources, Config.COVER_ART_SIZE)
            cover_filename = None
            
            # Download cover art if available
            if cover_url and Config.DOWNLOAD_COVER_ART:
                safe_track_name = sanitize_filename(f"{track_name}_{artist_names[0] if artist_names else 'unknown'}")
                cover_filename = f"{safe_track_name}_cover.jpg"
                cover_path = os.path.join(cover_art_folder, cover_filename)
                
                if download_cover_art(cover_url, cover_path):
                    print(f"   🖼️  Downloaded cover art: {cover_filename}")
                else:
                    cover_filename = None
            
            # Track duration
            duration_ms = track_data.get('trackDuration', {}).get('totalMilliseconds', 0)
            duration_seconds = duration_ms / 1000 if duration_ms else 0
            
            # Additional metadata
            track_number = track_data.get('trackNumber', 0)
            disc_number = track_data.get('discNumber', 1)
            playcount = track_data.get('playcount', '0')
            content_rating = track_data.get('contentRating', {}).get('label', 'NONE')
            
            # Added info (from item level)
            added_at = item.get('addedAt', {}).get('isoString', '')
            added_by_data = item.get('addedBy', {}).get('data', {})
            added_by_name = added_by_data.get('name', 'Unknown')
            added_by_username = added_by_data.get('username', '')
            
            # Added by avatar
            added_by_avatar_sources = added_by_data.get('avatar', {}).get('sources', [])
            added_by_avatar_url = get_best_cover_art_url(added_by_avatar_sources, 300)
            
            track_info = {
                # Basic info
                'track_name': track_name,
                'track_uri': track_uri,
                'artists': artist_names,
                'artist_uris': artist_uris,
                'artists_string': ', '.join(artist_names) if artist_names else 'Unknown Artist',
                
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
                'added_at_formatted': datetime.fromisoformat(added_at.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S') if added_at else '',
                'added_by_name': added_by_name,
                'added_by_username': added_by_username,
                'added_by_avatar_url': added_by_avatar_url,
                
                # Processing info
                'processed_at': datetime.now().isoformat(),
            }
            
            tracks_info.append(track_info)
            
            if i % 50 == 0:
                print(f"✅ Processed {i}/{len(items)} items...")
                
        except Exception as e:
            print(f"⚠️  Error processing item {i}: {e}")
            skipped_count += 1
            continue
    
    print(f"✅ Successfully extracted {len(tracks_info)} tracks with metadata")
    if skipped_count > 0:
        print(f"⏭️  Skipped {skipped_count} non-track items")
    
    return tracks_info

# === DOWNLOAD FUNCTIONS ===
def search_and_download_audio(track_info, output_folder):
    """Search for and download audio from YouTube with enhanced metadata"""
    import yt_dlp
    
    track_name = track_info['track_name']
    artists_str = track_info['artists_string']
    search_query = f"{track_name} {artists_str}"
    
    safe_filename = sanitize_filename(f"{track_name} - {artists_str}")
    output_path = os.path.join(output_folder, f"{safe_filename}.%(ext)s")
    
    ydl_opts = {
        'format': 'bestaudio/best',
        'extractaudio': True,
        'audioformat': 'mp3',
        'audioquality': Config.AUDIO_QUALITY,
        'outtmpl': output_path,
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
        'metadata': track_info
    }
    
    for attempt in range(Config.MAX_RETRIES):
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
            result['error'] = str(e)
            if attempt < Config.MAX_RETRIES - 1:
                print(f"   ⚠️  Attempt {attempt + 1} failed: {e}, retrying...")
                time.sleep(2)
            continue
    
    return result

# === MAIN EXECUTION ===
def main():
    print("🎵 Enhanced Spotify Playlist Downloader with Metadata")
    print("=" * 60)
    print("⚠️  LEGAL NOTICE: Only download content you have rights to access.")
    print("   Respect copyright laws and platform terms of service.")
    print("=" * 60)
    
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
    print("\n" + "="*60)
    print("PHASE 1: Capturing Spotify Playlist Data")
    print("="*60)
    
    # Setup browser
    print("🔄 Launching browser...")
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=options)
    driver.requests.clear()
    driver.get(Config.SPOTIFY_URL)
    
    print(f"🌐 Opened playlist: {Config.SPOTIFY_URL}")
    print("🎯 Starting capture process...")
    
    # Start capture and scroll threads
    capture_thread = threading.Thread(target=capture_requests, args=(driver,))
    capture_thread.daemon = True
    capture_thread.start()
    
    if Config.AUTO_SCROLL_ENABLED:
        scroll_thread = threading.Thread(target=auto_scroll, args=(driver,))
        scroll_thread.daemon = True
        scroll_thread.start()
    
    # Wait for user to stop or auto-stop after reasonable time
    print("\nCapturing playlist data... Press Enter to stop and proceed to processing")
    input()
    stop_capture = True
    
    # Wait a bit for threads to finish
    time.sleep(2)
    driver.quit()
    
    if not all_playlist_items:
        print("❌ No playlist items captured. Exiting.")
        return
    
    print(f"✅ Captured {len(all_playlist_items)} playlist items")
    
    # === PHASE 2: EXTRACT ENHANCED TRACK INFORMATION ===
    print("\n" + "="*60)
    print("PHASE 2: Extracting Enhanced Track Information & Metadata")
    print("="*60)
    
    tracks = extract_enhanced_track_info(all_playlist_items, cover_art_folder)
    
    if not tracks:
        print("❌ No tracks extracted. Exiting.")
        return
    
    # Save enhanced track information
    tracks_file = os.path.join(base_folder, "enhanced_tracks_metadata.json")
    tracks_data = {
        'extraction_info': {
            'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_tracks': len(tracks),
            'source_url': Config.SPOTIFY_URL,
            'cover_art_downloaded': Config.DOWNLOAD_COVER_ART,
            'cover_art_folder': cover_art_folder
        },
        'tracks': tracks
    }
    
    with open(tracks_file, 'w', encoding='utf-8') as f:
        json.dump(tracks_data, f, indent=2, ensure_ascii=False)
    
    print(f"📄 Enhanced track metadata saved to: {tracks_file}")
    
    # === PHASE 3: DOWNLOAD AUDIO ===
    print("\n" + "="*60)
    print("PHASE 3: Downloading Audio Files")
    print("="*60)
    
    print(f"🎵 Found {len(tracks)} tracks to download")
    response = input("Do you want to proceed with downloading? (y/N): ").strip().lower()
    
    if response != 'y':
        print("❌ Download cancelled")
        print(f"📄 Enhanced metadata saved in: {tracks_file}")
        return
    
    # Download tracks
    successful_downloads = 0
    failed_downloads = 0
    download_log = []
    
    log_file = os.path.join(base_folder, "download_log.txt")
    
    for i, track in enumerate(tracks, 1):
        print(f"\n🎵 [{i}/{len(tracks)}] {track['track_name']} - {track['artists_string']}")
        print(f"   📀 Album: {track['album_name']}")
        if track['duration_formatted']:
            print(f"   ⏱️  Duration: {track['duration_formatted']}")
        if track['added_at_formatted']:
            print(f"   📅 Added: {track['added_at_formatted']} by {track['added_by_name']}")
        
        try:
            result = search_and_download_audio(track, songs_folder)
            download_log.append(result)
            
            if result['status'] == 'success':
                successful_downloads += 1
                print(f"   ✅ Downloaded: {result['filename']}")
                print(f"   🎬 From video: {result['video_title']}")
            else:
                failed_downloads += 1
                print(f"   ❌ Failed: {result['error']}")
            
            # Log result
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"{i}. {track['track_name']} - {track['artists_string']}\n")
                f.write(f"   Album: {track['album_name']}\n")
                f.write(f"   Duration: {track['duration_formatted']}\n")
                f.write(f"   Added: {track['added_at_formatted']} by {track['added_by_name']}\n")
                f.write(f"   Status: {result['status']}\n")
                f.write(f"   Video: {result.get('video_title', 'N/A')}\n")
                f.write(f"   Error: {result.get('error', 'None')}\n\n")
            
            time.sleep(Config.DOWNLOAD_DELAY)
            
        except KeyboardInterrupt:
            print("\n⏹️  Download interrupted by user")
            break
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")
            failed_downloads += 1
    
    # === FINAL SUMMARY ===
    print("\n" + "="*60)
    print("DOWNLOAD COMPLETE - ENHANCED SUMMARY")
    print("="*60)
    
    print(f"📊 RESULTS:")
    print(f"   Total tracks: {len(tracks)}")
    print(f"   ✅ Successful downloads: {successful_downloads}")
    print(f"   ❌ Failed downloads: {failed_downloads}")
    print(f"   📈 Success rate: {(successful_downloads/len(tracks)*100):.1f}%")
    
    cover_art_count = len([f for f in os.listdir(cover_art_folder) if f.endswith('.jpg')])
    
    print(f"\n📁 FILES CREATED:")
    print(f"   🎵 Songs folder: {songs_folder}")
    print(f"   🖼️  Cover art folder: {cover_art_folder} ({cover_art_count} images)")
    print(f"   📄 Enhanced metadata: {tracks_file}")
    print(f"   📋 Download log: {log_file}")
    
    # Save final summary
    summary_file = os.path.join(base_folder, "enhanced_download_summary.json")
    summary_data = {
        'download_info': {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'source_url': Config.SPOTIFY_URL,
            'total_tracks': len(tracks),
            'successful_downloads': successful_downloads,
            'failed_downloads': failed_downloads,
            'success_rate': f"{(successful_downloads/len(tracks)*100):.1f}%" if tracks else "0%",
            'songs_folder': songs_folder,
            'cover_art_folder': cover_art_folder,
            'cover_art_downloaded': cover_art_count
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
    else:
        print(f"\n😔 No songs were successfully downloaded.")
        print(f"📋 Check the log file for details: {log_file}")

if __name__ == "__main__":
    main()
