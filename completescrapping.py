import json
import threading
import time
import os
import re
import subprocess
import sys
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

# === TRACK EXTRACTION FUNCTIONS ===
def extract_track_info(items):
    """Extract track names and artist names from playlist items"""
    tracks_info = []
    skipped_count = 0
    
    print(f"🎵 Processing {len(items)} items...")
    
    for i, item in enumerate(items, 1):
        try:
            item_v2 = item.get('itemV2', {})
            
            if item_v2.get('__typename') != 'TrackResponseWrapper':
                skipped_count += 1
                continue
                
            track_data = item_v2.get('data', {})
            track_name = track_data.get('name', 'Unknown Track')
            
            artists_data = track_data.get('artists', {}).get('items', [])
            artist_names = []
            
            for artist in artists_data:
                artist_name = artist.get('profile', {}).get('name', 'Unknown Artist')
                if artist_name not in artist_names:
                    artist_names.append(artist_name)
            
            track_info = {
                'track_name': track_name,
                'artists': artist_names,
                'artists_string': ', '.join(artist_names) if artist_names else 'Unknown Artist'
            }
            
            tracks_info.append(track_info)
            
            if i % 100 == 0:
                print(f"✅ Processed {i}/{len(items)} items...")
                
        except Exception as e:
            print(f"⚠️  Error processing item {i}: {e}")
            skipped_count += 1
            continue
    
    print(f"✅ Successfully extracted {len(tracks_info)} tracks")
    if skipped_count > 0:
        print(f"⏭️  Skipped {skipped_count} non-track items")
    
    return tracks_info

# === DOWNLOAD FUNCTIONS ===
def search_and_download_audio(track_name, artists, output_folder):
    """Search for and download audio from YouTube"""
    import yt_dlp
    
    artists_str = ' '.join(artists) if isinstance(artists, list) else artists
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
        'video_title': None
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
    print("🎵 Complete Spotify Playlist Downloader")
    print("=" * 50)
    print("⚠️  LEGAL NOTICE: Only download content you have rights to access.")
    print("   Respect copyright laws and platform terms of service.")
    print("=" * 50)
    
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
    os.makedirs(songs_folder, exist_ok=True)
    
    print(f"📁 Output folder: {base_folder}")
    print(f"🎵 Songs will be saved in: {songs_folder}")
    
    # === PHASE 1: CAPTURE PLAYLIST DATA ===
    print("\n" + "="*50)
    print("PHASE 1: Capturing Spotify Playlist Data")
    print("="*50)
    
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
    print("\nCapturing playlist data... Press Enter to stop and proceed to download")
    input()
    stop_capture = True
    
    # Wait a bit for threads to finish
    time.sleep(2)
    driver.quit()
    
    if not all_playlist_items:
        print("❌ No playlist items captured. Exiting.")
        return
    
    print(f"✅ Captured {len(all_playlist_items)} playlist items")
    
    # === PHASE 2: EXTRACT TRACK INFORMATION ===
    print("\n" + "="*50)
    print("PHASE 2: Extracting Track Information")
    print("="*50)
    
    tracks = extract_track_info(all_playlist_items)
    
    if not tracks:
        print("❌ No tracks extracted. Exiting.")
        return
    
    # Save track information
    tracks_file = os.path.join(base_folder, "extracted_tracks.json")
    tracks_data = {
        'extraction_info': {
            'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_tracks': len(tracks),
            'source_url': Config.SPOTIFY_URL
        },
        'tracks': tracks
    }
    
    with open(tracks_file, 'w', encoding='utf-8') as f:
        json.dump(tracks_data, f, indent=2, ensure_ascii=False)
    
    print(f"📄 Track information saved to: {tracks_file}")
    
    # === PHASE 3: DOWNLOAD AUDIO ===
    print("\n" + "="*50)
    print("PHASE 3: Downloading Audio Files")
    print("="*50)
    
    print(f"🎵 Found {len(tracks)} tracks to download")
    response = input("Do you want to proceed with downloading? (y/N): ").strip().lower()
    
    if response != 'y':
        print("❌ Download cancelled")
        print(f"📄 Track list saved in: {tracks_file}")
        return
    
    # Download tracks
    successful_downloads = 0
    failed_downloads = 0
    download_log = []
    
    log_file = os.path.join(base_folder, "download_log.txt")
    
    for i, track in enumerate(tracks, 1):
        print(f"\n🎵 [{i}/{len(tracks)}] {track['track_name']} - {track['artists_string']}")
        
        try:
            result = search_and_download_audio(
                track['track_name'],
                track['artists'],
                songs_folder
            )
            
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
    print("\n" + "="*50)
    print("DOWNLOAD COMPLETE - SUMMARY")
    print("="*50)
    
    print(f"📊 RESULTS:")
    print(f"   Total tracks: {len(tracks)}")
    print(f"   ✅ Successful downloads: {successful_downloads}")
    print(f"   ❌ Failed downloads: {failed_downloads}")
    print(f"   📈 Success rate: {(successful_downloads/len(tracks)*100):.1f}%")
    
    print(f"\n📁 FILES CREATED:")
    print(f"   🎵 Songs folder: {songs_folder}")
    print(f"   📄 Track data: {tracks_file}")
    print(f"   📋 Download log: {log_file}")
    
    # Save final summary
    summary_file = os.path.join(base_folder, "download_summary.json")
    summary_data = {
        'download_info': {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'source_url': Config.SPOTIFY_URL,
            'total_tracks': len(tracks),
            'successful_downloads': successful_downloads,
            'failed_downloads': failed_downloads,
            'success_rate': f"{(successful_downloads/len(tracks)*100):.1f}%" if tracks else "0%",
            'songs_folder': songs_folder
        },
        'download_results': download_log
    }
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    
    print(f"   📊 Summary: {summary_file}")
    
    if successful_downloads > 0:
        print(f"\n🎉 Successfully downloaded {successful_downloads} songs!")
        print(f"🎵 Your music is ready in: {songs_folder}")
    else:
        print(f"\n😔 No songs were successfully downloaded.")
        print(f"📋 Check the log file for details: {log_file}")

if __name__ == "__main__":
    main()
