import asyncio
import contextlib
import json
import os
import re
import time
import aiohttp
import shutil
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import yt_dlp
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message
from py_yt import VideosSearch

from ANNIEMUSIC.utils.cookie_handler import COOKIE_PATH
from ANNIEMUSIC.utils.database import is_on_off
from ANNIEMUSIC.utils.downloader import download_audio_concurrent, yt_dlp_download
from ANNIEMUSIC.utils.errors import capture_internal_err
from ANNIEMUSIC.utils.formatters import time_to_seconds
from ANNIEMUSIC.utils.tuning import (
    YTDLP_TIMEOUT,
    YOUTUBE_META_MAX,
    YOUTUBE_META_TTL,
)
from ANNIEMUSIC import LOGGER

_cache: Dict[str, Tuple[float, List[Dict]]] = {}
_cache_lock = asyncio.Lock()
_formats_cache: Dict[str, Tuple[float, List[Dict], str]] = {}
_formats_lock = asyncio.Lock()

# API URL System - DYNAMIC
YOUR_API_URL = None
FALLBACK_API_URL = "https://shrutibots.site"

# Rate limiting protection
_request_timestamps = []
_RATE_LIMIT_WINDOW = 60  # 60 seconds window
_MAX_REQUESTS_PER_WINDOW = 10  # Max 10 requests per minute

async def load_api_url():
    global YOUR_API_URL
    logger = LOGGER("ANNIEMUSIC.platforms.Youtube.py")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://pastebin.com/raw/rLsBhAQa", timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    content = await response.text()
                    YOUR_API_URL = content.strip()
                    logger.info(f"✅ API URL loaded successfully: {YOUR_API_URL}")
                else:
                    YOUR_API_URL = FALLBACK_API_URL
                    logger.info("ℹ️ Using fallback API URL")
    except Exception:
        YOUR_API_URL = FALLBACK_API_URL
        logger.info("ℹ️ Using fallback API URL")
    
    return YOUR_API_URL

# Initialize API URL on startup
try:
    loop = asyncio.get_event_loop()
    if loop.is_running():
        asyncio.create_task(load_api_url())
    else:
        loop.run_until_complete(load_api_url())
except RuntimeError:
    pass

def _cookiefile_path() -> Optional[str]:
    path = str(COOKIE_PATH)
    try:
        if path and os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    except Exception:
        pass
    return None

def _cookies_args() -> List[str]:
    p = _cookiefile_path()
    return ["--cookies", p] if p else []

async def _exec_proc(*args: str) -> Tuple[bytes, bytes]:
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        return await asyncio.wait_for(proc.communicate(), timeout=YTDLP_TIMEOUT)
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            proc.kill()
        return b"", b"timeout"

# Rate limiting check
def _check_rate_limit():
    global _request_timestamps
    now = time.time()
    
    # Remove timestamps older than our window
    _request_timestamps = [ts for ts in _request_timestamps if now - ts < _RATE_LIMIT_WINDOW]
    
    # Check if we've exceeded the limit
    if len(_request_timestamps) >= _MAX_REQUESTS_PER_WINDOW:
        sleep_time = _RATE_LIMIT_WINDOW - (now - _request_timestamps[0])
        time.sleep(sleep_time)
        _request_timestamps = []  # Reset after sleep
    
    # Add current timestamp
    _request_timestamps.append(now)

# Helper function for stream download
async def _download_from_stream(session: aiohttp.ClientSession, stream_url: str, file_path: str, video_id: str) -> Optional[str]:
    """Download from stream URL"""
    try:
        async with session.get(
            stream_url,
            timeout=aiohttp.ClientTimeout(total=120)  # 2 minutes
        ) as file_response:
            if file_response.status != 200:
                return None
            
            with open(file_path, "wb") as f:
                async for chunk in file_response.content.iter_chunked(16384):
                    f.write(chunk)
            
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                return file_path
            else:
                return None
    except Exception:
        return None

# TELEGRAM DOWNLOAD
async def get_telegram_file(telegram_link: str, video_id: str, file_type: str) -> str:
    """
    TELEGRAM DOWNLOAD
    """
    try:
        extension = ".webm" if file_type == "audio" else ".mkv"
        file_path = os.path.join("downloads", f"{video_id}{extension}")
        
        # LOCAL FILE CHECK
        if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
            return file_path
        
        parsed = urlparse(telegram_link)
        parts = parsed.path.strip("/").split("/")
        
        if len(parts) < 2:
            return None
            
        channel_name = parts[0]
        message_id = int(parts[1])
        
        from ANNIEMUSIC import app
        
        max_retries = 1
        for attempt in range(max_retries):
            try:
                timeout_msg = 6.0
                timeout_download = 12.0
                
                # GET MESSAGE
                msg = await asyncio.wait_for(
                    app.get_messages(channel_name, message_id), 
                    timeout=timeout_msg
                )
                
                if not msg or not msg.document and not msg.video and not msg.audio:
                    return None
                
                os.makedirs("downloads", exist_ok=True)
                
                # DOWNLOAD
                await asyncio.wait_for(
                    msg.download(file_name=file_path),
                    timeout=timeout_download
                )
                
                # FILE VERIFICATION
                if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
                    print("✅ Telegram download")
                    return file_path
                else:
                    return None
                    
            except asyncio.TimeoutError:
                return None
            except Exception:
                return None
        
        return None
        
    except Exception:
        return None

# API DOWNLOAD FUNCTIONS FROM Test.py
async def download_song(link: str) -> str:
    global YOUR_API_URL
    
    if not YOUR_API_URL:
        await load_api_url()
        if not YOUR_API_URL:
            YOUR_API_URL = FALLBACK_API_URL
    
    video_id = link.split('v=')[-1].split('&')[0] if 'v=' in link else link

    if not video_id or len(video_id) < 3:
        return None

    DOWNLOAD_DIR = "downloads"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp3")

    if os.path.exists(file_path):
        return file_path

    try:
        async with aiohttp.ClientSession() as session:
            params = {"url": video_id, "type": "audio"}
            
            async with session.get(
                f"{YOUR_API_URL}/download",
                params=params,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status != 200:
                    return None

                data = await response.json()
                download_token = data.get("download_token")
                
                if not download_token:
                    return None
                
                stream_url = f"{YOUR_API_URL}/stream/{video_id}?type=audio"
                
                async with session.get(
                    stream_url,
                    headers={"X-Download-Token": download_token},
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as file_response:
                    if file_response.status != 200:
                        return None
                        
                    with open(file_path, "wb") as f:
                        async for chunk in file_response.content.iter_chunked(16384):
                            f.write(chunk)
                    
                    return file_path

    except Exception:
        return None

async def download_video_ytdlp(link: str) -> str:
    """
    Download video using yt-dlp directly with cookies (FIRST TRY)
    """
    video_id = link.split('v=')[-1].split('&')[0] if 'v=' in link else link

    if not video_id or len(video_id) < 3:
        return None

    DOWNLOAD_DIR = "downloads"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp4")

    if os.path.exists(file_path):
        return file_path

    # Rate limiting check
    _check_rate_limit()
    
    try:
        # Prepare yt-dlp options with cookies
        ytdlp_opts = [
            "yt-dlp",
            *(_cookies_args()),
            "--no-warnings",
            "--geo-bypass",
            "--force-ipv4",
            "-f",
            "best[height<=?720][width<=?1280]/best",
            "-o",
            file_path,
            link
        ]
        
        # Execute yt-dlp command
        stdout, stderr = await _exec_proc(*ytdlp_opts)
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
            print("✅ Video downloaded via yt-dlp with cookies")
            return file_path
        else:
            # Try alternative formats if first attempt fails
            alternative_formats = [
                "best[ext=mp4]",
                "best",
                "worst[ext=mp4]",
                "worst"
            ]
            
            for fmt in alternative_formats:
                try:
                    ytdlp_opts = [
                        "yt-dlp",
                        *(_cookies_args()),
                        "--no-warnings",
                        "--geo-bypass",
                        "--force-ipv4",
                        "-f",
                        fmt,
                        "-o",
                        file_path,
                        link
                    ]
                    
                    stdout, stderr = await _exec_proc(*ytdlp_opts)
                    
                    if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
                        print(f"✅ Video downloaded via yt-dlp with format: {fmt}")
                        return file_path
                    
                    await asyncio.sleep(1)
                except Exception:
                    continue
            
            return None

    except Exception as e:
        print(f"❌ yt-dlp video download error: {str(e)}")
        return None

async def download_video_api(link: str) -> str:
    """
    Download video using API (FALLBACK METHOD)
    """
    global YOUR_API_URL
    
    if not YOUR_API_URL:
        await load_api_url()
        if not YOUR_API_URL:
            YOUR_API_URL = FALLBACK_API_URL
    
    video_id = link.split('v=')[-1].split('&')[0] if 'v=' in link else link

    if not video_id or len(video_id) < 3:
        return None

    DOWNLOAD_DIR = "downloads"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp4")

    if os.path.exists(file_path):
        return file_path

    try:
        async with aiohttp.ClientSession() as session:
            params = {"url": video_id, "type": "video"}
            
            async with session.get(
                f"{YOUR_API_URL}/download",
                params=params,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status != 200:
                    return None

                data = await response.json()
                download_token = data.get("download_token")
                
                if not download_token:
                    return None
                
                stream_url = f"{YOUR_API_URL}/stream/{video_id}?type=video"
                
                async with session.get(
                    stream_url,
                    headers={"X-Download-Token": download_token},
                    timeout=aiohttp.ClientTimeout(total=600)
                ) as file_response:
                    if file_response.status != 200:
                        return None
                        
                    with open(file_path, "wb") as f:
                        async for chunk in file_response.content.iter_chunked(16384):
                            f.write(chunk)
                    
                    return file_path

    except Exception as e:
        print(f"❌ API video download error: {str(e)}")
        return None

async def download_video(link: str) -> str:
    """
    Main video download function - tries yt-dlp first, then API fallback
    """
    print("🎬 Trying yt-dlp for video download...")
    
    # FIRST TRY: yt-dlp with cookies
    result = await download_video_ytdlp(link)
    if result:
        return result
    
    print("🔄 yt-dlp failed, trying API fallback...")
    
    # SECOND TRY: API fallback
    result = await download_video_api(link)
    if result:
        return result
    
    print("❌ Both yt-dlp and API failed for video download")
    return None

# AUDIO DOWNLOAD WITH YT-DLP (FIXED VERSION)
async def download_audio_ytdlp(link: str) -> str:
    """
    Download audio using yt-dlp with proper cookies
    """
    video_id = link.split('v=')[-1].split('&')[0] if 'v=' in link else link

    if not video_id or len(video_id) < 3:
        return None

    DOWNLOAD_DIR = "downloads"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.webm")

    if os.path.exists(file_path):
        return file_path

    # Rate limiting check
    _check_rate_limit()
    
    try:
        # Prepare yt-dlp options with cookies for audio
        ytdlp_opts = [
            "yt-dlp",
            *(_cookies_args()),
            "--no-warnings",
            "--geo-bypass",
            "--force-ipv4",
            "-f",
            "bestaudio[ext=webm]/bestaudio",
            "--extract-audio",
            "--audio-format", "webm",
            "-o",
            file_path,
            link
        ]
        
        # Execute yt-dlp command
        stdout, stderr = await _exec_proc(*ytdlp_opts)
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
            print("✅ Audio downloaded via yt-dlp with cookies")
            return file_path
        else:
            # Try alternative audio formats
            alternative_formats = [
                "bestaudio[ext=m4a]/bestaudio",
                "bestaudio/best",
                "worstaudio"
            ]
            
            for fmt in alternative_formats:
                try:
                    alt_file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.webm")
                    ytdlp_opts = [
                        "yt-dlp",
                        *(_cookies_args()),
                        "--no-warnings",
                        "--geo-bypass",
                        "--force-ipv4",
                        "-f",
                        fmt,
                        "--extract-audio",
                        "--audio-format", "webm",
                        "-o",
                        alt_file_path,
                        link
                    ]
                    
                    stdout, stderr = await _exec_proc(*ytdlp_opts)
                    
                    if os.path.exists(alt_file_path) and os.path.getsize(alt_file_path) > 10240:
                        print(f"✅ Audio downloaded via yt-dlp with format: {fmt}")
                        return alt_file_path
                    
                    await asyncio.sleep(1)
                except Exception:
                    continue
            
            return None

    except Exception as e:
        print(f"❌ yt-dlp audio download error: {str(e)}")
        return None

# Original API DOWNLOAD (kept for backward compatibility)
async def download_via_api(link: str, download_type: str = "audio") -> Optional[str]:
    """API DOWNLOAD"""
    
    if download_type == "video":
        # Use video download function with fallback
        return await download_video(link)
    
    # Use new audio download function
    return await download_song(link)

@capture_internal_err
async def cached_youtube_search(query: str) -> List[Dict]:
    key = f"q:{query}"
    now = time.time()
    async with _cache_lock:
        if key in _cache:
            ts, val = _cache[key]
            if now - ts < YOUTUBE_META_TTL:
                return val
            _cache.pop(key, None)
        if len(_cache) > YOUTUBE_META_MAX:
            _cache.clear()
    try:
        data = await VideosSearch(query, limit=1).next()
        result = data.get("result", [])
    except Exception:
        result = []
    if result:
        async with _cache_lock:
            _cache[key] = (now, result)
    return result

# Shell command function from Test.py
async def shell_cmd(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, errorz = await proc.communicate()
    if errorz:
        if "unavailable videos are hidden" in (errorz.decode("utf-8")).lower():
            return out.decode("utf-8")
        else:
            return errorz.decode("utf-8")
    return out.decode("utf-8")

class YouTubeAPI:
    def __init__(self) -> None:
        self.base_url = "https://www.youtube.com/watch?v="
        self.playlist_url = "https://youtube.com/playlist?list="
        self.status = "https://www.youtube.com/oembed?url="
        self._url_pattern = re.compile(r"(?:youtube\.com|youtu\.be)")
        self.reg = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

    def _prepare_link(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> str:
        if isinstance(videoid, str) and videoid.strip():
            link = self.base_url + videoid.strip()
        if "youtu.be" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]
        elif "youtube.com/shorts/" in link or "youtube.com/live/" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]
        return link.split("&")[0]

    # URL extraction method
    @capture_internal_err
    async def url(self, message: Message) -> Optional[str]:
        """
        Extract YouTube URL from message
        """
        msgs = [message] + (
            [message.reply_to_message] if message.reply_to_message else []
        )
        for msg in msgs:
            text = msg.text or msg.caption or ""
            entities = msg.entities or msg.caption_entities or []
            for ent in entities:
                if ent.type == MessageEntityType.URL:
                    url = text[ent.offset : ent.offset + ent.length]
                    if self._url_pattern.search(url):
                        return url
                if ent.type == MessageEntityType.TEXT_LINK:
                    url = ent.url
                    if self._url_pattern.search(url):
                        return url
        return None

    @capture_internal_err
    async def exists(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> bool:
        return bool(self._url_pattern.search(self._prepare_link(link, videoid)))

    @capture_internal_err
    async def _fetch_video_info(
        self, query: str, *, use_cache: bool = True
    ) -> Optional[Dict]:
        q = self._prepare_link(query)
        if use_cache and not q.startswith("http"):
            res = await cached_youtube_search(q)
            return res[0] if res else None
        data = await VideosSearch(q, limit=1).next()
        result = data.get("result", [])
        return result[0] if result else None

    @capture_internal_err
    async def is_live(self, link: str) -> bool:
        # Rate limiting check
        _check_rate_limit()
        
        prepared = self._prepare_link(link)
        stdout, _ = await _exec_proc(
            "yt-dlp", *(_cookies_args()), "--dump-json", prepared
        )
        if not stdout:
            return False
        try:
            info = json.loads(stdout.decode())
            return bool(info.get("is_live"))
        except json.JSONDecodeError:
            return False

    @capture_internal_err
    async def details(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], int, str, str]:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        if not info:
            raise ValueError("Video not found")
        dt = info.get("duration")
        ds = int(time_to_seconds(dt)) if dt else 0
        thumb = (
            info.get("thumbnail")
            or info.get("thumbnails", [{}])[0].get("url", "")
        ).split("?")[0]
        return info.get("title", ""), dt, ds, thumb, info.get("id", "")

    @capture_internal_err
    async def title(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> str:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        return info.get("title", "") if info else ""

    @capture_internal_err
    async def duration(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Optional[str]:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        return info.get("duration") if info else None

    @capture_internal_err
    async def thumbnail(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> str:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        if info:
            thumb = info.get("thumbnail") or info.get("thumbnails", [{}])[0].get("url", "")
            return thumb.split("?")[0] if thumb else ""
        return ""

    @capture_internal_err
    async def video(self, link: str, videoid: Union[str, bool, None] = None) -> Tuple[int, str]:
        link = self._prepare_link(link, videoid)
        
        # Try yt-dlp video download function first
        try:
            downloaded_file = await download_video(link)
            if downloaded_file:
                return (1, downloaded_file)
        except Exception:
            pass
        
        # Rate limiting check
        _check_rate_limit()
        
        ytdlp_args = [
            "yt-dlp",
            *(_cookies_args()),
            "--no-warnings",
            "--geo-bypass",
            "--force-ipv4",
            "-g",
            "-f",
            "best[height<=?720][width<=?1280]/best",
            link,
        ]
        
        stdout, stderr = await _exec_proc(*ytdlp_args)
        
        if stdout:
            stream_url = stdout.decode().split("\n")[0]
            if stream_url and stream_url.startswith('http'):
                return (1, stream_url)
            else:
                return (0, "Invalid stream URL")
        else:
            error_msg = stderr.decode() if stderr else "Unknown error"
            
            if "429" in error_msg or "Too Many Requests" in error_msg:
                await asyncio.sleep(30)
                return (0, "Rate limited")
            elif "403" in error_msg:
                return await self._try_alternative_format(link)
            else:
                return (0, error_msg)

    async def _try_alternative_format(self, link: str) -> Tuple[int, str]:
        """Try alternative formats"""
        
        format_options = [
            "best[height<=480]",
            "best[ext=mp4]", 
            "best",
            "worst"
        ]
        
        for fmt in format_options:
            stdout, stderr = await _exec_proc(
                "yt-dlp",
                *(_cookies_args()),
                "--no-warnings",
                "-g",
                "-f",
                fmt,
                link,
            )
            
            if stdout:
                stream_url = stdout.decode().split("\n")[0]
                if stream_url and stream_url.startswith('http'):
                    return (1, stream_url)
            
            await asyncio.sleep(1)
        
        return (0, "All format attempts failed")

    @capture_internal_err
    async def playlist(
        self, link: str, limit: int, user_id, videoid: Union[str, bool, None] = None
    ) -> List[str]:
        if videoid:
            link = self.playlist_url + str(videoid)
        link = link.split("&")[0]
        
        # Rate limiting check
        _check_rate_limit()
        
        # Use shell_cmd method from Test.py
        playlist = await shell_cmd(
            f"yt-dlp -i --get-id --flat-playlist --playlist-end {limit} --skip-download {link}"
        )
        try:
            items = [key for key in playlist.split("\n") if key]
        except:
            items = []
        return items

    @capture_internal_err
    async def track(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[Dict, str]:
        try:
            info = await self._fetch_video_info(self._prepare_link(link, videoid))
            if not info:
                raise ValueError("Track not found via API")
        except Exception:
            # Rate limiting check
            _check_rate_limit()
            
            prepared = self._prepare_link(link, videoid)
            stdout, _ = await _exec_proc(
                "yt-dlp", *(_cookies_args()), "--dump-json", prepared
            )
            if not stdout:
                raise ValueError("Track not found (yt-dlp fallback)")
            info = json.loads(stdout.decode())
        thumb = (
            info.get("thumbnail")
            or info.get("thumbnails", [{}])[0].get("url", "")
        ).split("?")[0]
        details = {
            "title": info.get("title", ""),
            "link": info.get("webpage_url", self._prepare_link(link, videoid)),
            "vidid": info.get("id", ""),
            "duration_min": info.get("duration")
            if isinstance(info.get("duration"), str)
            else None,
            "thumb": thumb,
        }
        return details, info.get("id", "")

    @capture_internal_err
    async def formats(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[List[Dict], str]:
        link = self._prepare_link(link, videoid)
        key = f"f:{link}"
        now = time.time()
        async with _formats_lock:
            cached = _formats_cache.get(key)
            if cached and now - cached[0] < YOUTUBE_META_TTL:
                return cached[1], cached[2]

        # Rate limiting check
        _check_rate_limit()
        
        opts = {"quiet": True}
        cf = _cookiefile_path()
        if cf:
            opts["cookiefile"] = cf
        out: List[Dict] = []
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(link, download=False)
                for fmt in info.get("formats", []):
                    # Skip dash formats
                    if "dash" in str(fmt.get("format", "")).lower():
                        continue
                    # Check for required keys
                    if not any(k in fmt for k in ("filesize", "filesize_approx")):
                        continue
                    if not all(k in fmt for k in ("format", "format_id", "ext", "format_note")):
                        continue
                    size = fmt.get("filesize") or fmt.get("filesize_approx")
                    if not size:
                        continue
                    out.append(
                        {
                            "format": fmt["format"],
                            "filesize": size,
                            "format_id": fmt["format_id"],
                            "ext": fmt["ext"],
                            "format_note": fmt["format_note"],
                            "yturl": link,
                        }
                    )
        except Exception:
            pass

        async with _formats_lock:
            if len(_formats_cache) > YOUTUBE_META_MAX:
                _formats_cache.clear()
            _formats_cache[key] = (now, out, link)

        return out, link

    @capture_internal_err
    async def slider(
        self, link: str, query_type: int, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], str, str]:
        data = await VideosSearch(self._prepare_link(link, videoid), limit=10).next()
        results = data.get("result", [])
        if not results or query_type >= len(results):
            raise IndexError(
                f"Query type index {query_type} out of range (found {len(results)} results)"
            )
        r = results[query_type]
        return (
            r.get("title", ""),
            r.get("duration"),
            r.get("thumbnails", [{}])[0].get("url", "").split("?")[0],
            r.get("id", ""),
        )

    # MAIN DOWNLOAD FUNCTION - ENHANCED WITH NEW API FUNCTIONS
    @capture_internal_err
    async def download(
        self,
        link: str,
        mystic,
        *,
        video: Union[bool, str, None] = None,
        videoid: Union[str, bool, None] = None,
        songaudio: Union[bool, str, None] = None,
        songvideo: Union[bool, str, None] = None,
        format_id: Union[bool, str, None] = None,
        title: Union[bool, str, None] = None,
    ) -> Union[Tuple[str, Optional[bool]], Tuple[None, None]]:
        link = self._prepare_link(link, videoid)
        video_id = link.split('v=')[-1].split('&')[0] if 'v=' in link else link
        
        # COMMON FILE PATH CHECK
        extension = ".webm" if not video else ".mp4"
        common_file_path = os.path.join("downloads", f"{video_id}{extension}")
        
        # 1. LOCAL CACHE CHECK (FASTEST)
        if os.path.exists(common_file_path) and os.path.getsize(common_file_path) > 10240:
            print("✅ Local cache")
            return common_file_path, True

        # VIDEO KE LIYE
        if songvideo or video:
            # Try yt-dlp video download function first
            try:
                downloaded_file = await download_video(link)
                if downloaded_file:
                    print("✅ yt-dlp video download with cookies")
                    
                    # Convert to common file path if needed
                    if downloaded_file != common_file_path and downloaded_file.endswith('.mp4'):
                        try:
                            shutil.move(downloaded_file, common_file_path)
                            return common_file_path, True
                        except Exception:
                            return downloaded_file, True
                    return downloaded_file, True
            except Exception as e:
                print(f"❌ Video download error: {str(e)}")
            
            # Fallback to direct stream
            status, stream_url = await self.video(link)
            if status == 1:
                print("✅ Video stream")
                return stream_url, None
            else:
                return None, None

        # AUDIO KE LIYE - ALL METHODS IN ORDER
        else:
            # 2. TRY NEW download_song FUNCTION (API)
            try:
                api_result = await download_song(link)
                if api_result:
                    print("✅ New API audio download")
                    
                    # Convert .mp3 to .webm if needed
                    if api_result.endswith('.mp3') and common_file_path.endswith('.webm'):
                        mp3_path = api_result
                        webm_path = common_file_path
                        try:
                            # Simple conversion or just rename if format doesn't matter
                            shutil.move(mp3_path, webm_path)
                            return webm_path, True
                        except Exception:
                            return api_result, True
                    return api_result, True
            except Exception as e:
                print(f"❌ API audio download error: {str(e)}")
            
            # 3. TRY YT-DLP AUDIO WITH PROPER COOKIES
            try:
                ytdlp_result = await download_audio_ytdlp(link)
                if ytdlp_result:
                    print("✅ yt-dlp audio with cookies")
                    
                    # MOVE TO COMMON LOCATION IF NEEDED
                    if ytdlp_result != common_file_path:
                        try:
                            shutil.move(ytdlp_result, common_file_path)
                            return common_file_path, True
                        except Exception:
                            return ytdlp_result, True
                    return ytdlp_result, True
            except Exception as e:
                print(f"❌ yt-dlp audio error: {str(e)}")
            
            # 4. TRY OLD API METHOD
            api_result = await download_via_api(link, "audio")
            if api_result:
                return api_result, True
            
            # 5. TRY YT-DLP (DIRECT - NO is_on_off CONDITION)
            try:
                p = await yt_dlp_download(link, type="audio")
                if p and os.path.exists(p) and os.path.getsize(p) > 10240:
                    print("✅ yt-dlp (original)")
                    
                    # MOVE TO COMMON LOCATION IF NEEDED
                    if p != common_file_path:
                        try:
                            shutil.move(p, common_file_path)
                            return common_file_path, True
                        except Exception:
                            return p, True
                    return p, True
            except Exception as e:
                print(f"❌ Original yt-dlp error: {str(e)}")
            
            # 6. TRY CONCURRENT AS LAST RESORT
            try:
                p = await download_audio_concurrent(link)
                if p and os.path.exists(p) and os.path.getsize(p) > 10240:
                    print("✅ concurrent")
                    
                    # MOVE TO COMMON LOCATION IF NEEDED
                    if p != common_file_path:
                        try:
                            shutil.move(p, common_file_path)
                            return common_file_path, True
                        except Exception:
                            return p, True
                    return p, True
            except Exception as e:
                print(f"❌ Concurrent download error: {str(e)}")
            
            # 7. ALL FAILED
            print("❌ All audio download methods failed")
            return None, None
