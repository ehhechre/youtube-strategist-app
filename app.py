# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta
import plotly.express as px
import re
import hashlib
import pickle
from pathlib import Path
import sqlite3
import threading
import warnings
import time
import logging
from pytrends.request import TrendReq
import numpy as np
from collections import Counter
import unicodedata

# --- 1. КОНФИГУРАЦИЯ СТРАНИЦЫ И ЛОГИРОВАНИЕ ---
st.set_page_config(
    page_title="YouTube AI Strategist 🧠",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)
warnings.filterwarnings('ignore')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youtube_strategist.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Константы для API лимитов
YOUTUBE_API_DAILY_QUOTA = 10000
REQUEST_DELAY = 0.1
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

st.markdown("""
<style>
    .main-header {
        font-size: 2.8rem;
        color: #FF0000;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
        background: linear-gradient(90deg, #FF0000 0%, #FF6B6B 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .stButton>button {
        border-radius: 8px;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    .custom-container {
        background: linear-gradient(135deg, rgba(42, 57, 62, 0.5), rgba(62, 77, 82, 0.3));
        padding: 1.5rem;
        border-radius: 15px;
        border-left: 5px solid #00a0dc;
        margin-top: 1rem;
        backdrop-filter: blur(10px);
    }
    .insight-box {
        background: linear-gradient(135deg, #262730, #3a3b45);
        padding: 1rem;
        border-radius: 15px;
        margin-top: 1rem;
        border: 1px solid #444;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }
    .metric-card {
        background: linear-gradient(135deg, #1e1e2e, #2a2a3e);
        padding: 1rem;
        border-radius: 10px;
        border: 1px solid #444;
        margin: 0.5rem 0;
    }
    .success-alert {
        background: linear-gradient(135deg, rgba(34, 197, 94, 0.1), rgba(34, 197, 94, 0.05));
        border: 1px solid rgba(34, 197, 94, 0.3);
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .warning-alert {
        background: linear-gradient(135deg, rgba(251, 146, 60, 0.1), rgba(251, 146, 60, 0.05));
        border: 1px solid rgba(251, 146, 60, 0.3);
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. УТИЛИТЫ И ВАЛИДАЦИЯ ---

def validate_youtube_api_key(api_key: str) -> bool:
    """Проверка формата YouTube API ключа"""
    if not api_key or not isinstance(api_key, str):
        return False
    api_key = api_key.strip()
    if api_key.startswith('AIza') and len(api_key) == 39:
        return True
    if len(api_key) > 30 and re.match(r'^[A-Za-z0-9_-]+$', api_key):
        return True
    return False

def safe_format_number(num) -> str:
    """Безопасное форматирование чисел"""
    try:
        if pd.isna(num) or num is None:
            return "0"
        num = float(num)
        if num >= 1_000_000_000:
            return f"{num/1_000_000_000:.1f}B"
        elif num >= 1_000_000:
            return f"{num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num/1_000:.1f}K"
        return str(int(num))
    except (ValueError, TypeError, OverflowError):
        return "0"

def clean_text(text: str) -> str:
    """Очистка текста от проблемных символов"""
    if not text or not isinstance(text, str):
        return ""
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
    return text.strip()

def safe_int_conversion(value, default=0) -> int:
    """Безопасное преобразование в int"""
    try:
        if pd.isna(value) or value is None:
            return default
        return int(float(value))
    except (ValueError, TypeError, OverflowError):
        return default

def safe_float_conversion(value, default=0.0) -> float:
    """Безопасное преобразование в float"""
    try:
        if pd.isna(value) or value is None:
            return default
        return float(value)
    except (ValueError, TypeError, OverflowError):
        return default

def validate_keyword(keyword: str) -> bool:
    """Валидация ключевого слова"""
    if not keyword or not isinstance(keyword, str):
        return False
    keyword = keyword.strip()
    if len(keyword) < 2 or len(keyword) > 100:
        return False
    if keyword.count(' ') > 10:
        return False
    if re.search(r'[<>"\'\[\]{}|\\`]', keyword):
        return False
    return True

def retry_api_call(func, max_retries=MAX_RETRIES, delay=REQUEST_DELAY):
    """Декоратор для повторных попыток API вызовов"""
    def wrapper(*args, **kwargs):
        last_exception = None
        for attempt in range(max_retries):
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    logger.info(f"API вызов успешен с {attempt + 1} попытки")
                return result
            except HttpError as e:
                last_exception = e
                status_code = e.resp.status
                if status_code == 403:
                    st.error("❌ Превышена квота YouTube API или доступ запрещен. Проверьте ключ и его ограничения в Google Cloud Console.")
                    logger.error(f"Ошибка 403 (Forbidden). Детали: {e.content}")
                    break
                elif status_code == 400:
                    st.error("❌ Неверный запрос к YouTube API. Возможно, некорректные параметры.")
                    logger.error(f"Ошибка 400 (Bad Request). Детали: {e.content}")
                    break
                elif status_code in [500, 502, 503, 504]:
                    logger.warning(f"Серверная ошибка {status_code}, попытка {attempt + 1}")
                    if attempt < max_retries - 1:
                        time.sleep(delay * (2 ** attempt))
                        continue
                else:
                    logger.error(f"HTTP ошибка {status_code}: {e}")
                    break
            except Exception as e:
                last_exception = e
                logger.warning(f"Ошибка API вызова (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(delay)
                    continue
                break
        logger.error(f"API вызов не удался после {max_retries} попыток: {last_exception}")
        raise last_exception
    return wrapper

# --- 3. КЛАССЫ-АНАЛИЗАТОРЫ ---

class CacheManager:
    def __init__(self, cache_dir: str = "data/cache"):
        self.db_path = Path(cache_dir) / "youtube_ai_cache.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self._init_sqlite()
        self.ttl_map = {
            'search': 3600 * 4,
            'channels': 3600 * 24 * 7,
            'trends': 3600 * 8,
        }
        self.stats = {'hits': 0, 'misses': 0, 'errors': 0}

    def _init_sqlite(self):
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10.0)
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY, value BLOB, expires_at TIMESTAMP
                    )
                ''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_expires_at ON cache(expires_at)')
                conn.commit()
                conn.close()
        except sqlite3.Error as e:
            logger.error(f"Ошибка инициализации кэша: {e}")
            st.error(f"Критическая ошибка инициализации кэша: {e}")

    def get(self, key: str):
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM cache WHERE key = ? AND expires_at > ?", (key, datetime.now()))
                result = cursor.fetchone()
                conn.close()
                if result:
                    self.stats['hits'] += 1
                    return pickle.loads(result[0])
                self.stats['misses'] += 1
                return None
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Ошибка чтения из кэша: {e}")
            return None

    def set(self, key: str, value: any, category: str):
        try:
            with self.lock:
                ttl = self.ttl_map.get(category, 3600)
                expires_at = datetime.now() + timedelta(seconds=ttl)
                value_blob = pickle.dumps(value)
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute("INSERT OR REPLACE INTO cache (key, value, expires_at) VALUES (?, ?, ?)", (key, value_blob, expires_at))
                conn.commit()
                conn.close()
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Ошибка записи в кэш: {e}")

    def clean_expired(self) -> int:
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM cache WHERE expires_at < ?", (datetime.now(),))
                expired_count = cursor.rowcount
                conn.commit()
                conn.close()
                return expired_count
        except Exception as e:
            logger.error(f"Ошибка очистки кэша: {e}")
            return 0

    def get_cache_info(self) -> dict:
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM cache")
                count = cursor.fetchone()[0]
                conn.close()
                size_mb = self.db_path.stat().st_size / (1024 * 1024) if self.db_path.exists() else 0
                return {
                    'total_records': count or 0,
                    'total_size_mb': round(size_mb, 2),
                    'hit_rate': round(self.stats['hits'] / max(self.stats['hits'] + self.stats['misses'], 1) * 100, 1)
                }
        except Exception as e:
            logger.error(f"Ошибка получения информации о кэше: {e}")
            return {'error': str(e)}

    def generate_key(self, *args) -> str:
        """Генерация ключей для кэша."""
        combined = "|".join(map(str, args))
        return hashlib.md5(combined.encode('utf-8')).hexdigest()

class YouTubeAnalyzer:
    def __init__(self, api_key: str, cache: CacheManager):
        try:
            self.youtube = build('youtube', 'v3', developerKey=api_key)
            self.cache = cache
            self.api_key = api_key
            self.quota_used = 0
            logger.info("YouTube API инициализирован успешно")
        except Exception as e:
            logger.error(f"Ошибка инициализации YouTube API: {e}")
            raise

    def test_connection(self) -> bool:
        """Тестирование соединения с YouTube API"""
        try:
            self.youtube.i18nLanguages().list(part='snippet', hl='en').execute()
            logger.info("YouTube API соединение успешно протестировано.")
            return True
        except HttpError as e:
            logger.error(f"Тест соединения с YouTube API не удался: {e}")
            details = e.error_details[0] if hasattr(e, 'error_details') and e.error_details else {}
            st.error(f"❌ Ошибка подключения к YouTube: {e.resp.status} - {details.get('reason', 'Unknown')}. Проверьте ваш API ключ.")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при тесте соединения с YouTube API: {e}")
            return False

    def _make_api_request(self, request_func, *args, **kwargs):
        """Обертка для API запросов с обработкой ошибок и квот"""
        try:
            if self.quota_used > YOUTUBE_API_DAILY_QUOTA * 0.9:
                st.warning("⚠️ Приближаемся к лимиту YouTube API квоты")
            response = retry_api_call(request_func)(*args, **kwargs).execute()
            return response
        except HttpError as e:
            if e.resp.status == 403:
                st.error("❌ Превышена квота YouTube API или доступ ограничен")
            elif e.resp.status == 400:
                st.error("❌ Некорректный запрос к YouTube API")
            else:
                st.error(f"❌ Ошибка YouTube API: {e}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка API: {e}")
            raise

    def get_channel_stats(self, channel_ids: list):
        """Получение статистики каналов"""
        if not channel_ids: return {}
        unique_ids = list(set(filter(None, channel_ids)))
        if not unique_ids: return {}
        cache_key = self.cache.generate_key('channels', sorted(unique_ids))
        if cached_data := self.cache.get(cache_key):
            return cached_data
        
        channel_stats = {}
        try:
            for i in range(0, len(unique_ids), 50):
                chunk_ids = unique_ids[i:i+50]
                request = self.youtube.channels().list(part="statistics,snippet,brandingSettings", id=",".join(chunk_ids))
                response = self._make_api_request(lambda: request)
                self.quota_used += 1

                for item in response.get('items', []):
                    stats = item.get('statistics', {})
                    snippet = item.get('snippet', {})
                    
                    channel_stats[item['id']] = {
                        'subscribers': safe_int_conversion(stats.get('subscriberCount', 0)),
                        'total_views': safe_int_conversion(stats.get('viewCount', 0)),
                        'video_count': safe_int_conversion(stats.get('videoCount', 0)),
                        'title': clean_text(snippet.get('title', 'Неизвестно')),
                        'verified': 'verified' in str(snippet.get('thumbnails', {})),
                    }
                if i + 50 < len(unique_ids):
                    time.sleep(REQUEST_DELAY)
            
            self.cache.set(cache_key, channel_stats, 'channels')
            logger.info(f"Получена статистика для {len(channel_stats)} каналов")
            return channel_stats
        except Exception as e:
            logger.error(f"Ошибка получения статистики каналов: {e}")
            st.warning(f"Не удалось получить полную статистику каналов: {e}")
            return {}

    def search_videos(self, keyword: str, max_results: int = 100, published_after=None):
        """Поиск видео с получением детальной статистики"""
        if not validate_keyword(keyword):
            st.error("❌ Некорректное ключевое слово")
            return None
        max_results = min(max_results, 500)
        
        cache_key = self.cache.generate_key('search_v5_simplified', keyword, max_results, published_after)
        if cached_data := self.cache.get(cache_key):
            st.toast("🚀 Результаты поиска загружены из кэша!", icon="⚡️")
            return cached_data
        
        try:
            video_snippets = []
            next_page_token = None
            search_params = {'q': clean_text(keyword), 'part': 'snippet', 'type': 'video', 'order': 'relevance'}
            if published_after:
                search_params['publishedAfter'] = published_after

            progress_bar = st.progress(0)
            status_text = st.empty()
            
            fetched_count = 0
            while fetched_count < max_results:
                search_params['maxResults'] = min(50, max_results - fetched_count)
                if next_page_token:
                    search_params['pageToken'] = next_page_token
                
                status_text.text(f"🔍 Ищем видео: {fetched_count}/{max_results}")
                progress_bar.progress(fetched_count / max_results)
                
                request = self.youtube.search().list(**search_params)
                search_response = self._make_api_request(lambda: request)
                self.quota_used += 100 # Search operation is costly
                new_items = search_response.get('items', [])
                
                if not new_items: break
                video_snippets.extend(new_items)
                fetched_count = len(video_snippets)
                next_page_token = search_response.get('nextPageToken')
                if not next_page_token: break
                time.sleep(REQUEST_DELAY)

            progress_bar.progress(1.0)
            status_text.text(f"✅ Найдено {len(video_snippets)} видео. Собираем детали...")
            
            if not video_snippets: return []

            video_ids = [item['id']['videoId'] for item in video_snippets if 'videoId' in item.get('id', {})]
            channel_ids = list(set([item['snippet']['channelId'] for item in video_snippets]))

            status_text.text("📊 Получаем статистику каналов...")
            channel_stats = self.get_channel_stats(channel_ids)
            
            videos = []
            all_video_details = []
            for i in range(0, len(video_ids), 50):
                chunk_ids = video_ids[i:i+50]
                status_text.text(f"📊 Получаем детали видео ({i+len(chunk_ids)}/{len(video_ids)})...")
                
                request = self.youtube.videos().list(part='statistics,contentDetails,snippet', id=','.join(chunk_ids))
                stats_response = self._make_api_request(lambda: request)
                self.quota_used += 1
                all_video_details.extend(stats_response.get('items', []))
                if i + 50 < len(video_ids):
                    time.sleep(REQUEST_DELAY)

            video_details_map = {item['id']: item for item in all_video_details}
            
            for snippet_item in video_snippets:
                video_id = snippet_item['id'].get('videoId')
                if not video_id or video_id not in video_details_map:
                    continue
                
                details = video_details_map[video_id]
                stats = details.get('statistics', {})
                content_details = details.get('contentDetails', {})
                video_snippet = details.get('snippet', {})
                
                duration = self._parse_duration(content_details.get('duration', 'PT0S'))
                channel_id = video_snippet.get('channelId')
                channel_info = channel_stats.get(channel_id, {})
                
                video_data = {
                    'video_id': video_id,
                    'title': clean_text(video_snippet.get('title', '')),
                    'channel': clean_text(video_snippet.get('channelTitle', '')),
                    'channel_id': channel_id,
                    'subscribers': channel_info.get('subscribers', 0),
                    'subscribers_formatted': safe_format_number(channel_info.get('subscribers', 0)),
                    'published': video_snippet.get('publishedAt', ''),
                    'views': safe_int_conversion(stats.get('viewCount', 0)),
                    'views_formatted': safe_format_number(safe_int_conversion(stats.get('viewCount', 0))),
                    'likes': safe_int_conversion(stats.get('likeCount', 0)),
                    'likes_formatted': safe_format_number(safe_int_conversion(stats.get('likeCount', 0))),
                    'comments': safe_int_conversion(stats.get('commentCount', 0)),
                    'duration': duration,
                    'duration_formatted': self._format_duration(duration),
                    'is_short': duration <= 1.05,
                    'short_indicator': "🩳 Shorts" if duration <= 1.05 else "📹 Видео",
                    'tags': video_snippet.get('tags', [])[:20],
                    'thumbnail': snippet_item['snippet'].get('thumbnails', {}).get('medium', {}).get('url', ''),
                    'video_url': f"https://www.youtube.com/watch?v={video_id}",
                    'video_url_markdown': f"[Ссылка](https://www.youtube.com/watch?v={video_id})"
                }
                videos.append(video_data)
            
            progress_bar.empty()
            status_text.empty()
            
            self.cache.set(cache_key, videos, 'search')
            logger.info(f"Поиск завершен: найдено {len(videos)} видео для '{keyword}'")
            return videos
            
        except Exception as e:
            logger.error(f"Критическая ошибка в search_videos для '{keyword}': {e}", exc_info=True)
            st.error(f"Ошибка при поиске видео: {e}")
            return None

    def _parse_duration(self, duration_str: str) -> float:
        """Парсинг продолжительности из формата ISO 8601"""
        if not duration_str: return 0
        try:
            match = re.search(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
            if not match: return 0
            h, m, s = (safe_int_conversion(g) for g in match.groups())
            return h * 60 + m + s / 60
        except Exception as e:
            logger.error(f"Ошибка парсинга продолжительности '{duration_str}': {e}")
            return 0
    
    def _format_duration(self, duration_minutes: float) -> str:
        """Форматирование продолжительности в читаемый вид"""
        try:
            if duration_minutes is None: return "0:00"
            if duration_minutes < 1: return f"0:{int(duration_minutes * 60):02d}"
            total_seconds = int(duration_minutes * 60)
            hours, rem = divmod(total_seconds, 3600)
            minutes, seconds = divmod(rem, 60)
            if hours > 0: return f"{hours}:{minutes:02d}:{seconds:02d}"
            else: return f"{minutes}:{seconds:02d}"
        except Exception:
            return "0:00"
    
    def analyze_competition(self, videos: list):
        """Анализ конкуренции на основе собранных видео"""
        if not videos: return {}, pd.DataFrame()
        try:
            df = pd.DataFrame(videos)
            df['published'] = pd.to_datetime(df['published'], errors='coerce', utc=True).dt.tz_localize(None)
            df = df.dropna(subset=['published', 'views'])
            if df.empty:
                logger.warning("После фильтрации данных DataFrame пуст")
                return {}, pd.DataFrame()
            
            df['days_ago'] = (datetime.now() - df['published']).dt.days.fillna(0)
            df['engagement_rate'] = np.where(df['views'] > 0, ((df['likes'] + df['comments']) / df['views']) * 100, 0)
            
            analysis = {
                'total_videos': len(df),
                'avg_views': safe_float_conversion(df['views'].mean()),
                'median_views': safe_float_conversion(df['views'].median()),
                'top_10_avg_views': safe_float_conversion(df.nlargest(min(10, len(df)), 'views')['views'].mean()),
                'engagement_rate': safe_float_conversion(df['engagement_rate'].mean()),
                'videos_last_week': len(df[df['days_ago'] <= 7]),
                'unique_channels': df['channel'].nunique(),
            }

            score = 0
            if analysis['top_10_avg_views'] < 20000: score += 4
            elif analysis['top_10_avg_views'] < 50000: score += 3
            elif analysis['top_10_avg_views'] < 200000: score += 2
            
            if analysis['videos_last_week'] < 2: score += 3
            elif analysis['videos_last_week'] < 5: score += 2
            
            if analysis['unique_channels'] < 15: score += 2
            elif analysis['unique_channels'] < 30: score += 1
            
            competition_levels = {
                0: 'Экстремально высокая 🔴', 1: 'Очень высокая 🔴', 2: 'Очень высокая 🔴',
                3: 'Высокая 🟠', 4: 'Высокая 🟠', 5: 'Средняя 🟡', 6: 'Средняя 🟡',
                7: 'Низкая 🟢', 8: 'Низкая 🟢', 9: 'Очень низкая 🟢', 10: 'Минимальная 🟢'
            }
            analysis['competition_level'] = competition_levels.get(score, 'Экстремально высокая 🔴')
            return analysis, df
            
        except Exception as e:
            logger.error(f"Ошибка анализа конкуренции: {e}", exc_info=True)
            return {}, pd.DataFrame()

class AdvancedTrendsAnalyzer:
    def __init__(self, cache: CacheManager):
        self.cache = cache
        
    def _get_pytrends(self):
        try:
            return TrendReq(hl='ru-RU', tz=180, timeout=(10, 25), retries=2, backoff_factor=0.1)
        except Exception as e:
            st.warning(f"Ошибка инициализации Google Trends: {e}")
            return None

    def analyze_keyword_trends(self, keyword: str):
        cache_key = self.cache.generate_key('advanced_trends', keyword)
        if cached_data := self.cache.get(cache_key):
            st.toast("📈 Данные трендов загружены из кэша!", icon="⚡️")
            return cached_data
            
        pytrends = self._get_pytrends()
        if not pytrends: return None
            
        try:
            pytrends.build_payload([keyword], timeframe='today 12-m', geo='')
            interest_12m = pytrends.interest_over_time()
            
            if interest_12m.empty or keyword not in interest_12m.columns:
                 st.warning(f"Не удалось найти данные трендов для '{keyword}'.")
                 return None
            
            result = {'interest_df': interest_12m}
            self.cache.set(cache_key, result, 'trends')
            return result
        except Exception as e:
            st.warning(f"Не удалось получить данные из Google Trends: {str(e)}")
            return None

# --- 4. ГЛАВНЫЙ ИНТЕРФЕЙС ---

def main():
    st.markdown('<h1 class="main-header">YouTube Data Strategist 📈</h1>', unsafe_allow_html=True)
    
    with st.sidebar:
        st.header("⚙️ Настройки")
        st.subheader("🔑 YouTube API")
        youtube_api_key = st.text_input("YouTube API Key", type="password", help="Получите ключ в Google Cloud Console", key="youtube_api_key")
        
        if youtube_api_key:
            if validate_youtube_api_key(youtube_api_key):
                st.success("✅ YouTube API ключ выглядит корректно")
            else:
                st.warning("⚠️ Формат ключа может быть неверным")
        
        st.markdown("---")
        st.subheader("🔍 Параметры анализа")
        max_results = st.slider("Видео для анализа", 20, 200, 100, 10, key="max_results")
        date_range_options = {"За все время": None, "За последний год": 365, "За 6 месяцев": 180, "За 3 месяца": 90, "За месяц": 30}
        selected_date_range = st.selectbox("Период анализа:", list(date_range_options.keys()), index=1, key="date_range")
        days_limit = date_range_options[selected_date_range]
        
        if not youtube_api_key:
            st.warning("👆 Введите YouTube API ключ для начала работы")
            st.stop()
        
        cache = CacheManager()
        st.markdown("---")
        st.subheader("💾 Управление кэшем")
        cache_info = cache.get_cache_info()
        if 'error' not in cache_info:
            st.info(f"Записей: {cache_info.get('total_records', 0)}, Размер: {cache_info.get('total_size_mb', 0)} MB, Hit Rate: {cache_info.get('hit_rate', 0)}%")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🧹 Очистить устаревший"):
                st.success(f"Удалено {cache.clean_expired()} записей")
                st.rerun()
        with col2:
            if st.button("💥 Очистить весь кэш"):
                if cache.db_path.exists(): cache.db_path.unlink(missing_ok=True)
                st.success("Кэш полностью очищен"); st.rerun()
        
        st.markdown("---")
        st.info("Автор: [Telegram](https://t.me/i_gma)")

    keyword = st.text_input("🎯 Введите тему для анализа", placeholder="Например: n8n автоматизация, фотография для начинающих...", key="keyword_input")
    
   # Инициализация состояния для примеров
if 'example_keyword' not in st.session_state:
    st.session_state.example_keyword = None

# Обработка нажатия на кнопку примера
if st.session_state.example_keyword:
    keyword = st.session_state.example_keyword
    st.session_state.example_keyword = None  # Сбрасываем после использования
else:
    keyword = st.text_input("🎯 Введите тему для анализа", placeholder="Например: n8n автоматизация, фотография для начинающих...", key="keyword_input")

col1, col2, col3 = st.columns(3)
examples = ["python для начинающих", "монтаж видео", "инвестиции в акции"]
if col1.button(f"📌 {examples[0]}", use_container_width=True): 
    st.session_state.example_keyword = examples[0]
    st.rerun()
if col2.button(f"📌 {examples[1]}", use_container_width=True): 
    st.session_state.example_keyword = examples[1]
    st.rerun()
if col3.button(f"📌 {examples[2]}", use_container_width=True): 
    st.session_state.example_keyword = examples[2]
    st.rerun()
            
    if st.button("🚀 Глубокий анализ!", type="primary", use_container_width=True, disabled=not keyword):
        try:
            analyzer = YouTubeAnalyzer(youtube_api_key, cache)
            if not analyzer.test_connection():
                st.stop()
            
            with st.spinner("🌊 Анализирую YouTube..."):
                published_after_date = (datetime.now() - timedelta(days=days_limit)).isoformat("T") + "Z" if days_limit else None
                videos = analyzer.search_videos(keyword, max_results, published_after=published_after_date)
                
                if not videos:
                    st.warning(f"🔍 Не найдено видео по запросу '{keyword}'. Попробуйте изменить ключевое слово или период анализа.")
                    st.stop()
                
                comp_analysis, df = analyzer.analyze_competition(videos)
                trends_analyzer = AdvancedTrendsAnalyzer(cache)
                trends_data = trends_analyzer.analyze_keyword_trends(keyword)

            st.markdown(f"# 📊 Анализ ниши: **{keyword}**")
            
            cols = st.columns(5)
            cols[0].metric("📹 Видео", f"{len(df)}")
            cols[1].metric("🏆 Конкуренция", comp_analysis.get('competition_level', 'N/A').split()[0])
            cols[2].metric("👀 Сред. просмотры", safe_format_number(int(comp_analysis.get('avg_views', 0))))
            cols[3].metric("💬 Активность", f"{comp_analysis.get('engagement_rate', 0):.1f}%")
            cols[4].metric("📺 Каналов", comp_analysis.get('unique_channels', 0))

            tab1, tab2, tab3, tab4 = st.tabs(["📈 Популярность", "🏆 Топ видео", "🏷️ Популярные теги", "📊 Статистика"])

            with tab1:
                if trends_data and 'interest_df' in trends_data and not trends_data['interest_df'].empty:
                    fig = px.line(trends_data['interest_df'], x=trends_data['interest_df'].index, y=keyword, title=f'Популярность темы: "{keyword}" за 12 месяцев')
                    fig.update_layout(template='plotly_dark')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("📈 Данные Google Trends недоступны для этого запроса.")

            with tab2:
                st.markdown("### 🏆 Топ-10 видео по просмотрам")
                if not df.empty:
                    for _, video in df.nlargest(10, 'views').iterrows():
                        with st.container(border=True):
                            col1, col2 = st.columns([1, 4])
                            with col1:
                                st.image(video.get('thumbnail', ''))
                            with col2:
                                st.markdown(f"""
                                **[{video['title']}]({video['video_url']})**<br>
                                📺 **{video['channel']}** ({video['subscribers_formatted']} подписчиков)<br>
                                👀 {video['views_formatted']} • 👍 {video['likes_formatted']} • ⏱️ {video['duration_formatted']}
                                """, unsafe_allow_html=True)
            
            with tab3:
                st.markdown("### 🏷️ Самые популярные теги в нише")
                all_tags = [tag.lower() for v in videos if v.get('tags') for tag in v['tags']]
                if all_tags:
                    tag_counts = Counter(all_tags).most_common(25)
                    tags_df = pd.DataFrame(tag_counts, columns=['Тег', 'Частота'])
                    fig = px.bar(tags_df, x='Частота', y='Тег', orientation='h', title='Топ-25 тегов')
                    fig.update_layout(template='plotly_dark', yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Теги не найдены в проанализированных видео.")
            
            with tab4:
                st.markdown("### 🗂️ Все найденные видео")
                if not df.empty:
                    display_df = df[['title', 'channel', 'subscribers', 'views', 'likes', 'duration_formatted', 'short_indicator', 'video_url_markdown', 'published']]
                    st.dataframe(display_df.rename(columns={
                        'title':'Заголовок',
                        'channel':'Канал',
                        'subscribers': 'Подписчики',
                        'views':'Просмотры',
                        'likes':'Лайки',
                        'duration_formatted':'Длительность',
                        'short_indicator': 'Тип видео',
                        'video_url_markdown': 'URL',
                        'published':'Дата'
                    }), use_container_width=True, hide_index=True)

                    csv_data = df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button("📥 Скачать полные данные (CSV)", csv_data, f'youtube_analysis_{keyword.replace(" ", "_")}.csv', 'text/csv')

        except Exception as e:
            st.error(f"❌ Произошла непредвиденная ошибка: {str(e)}")
            logger.error(f"Критическая ошибка в main(): {e}", exc_info=True)

if __name__ == "__main__":
    main()
