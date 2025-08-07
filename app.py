# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
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
import openai
import numpy as np
from collections import Counter
import requests
import json
from dataclasses import dataclass
from urllib.parse import quote_plus
import unicodedata
import math

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
    .openai-result {
        background: linear-gradient(135deg, rgba(26, 142, 95, 0.1), rgba(46, 162, 115, 0.05));
        padding: 1.5rem;
        border-radius: 15px;
        border-left: 5px solid #1a8e5f;
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

# --- 2. УТИЛИТЫ И ВАЛИДАЦИЯ (УЛУЧШЕННЫЕ) ---

def validate_youtube_api_key(api_key: str) -> bool:
    """Улучшенная проверка формата YouTube API ключа"""
    if not api_key or not isinstance(api_key, str):
        return False
    
    api_key = api_key.strip()
    
    if api_key.startswith('AIza') and len(api_key) == 39:
        return True
    
    if len(api_key) > 30 and re.match(r'^[A-Za-z0-9_-]+$', api_key):
        return True
    
    return False

def validate_openai_api_key(api_key: str) -> bool:
    """Улучшенная проверка OpenAI API ключа"""
    if not api_key or not isinstance(api_key, str):
        return False
    
    api_key = api_key.strip()
    return api_key.startswith('sk-') and len(api_key) > 40

def validate_serpapi_key(api_key: str) -> bool:
    """Улучшенная проверка SerpAPI ключа"""
    if not api_key or not isinstance(api_key, str):
        return False
    
    api_key = api_key.strip()
    return len(api_key) > 30 and all(c.isalnum() for c in api_key)

def safe_format_number(num) -> str:
    """Безопасное форматирование чисел с обработкой ошибок"""
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

def extract_keywords_from_titles(titles: list, min_length=3, max_keywords=15) -> list:
    """Улучшенное извлечение ключевых слов"""
    if not titles:
        return []
    
    all_words = []
    stop_words = {
        'и', 'в', 'на', 'с', 'по', 'для', 'как', 'что', 'это', 'не', 'за', 'от', 'до',
        'из', 'к', 'о', 'у', 'же', 'еще', 'уже', 'или', 'так', 'но', 'а', 'их', 'его',
        'её', 'мой', 'твой', 'наш', 'ваш', 'который', 'которая', 'которое', 'если',
        'чтобы', 'когда', 'где', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to',
        'for', 'of', 'with', 'by', 'you', 'are', 'can', 'all', 'any', 'how', 'what',
        'when', 'where', 'why', 'this', 'that', 'have', 'had', 'will', 'been', 'were',
        'was', 'are', 'is', 'am', 'be', 'do', 'did', 'does', 'has', 'get', 'got'
    }
    
    try:
        for title in titles:
            if not title:
                continue
            
            title_clean = clean_text(str(title).lower())
            words = re.findall(r'\b[а-яё]{3,}|[a-z]{3,}\b', title_clean)
            filtered_words = [
                word for word in words
                if len(word) >= min_length and word not in stop_words
            ]
            all_words.extend(filtered_words)
        
        word_counts = Counter(all_words)
        return word_counts.most_common(max_keywords)
    
    except Exception as e:
        logger.error(f"Ошибка извлечения ключевых слов: {e}")
        return []

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
            'search': 3600*4,
            'channels': 3600*24*7,
            'trends': 3600*8,
            'openai': 3600*24,
            'serpapi': 3600*6
        }
        self.stats = {'hits': 0, 'misses': 0, 'errors': 0, 'size_mb': 0}
        self._update_cache_stats()

    def _init_sqlite(self):
        """Инициализация SQLite с улучшенной обработкой ошибок"""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                with self.lock:
                    conn = sqlite3.connect(
                        self.db_path,
                        check_same_thread=False,
                        timeout=10.0
                    )
                    cursor = conn.cursor()
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS cache (
                            key TEXT PRIMARY KEY,
                            value BLOB,
                            expires_at TIMESTAMP,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            access_count INTEGER DEFAULT 1,
                            category TEXT,
                            size_bytes INTEGER
                        )
                    ''')
                    
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_expires_at ON cache(expires_at)')
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_category ON cache(category)')
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_access_count ON cache(access_count)')
                    
                    cursor.execute('PRAGMA journal_mode=WAL')
                    cursor.execute('PRAGMA synchronous=NORMAL')
                    cursor.execute('PRAGMA cache_size=10000')
                    
                    conn.commit()
                    conn.close()
                    logger.info("Кэш база данных инициализирована успешно")
                    return
            
            except sqlite3.Error as e:
                logger.error(f"Ошибка инициализации кэша (попытка {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    time.sleep(1)
                    continue
                else:
                    st.error(f"Критическая ошибка инициализации кэша: {e}")

    def _update_cache_stats(self):
        """Обновление статистики кэша"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                
                cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
                result = cursor.fetchone()
                if result:
                    self.stats['size_mb'] = round(result[0] / (1024 * 1024), 2)
                
                conn.close()
        except Exception as e:
            logger.error(f"Ошибка обновления статистики кэша: {e}")

    def get(self, key: str):
        """Получение данных из кэша с улучшенной обработкой"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                
                cursor.execute(
                    "SELECT value, expires_at, access_count FROM cache WHERE key = ?",
                    (key,)
                )
                result = cursor.fetchone()
                
                if result:
                    value_blob, expires_at, access_count = result
                    
                    if datetime.fromisoformat(expires_at) > datetime.now():
                        cursor.execute(
                            "UPDATE cache SET access_count = ? WHERE key = ?",
                            (access_count + 1, key)
                        )
                        conn.commit()
                        conn.close()
                        
                        self.stats['hits'] += 1
                        return pickle.loads(value_blob)
                    else:
                        cursor.execute("DELETE FROM cache WHERE key = ?", (key,))
                        conn.commit()
                
                conn.close()
                self.stats['misses'] += 1
                return None
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Ошибка чтения из кэша: {e}")
            return None

    def set(self, key: str, value: any, category: str):
        """Сохранение в кэш с метаданными"""
        try:
            with self.lock:
                ttl = self.ttl_map.get(category, 3600)
                expires_at = datetime.now() + timedelta(seconds=ttl)
                value_blob = pickle.dumps(value)
                size_bytes = len(value_blob)
                
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT OR REPLACE INTO cache
                    (key, value, expires_at, category, size_bytes, created_at, access_count)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                """, (key, value_blob, expires_at.isoformat(), category, size_bytes, datetime.now().isoformat()))
                
                conn.commit()
                conn.close()
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Ошибка записи в кэш: {e}")

    def clean_expired(self) -> int:
        """Улучшенная очистка с дополнительной логикой"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                
                cursor.execute(
                    "DELETE FROM cache WHERE expires_at < ?",
                    (datetime.now().isoformat(),)
                )
                expired_count = cursor.rowcount
                
                cursor.execute("SELECT COUNT(*) FROM cache")
                total_records = cursor.fetchone()[0]
                
                if total_records > 1000:
                    cursor.execute("""
                        DELETE FROM cache WHERE key IN (
                            SELECT key FROM cache
                            ORDER BY access_count ASC, created_at ASC
                            LIMIT ?
                        )
                    """, (total_records // 10,))
                    
                    old_records = cursor.rowcount
                    logger.info(f"Удалено {old_records} старых записей из кэша")
                
                cursor.execute("VACUUM")
                
                conn.commit()
                conn.close()
                
                self._update_cache_stats()
                return expired_count
                
        except Exception as e:
            logger.error(f"Ошибка очистки кэша: {e}")
            return 0

    def get_cache_info(self) -> dict:
        """Получение детальной информации о кэше"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                
                cursor.execute("SELECT COUNT(*), SUM(size_bytes) FROM cache")
                count, total_size = cursor.fetchone()
                
                cursor.execute("""
                    SELECT category, COUNT(*), SUM(size_bytes), AVG(access_count)
                    FROM cache GROUP BY category
                """)
                categories = cursor.fetchall()
                
                conn.close()
                
                return {
                    'total_records': count or 0,
                    'total_size_mb': round((total_size or 0) / (1024 * 1024), 2),
                    'categories': {cat: {'count': cnt, 'size_mb': round((size or 0) / (1024 * 1024), 2), 'avg_access': round(avg or 0, 1)} for cat, cnt, size, avg in categories},
                    'hit_rate': round(self.stats['hits'] / max(self.stats['hits'] + self.stats['misses'], 1) * 100, 1)
                }
        except Exception as e:
            logger.error(f"Ошибка получения информации о кэше: {e}")
            return {'error': str(e)}

    def generate_key(self, *args) -> str:
        """Улучшенная генерация ключей для кэша."""
        try:
            clean_args = []
            for arg in args:
                if isinstance(arg, (dict, list)):
                    s = json.dumps(arg, sort_keys=True)
                elif arg is None:
                    s = 'None'
                else:
                    s = str(arg)
                clean_args.append(s.strip()[:200])
            
            combined = "|".join(clean_args)
            return hashlib.md5(combined.encode('utf-8')).hexdigest()
        except Exception as e:
            logger.error(f"Ошибка генерации ключа кэша: {e}")
            return hashlib.md5(f"error_{time.time()}_{str(args)}".encode()).hexdigest()

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
        """Улучшенное тестирование соединения"""
        try:
            self.youtube.i18nLanguages().list(part='snippet', hl='en').execute()
            logger.info("YouTube API соединение успешно протестировано.")
            return True
        except HttpError as e:
            logger.error(f"Тест соединения с YouTube API не удался: {e}")
            details = e.error_details[0] if e.error_details else {}
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
            self.quota_used += 1 
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
        """Улучшенное получение статистики каналов"""
        if not channel_ids:
            return {}
        
        unique_ids = list(set(filter(None, channel_ids)))
        if not unique_ids:
            return {}
            
        cache_key = self.cache.generate_key('channels', sorted(unique_ids))
        if cached_data := self.cache.get(cache_key):
            return cached_data
        
        channel_stats = {}
        try:
            for i in range(0, len(unique_ids), 50):
                chunk_ids = unique_ids[i:i+50]
                
                request = self.youtube.channels().list(
                    part="statistics,snippet,brandingSettings",
                    id=",".join(chunk_ids)
                )
                response = self._make_api_request(lambda: request)
                self.quota_used += 1

                for item in response.get('items', []):
                    stats = item.get('statistics', {})
                    snippet = item.get('snippet', {})
                    branding = item.get('brandingSettings', {}).get('channel', {})
                    
                    channel_stats[item['id']] = {
                        'subscribers': safe_int_conversion(stats.get('subscriberCount', 0)),
                        'total_views': safe_int_conversion(stats.get('viewCount', 0)),
                        'video_count': safe_int_conversion(stats.get('videoCount', 0)),
                        'title': clean_text(snippet.get('title', 'Неизвестно')),
                        'description': clean_text(snippet.get('description', ''))[:500],
                        'published_at': snippet.get('publishedAt', ''),
                        'country': snippet.get('country', ''),
                        'verified': 'verified' in str(snippet.get('thumbnails', {})),
                        'keywords': branding.get('keywords', '').split(',')[:10] if branding.get('keywords') else []
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
        """Улучшенный поиск видео с дополнительными метриками"""
        
        if not validate_keyword(keyword):
            st.error("❌ Некорректное ключевое слово")
            return None
        
        if max_results > 500:
            max_results = 500
            st.warning("⚠️ Максимальное количество видео ограничено до 500")
        
        cache_key = self.cache.generate_key('search_v5', keyword, max_results, published_after)
        if cached_data := self.cache.get(cache_key):
            st.toast("🚀 Результаты поиска загружены из кэша!", icon="⚡️")
            return cached_data
        
        try:
            video_snippets = []
            next_page_token = None
            search_params = {
                'q': clean_text(keyword),
                'part': 'snippet',
                'type': 'video',
                'order': 'relevance',
            }
            
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
                self.quota_used += 99 
                new_items = search_response.get('items', [])
                
                if not new_items:
                    logger.warning(f"Поиск не вернул новых результатов для '{keyword}' на этой странице.")
                    break
                    
                video_snippets.extend(new_items)
                fetched_count = len(video_snippets)
                next_page_token = search_response.get('nextPageToken')
                
                if not next_page_token:
                    break
                
                time.sleep(REQUEST_DELAY)

            progress_bar.progress(1.0)
            status_text.text(f"✅ Найдено {len(video_snippets)} видео. Собираем детали...")
            
            if not video_snippets:
                return []

            video_ids = [item['id']['videoId'] for item in video_snippets if 'videoId' in item.get('id', {})]
            channel_ids = list(set([item['snippet']['channelId'] for item in video_snippets]))

            status_text.text("📊 Получаем статистику каналов...")
            channel_stats = self.get_channel_stats(channel_ids)
            
            videos = []
            
            all_video_details = []
            for i in range(0, len(video_ids), 50):
                chunk_ids = video_ids[i:i+50]
                status_text.text(f"📊 Получаем детали видео ({i+len(chunk_ids)}/{len(video_ids)})...")
                
                request = self.youtube.videos().list(
                    part='statistics,contentDetails,snippet,topicDetails',
                    id=','.join(chunk_ids)
                )
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
                topic_details = details.get('topicDetails', {})
                
                duration = self._parse_duration(content_details.get('duration', 'PT0S'))
                channel_id = video_snippet.get('channelId')
                channel_info = channel_stats.get(channel_id, {})
                
                category_id = safe_int_conversion(video_snippet.get('categoryId', 0))
                tags = video_snippet.get('tags', [])[:20]
                
                video_data = {
                    'video_id': video_id,
                    'title': clean_text(video_snippet.get('title', '')),
                    'channel': clean_text(video_snippet.get('channelTitle', '')),
                    'channel_id': channel_id,
                    'subscribers': channel_info.get('subscribers', 0),
                    'subscribers_formatted': safe_format_number(channel_info.get('subscribers', 0)),
                    'channel_total_views': channel_info.get('total_views', 0),
                    'channel_video_count': channel_info.get('video_count', 0),
                    'channel_verified': channel_info.get('verified', False),
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
                    'tags': tags,
                    'description': clean_text(video_snippet.get('description', ''))[:1000],
                    'definition': content_details.get('definition', 'sd').upper(),
                    'category_id': category_id,
                    'language': video_snippet.get('defaultLanguage', 'ru'),
                    'topics': topic_details.get('topicCategories', [])[:5],
                    'thumbnail': snippet_item['snippet'].get('thumbnails', {}).get('medium', {}).get('url', ''),
                    'video_url': f"https://www.youtube.com/watch?v={video_id}"
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
        """Улучшенный парсинг продолжительности"""
        if not duration_str:
            return 0
            
        try:
            match = re.search(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
            if not match:
                return 0
                
            h, m, s = (safe_int_conversion(g) for g in match.groups())
            return h * 60 + m + s / 60
        except Exception as e:
            logger.error(f"Ошибка парсинга продолжительности '{duration_str}': {e}")
            return 0
    
    def _format_duration(self, duration_minutes: float) -> str:
        """Форматирование продолжительности в читаемый вид"""
        try:
            if duration_minutes is None:
                return "0:00"
            duration_minutes = float(duration_minutes)
            if duration_minutes < 1:
                return f"0:{int(duration_minutes * 60):02d}"
            
            total_seconds = int(duration_minutes * 60)
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            
            if hours > 0:
                return f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                return f"{minutes}:{seconds:02d}"
        except Exception:
            return "0:00"
    
    def analyze_competition(self, videos: list):
        """Расширенный анализ конкуренции с дополнительными метриками"""
        if not videos:
            return {}, pd.DataFrame()
        
        try:
            df = pd.DataFrame(videos)
            
            df['published'] = pd.to_datetime(df['published'], errors='coerce', utc=True).dt.tz_localize(None)
            df = df.dropna(subset=['published', 'views'])
            
            if df.empty:
                logger.warning("После фильтрации данных DataFrame пуст")
                return {}, pd.DataFrame()
            
            df['views'] = df['views'].apply(lambda x: max(safe_int_conversion(x, 1), 1))
            df['days_ago'] = (datetime.now() - df['published']).dt.days.fillna(0)
            df['engagement_rate'] = np.where(df['views'] > 0, ((df['likes'] + df['comments']) / df['views']) * 100, 0)
            df['views_per_subscriber'] = np.where(df['subscribers'] > 0, df['views'] / df['subscribers'], 0)
            
            view_quartiles = df['views'].quantile([0.25, 0.5, 0.75, 0.9])
            
            analysis = {
                'total_videos': len(df),
                'avg_views': safe_float_conversion(df['views'].mean()),
                'median_views': safe_float_conversion(df['views'].median()),
                'top_10_avg_views': safe_float_conversion(df.nlargest(min(10, len(df)), 'views')['views'].mean()),
                'top_25_percent_views': safe_float_conversion(view_quartiles[0.75]),
                'engagement_rate': safe_float_conversion(df['engagement_rate'].mean()),
                'videos_last_week': len(df[df['days_ago'] <= 7]),
                'videos_last_month': len(df[df['days_ago'] <= 30]),
                'shorts_percentage': safe_float_conversion(df['is_short'].mean() * 100),
                'avg_days_to_top_10': safe_float_conversion(df.nlargest(min(10, len(df)), 'views')['days_ago'].mean()),
                'unique_channels': df['channel'].nunique(),
                'avg_channel_subscribers': safe_float_conversion(df.drop_duplicates(subset=['channel_id'])['subscribers'].mean()),
                'avg_duration': safe_float_conversion(df[~df['is_short']]['duration'].mean()),
                'hd_percentage': safe_float_conversion((df['definition'] == 'HD').mean() * 100),
                'verified_channels_count': safe_int_conversion(df['channel_verified'].sum()),
                'avg_likes_per_view': safe_float_conversion(np.where(df['views'] > 0, (df['likes'] / df['views']), 0).mean() * 100),
                'avg_comments_per_view': safe_float_conversion(np.where(df['views'] > 0, (df['comments'] / df['views']), 0).mean() * 100)
            }

            score = 0
            
            if analysis['top_10_avg_views'] < 20000: score += 4
            elif analysis['top_10_avg_views'] < 50000: score += 3
            elif analysis['top_10_avg_views'] < 200000: score += 2
            elif analysis['top_10_avg_views'] < 500000: score += 1
            
            if analysis['videos_last_week'] < 2: score += 3
            elif analysis['videos_last_week'] < 5: score += 2
            elif analysis['videos_last_week'] < 15: score += 1
            
            if analysis['unique_channels'] < 15: score += 2
            elif analysis['unique_channels'] < 30: score += 1
            
            if analysis['engagement_rate'] < 1.5: score += 2
            elif analysis['engagement_rate'] < 3: score += 1
            
            competition_levels = {
                0: 'Экстремально высокая 🔴', 1: 'Очень высокая 🔴', 2: 'Очень высокая 🔴',
                3: 'Высокая 🟠', 4: 'Высокая 🟠', 5: 'Средняя 🟡', 6: 'Средняя 🟡',
                7: 'Низкая 🟢', 8: 'Низкая 🟢', 9: 'Очень низкая 🟢', 10: 'Минимальная 🟢'
            }
            
            analysis['competition_level'] = competition_levels.get(score, 'Экстремально высокая 🔴')
            analysis['competition_score'] = score
            analysis['opportunity_rating'] = min(score * 10, 100)
            
            return analysis, df
            
        except Exception as e:
            logger.error(f"Ошибка анализа конкуренции: {e}", exc_info=True)
            return {}, pd.DataFrame()

class AdvancedTrendsAnalyzer:
    def __init__(self, cache: CacheManager):
        self.cache = cache
        
    def _get_pytrends(self):
        """Создание экземпляра pytrends с обработкой ошибок"""
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
        if not pytrends:
            return None
            
        try:
            pytrends.build_payload([keyword], timeframe='today 12-m', geo='')
            interest_12m = pytrends.interest_over_time()
            
            if interest_12m.empty or keyword not in interest_12m.columns:
                 st.warning(f"Не удалось найти данные трендов для '{keyword}'.")
                 return None
            
            try:
                pytrends.build_payload([keyword], timeframe='today 5-y', geo='')
                interest_5y = pytrends.interest_over_time()
            except:
                interest_5y = pd.DataFrame()
            
            try:
                related_queries = pytrends.related_queries()
                rising_queries = related_queries.get(keyword, {}).get('rising', pd.DataFrame())
                top_queries = related_queries.get(keyword, {}).get('top', pd.DataFrame())
            except:
                rising_queries = pd.DataFrame()
                top_queries = pd.DataFrame()
            
            series = interest_12m[keyword]
            recent_avg = series.tail(4).mean()
            previous_avg = series.iloc[-8:-4].mean()
            overall_avg = series.mean()
            
            if recent_avg > previous_avg * 1.2: trend_direction = "Быстро растущий 🚀"
            elif recent_avg > previous_avg * 1.1: trend_direction = "Растущий 📈"
            elif recent_avg < previous_avg * 0.8: trend_direction = "Падающий 📉"
            elif recent_avg < previous_avg * 0.9: trend_direction = "Слабо падающий 📉"
            else: trend_direction = "Стабильный ➡️"
            
            monthly_avg = series.groupby(series.index.month).mean()
            peak_months = monthly_avg.nlargest(3).index.tolist()
            
            result = {
                'interest_df': interest_12m,
                'interest_5y_df': interest_5y,
                'trend_direction': trend_direction,
                'recent_avg': recent_avg,
                'overall_avg': overall_avg,
                'trend_strength': abs(recent_avg - previous_avg) / previous_avg if previous_avg > 0 else 0,
                'rising_queries': rising_queries,
                'top_queries': top_queries,
                'peak_months': peak_months,
                'current_interest': series.iloc[-1] if not series.empty else 0
            }
            
            self.cache.set(cache_key, result, 'trends')
            return result
            
        except Exception as e:
            st.warning(f"Не удалось получить данные из Google Trends: {str(e)}")
            return None

# --- 4. АНАЛИЗАТОР ТЕГОВ ---

@dataclass
class TagScore:
    keyword: str
    search_volume: int
    competition_score: int
    seo_score: int
    overall_score: int
    difficulty: str

class YouTubeTagAnalyzer:
    def __init__(self, serpapi_key: str = None, cache: CacheManager = None):
        self.serpapi_key = serpapi_key
        self.use_serpapi = bool(serpapi_key)
        self.cache = cache
        self.base_serpapi = "https://serpapi.com/search"
        
    def get_search_volume_serpapi(self, keyword: str) -> int:
        """Получает поисковый объем через SerpAPI"""
        if not self.use_serpapi:
            return self._estimate_search_volume_basic(keyword)
            
        cache_key = self.cache.generate_key('serpapi_volume', keyword) if self.cache else None
        if cache_key and self.cache:
            if cached_data := self.cache.get(cache_key): return cached_data
        
        try:
            params = {'api_key': self.serpapi_key, 'engine': 'youtube', 'search_query': keyword}
            response = requests.get(self.base_serpapi, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            search_info = data.get('search_information', {})
            if 'total_results' in search_info: volume = safe_int_conversion(search_info['total_results'])
            elif 'video_results' in data: volume = len(data['video_results']) * 150
            else: volume = self._estimate_search_volume_basic(keyword)

            if cache_key and self.cache: self.cache.set(cache_key, volume, 'serpapi')
            return volume
                
        except Exception as e:
            st.warning(f"Ошибка SerpAPI для '{keyword}': {e}")
        return self._estimate_search_volume_basic(keyword)
    
    def _estimate_search_volume_basic(self, keyword: str) -> int:
        """Базовая эстимация без внешних API"""
        word_count = len(keyword.split())
        char_count = len(keyword)
        base_volume = max(1000, 5000 - (word_count * 500) - (char_count * 10))
        popular_words = {'как', 'что', 'зачем', 'почему', 'обзор', 'урок', 'туториал', 'guide', 'tutorial', 'how', 'what', 'review', 'tips'}
        bonus = sum(300 for word in keyword.lower().split() if word in popular_words)
        return min(base_volume + bonus, 50000)
    
    def analyze_competition_serpapi(self, keyword: str) -> dict:
        """Анализ конкуренции через SerpAPI"""
        if not self.use_serpapi:
            return self._analyze_competition_basic(keyword)
            
        cache_key = self.cache.generate_key('serpapi_competition', keyword) if self.cache else None
        if cache_key and self.cache:
            if cached_data := self.cache.get(cache_key): return cached_data
        
        try:
            params = {'api_key': self.serpapi_key, 'engine': 'youtube', 'search_query': keyword}
            response = requests.get(self.base_serpapi, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'video_results' not in data: return self._analyze_competition_basic(keyword)
            
            videos = data['video_results'][:20]
            analysis = self._process_competition_data(videos, keyword)
            
            if cache_key and self.cache: self.cache.set(cache_key, analysis, 'serpapi')
            return analysis
            
        except Exception as e:
            st.warning(f"Ошибка анализа конкуренции: {e}")
            return self._analyze_competition_basic(keyword)
    
    def _analyze_competition_basic(self, keyword: str) -> dict:
        """Базовый анализ конкуренции (без внешних API)"""
        word_count = len(keyword.split())
        if word_count == 1: competition_level, optimized_ratio = "High", 0.8
        elif word_count == 2: competition_level, optimized_ratio = "Medium", 0.6
        else: competition_level, optimized_ratio = "Low", 0.4
        
        return {
            'total_videos': 20, 'optimized_titles': int(20 * optimized_ratio),
            'high_view_videos': max(1, int(20 * (1 - optimized_ratio))),
            'verified_channels': max(1, int(20 * 0.3)), 'avg_views': 15000 if word_count == 1 else 8000,
            'keyword_in_title': int(20 * optimized_ratio), 'recent_videos': 3,
            'competition_level': competition_level
        }
    
    def _process_competition_data(self, videos: list, keyword: str) -> dict:
        """Обработка данных конкуренции"""
        analysis = {'total_videos': len(videos), 'optimized_titles': 0, 'high_view_videos': 0, 'verified_channels': 0, 'avg_views': 0, 'keyword_in_title': 0, 'recent_videos': 0}
        total_views = 0
        keyword_lower = keyword.lower()
        
        for video in videos:
            title = video.get('title', '').lower()
            if keyword_lower in title:
                analysis['optimized_titles'] += 1
                analysis['keyword_in_title'] += 1
            
            views = self._extract_views(video.get('view_count_text', '0'))
            total_views += views
            if views > 100000: analysis['high_view_videos'] += 1
            if video.get('channel', {}).get('verified', False): analysis['verified_channels'] += 1
            if self._is_recent(video.get('published_date', '')): analysis['recent_videos'] += 1
        
        if analysis['total_videos'] > 0: analysis['avg_views'] = total_views // analysis['total_videos']
        return analysis
    
    def _extract_views(self, views_str: str) -> int:
        """Надежное извлечение количества просмотров из текстового формата."""
        if not views_str or not isinstance(views_str, str): return 0
        views_str = views_str.lower().replace(',', '').replace(' views', '').strip()
        num_part = re.match(r'[\d.]+', views_str)
        if not num_part: return 0
        num = float(num_part.group(0))
        if 'k' in views_str: return int(num * 1000)
        if 'm' in views_str: return int(num * 1000000)
        if 'b' in views_str: return int(num * 1000000000)
        return int(num)
    
    def _is_recent(self, date_str: str) -> bool:
        """Проверка свежести видео"""
        if not date_str: return False
        recent_indicators = ['day', 'days', 'week', 'weeks', 'hour', 'hours', 'час', 'день', 'недел']
        return any(indicator in date_str.lower() for indicator in recent_indicators)
    
    def calculate_scores(self, keyword: str, analysis: dict, search_volume: int) -> TagScore:
        """Рассчитывает все оценки для тега"""
        total = analysis.get('total_videos', 0)
        if total == 0:
            competition_score = 50
        else:
            optimized_ratio = analysis.get('optimized_titles', 0) / total
            high_views_ratio = analysis.get('high_view_videos', 0) / total
            verified_ratio = analysis.get('verified_channels', 0) / total
            avg_views_factor = min(analysis.get('avg_views', 0) / 500000, 1.0)
            competition_score = min(int((optimized_ratio * 0.3 + high_views_ratio * 0.25 + verified_ratio * 0.2 + avg_views_factor * 0.25) * 100), 100)
        
        if total > 0:
            keyword_optimization = analysis.get('keyword_in_title', 0) / total
            seo_score = max(int((1.0 - keyword_optimization) * 100), 10)
        else:
            seo_score = 50
        
        volume_score = min(math.log10(max(search_volume, 1)) * 20, 100)
        competition_inverted = 100 - competition_score
        overall_score = min(int(volume_score * 0.4 + competition_inverted * 0.35 + seo_score * 0.25), 100)
        
        if competition_score <= 20: difficulty = "Очень низкая 🟢"
        elif competition_score <= 40: difficulty = "Низкая 🟢"
        elif competition_score <= 60: difficulty = "Средняя 🟡"
        elif competition_score <= 80: difficulty = "Высокая 🟠"
        else: difficulty = "Очень высокая 🔴"
        
        return TagScore(keyword=keyword, search_volume=search_volume, competition_score=competition_score, seo_score=seo_score, overall_score=overall_score, difficulty=difficulty)
    
    def analyze_keyword(self, keyword: str) -> TagScore:
        """Полный анализ ключевого слова"""
        search_volume = self.get_search_volume_serpapi(keyword)
        competition_analysis = self.analyze_competition_serpapi(keyword)
        return self.calculate_scores(keyword, competition_analysis, search_volume)
    
    def analyze_multiple_keywords(self, keywords: list) -> list:
        """Анализ нескольких ключевых слов"""
        results = []
        if not keywords: return []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, keyword in enumerate(keywords):
            try:
                status_text.text(f"🏷️ Анализирую тег: {keyword} ({i+1}/{len(keywords)})")
                progress_bar.progress((i + 1) / len(keywords))
                result = self.analyze_keyword(keyword)
                results.append(result)
                if self.use_serpapi: time.sleep(0.7)
            except Exception as e:
                st.warning(f"Ошибка анализа '{keyword}': {e}")
                continue
        
        progress_bar.empty()
        status_text.empty()
        return sorted(results, key=lambda x: x.overall_score, reverse=True)

class ContentStrategist:
    def __init__(self, openai_key=None, openai_model=None):
        self.use_openai = bool(openai_key and openai_model)
        if self.use_openai:
            try:
                self.client = openai.OpenAI(api_key=openai_key)
                self.model = openai_model
            except Exception as e:
                st.error(f"Ошибка инициализации OpenAI: {e}")
                self.use_openai = False

    def get_strategy(self, keyword: str, comp_analysis: dict, trends_data: dict, df: pd.DataFrame, cache: CacheManager):
        if not comp_analysis: return "Недостаточно данных для генерации стратегии."
        cache_key = None
        if self.use_openai:
            cache_key = cache.generate_key('openai_v4', keyword, self.model, comp_analysis, trends_data)
            if cached_strategy := cache.get(cache_key):
                st.toast("🤖 AI Стратегия загружена из кэша!", icon="🧠")
                return cached_strategy
        
        if self.use_openai: strategy = self._get_ai_strategy(keyword, comp_analysis, trends_data, df)
        else: strategy = self._get_rule_based_strategy(keyword, comp_analysis, df)
        
        if self.use_openai and cache_key and "Ошибка" not in strategy:
            cache.set(cache_key, strategy, 'openai')
        return strategy

    def _get_rule_based_strategy(self, keyword: str, comp_analysis: dict, df: pd.DataFrame):
        """Улучшенная базовая стратегия без AI"""
        if not df.empty:
            titles = df['title'].tolist()
            popular_words = extract_keywords_from_titles(titles)
            top_words = [word for word, count in popular_words[:5]]
        else:
            top_words = []
        
        competition_level = comp_analysis.get('competition_level', 'Неизвестно')
        avg_views = comp_analysis.get('avg_views', 0)
        shorts_percentage = comp_analysis.get('shorts_percentage', 0)
        strategy_parts = []
        
        if 'низкая' in competition_level.lower(): verdict = "🎯 **ОТЛИЧНАЯ ВОЗМОЖНОСТЬ!** Низкая конкуренция дает хорошие шансы для роста."
        elif 'средняя' in competition_level.lower(): verdict = "⚡ **ХОРОШИЕ ПЕРСПЕКТИВЫ** с правильным подходом. Нужна качественная стратегия."
        else: verdict = "🔥 **ВЫСОКАЯ КОНКУРЕНЦИЯ** - требуется уникальный подход и высокое качество контента."
        strategy_parts.append(f"### 🎯 Вердикт\n{verdict}")
        
        insights = []
        if avg_views < 50000: insights.append("Средние просмотры невысокие - есть возможность выделиться качеством")
        if shorts_percentage > 50: insights.append("Много Shorts в нише - рассмотрите этот формат")
        if top_words: insights.append(f"Популярные слова в заголовках: {', '.join(top_words[:3])}")
        if insights: strategy_parts.append("### 🔍 Ключевые инсайты\n- " + "\n- ".join(insights))

        content_ideas = [
            f"**Полное руководство по {keyword}** - подробный туториал для начинающих",
            f"**Топ-5 ошибок в {keyword}** - разбор частых проблем",
            f"**{keyword}: до и после** - кейсы и результаты",
            f"**Как начать в {keyword} без опыта** - пошаговый план",
            f"**Секреты {keyword}, о которых не говорят** - инсайдерская информация"
        ]
        if shorts_percentage > 30:
            content_ideas.extend([f"**{keyword} за 60 секунд** - короткие обучающие видео", f"**Быстрые советы по {keyword}** - серия коротких роликов"])
        strategy_parts.append("### 💡 Идеи для контента\n- " + "\n- ".join(content_ideas))
        
        optimization_tips = [
            "🎨 **Яркие превью** - используйте контрастные цвета и четкий текст",
            "⏰ **Оптимальное время публикации** - тестируйте 18:00-21:00 по МСК",
            "🎯 **Цепляющие заголовки** - используйте числа, вопросы, интригу",
            "📝 **Подробные описания** - добавьте тайм-коды и полезные ссылки",
            "🏷️ **Правильные теги** - микс популярных и нишевых тегов"
        ]
        strategy_parts.append("### 🚀 Рекомендации по оптимизации\n- " + "\n- ".join(optimization_tips))
        return "\n\n".join(strategy_parts)
    
    def _get_ai_strategy(self, keyword: str, comp_analysis: dict, trends_data: dict, df: pd.DataFrame):
        st.toast("🤖 Отправляю данные на анализ в OpenAI...", icon="🧠")
        
        top_titles, top_channels = [], []
        if not df.empty:
            top_videos = df.nlargest(10, 'views')
            top_titles = top_videos['title'].tolist()
            top_channels = top_videos['channel'].value_counts().head(5).to_dict()
        
        trends_info = "Нет данных"
        if trends_data:
            trends_info = f"{trends_data.get('trend_direction', 'Неизвестно')}"
            if 'recent_avg' in trends_data: trends_info += f" (текущий интерес: {trends_data['recent_avg']:.0f})"
        
        prompt = f"""
        Ты — ведущий YouTube-стратег с опытом более 10 лет. Проведи глубокий анализ ниши и создай детальную стратегию продвижения.

        **АНАЛИЗИРУЕМАЯ ТЕМА:** "{keyword}"

        **ДАННЫЕ КОНКУРЕНТНОГО АНАЛИЗА:**
        - Общее количество видео: {comp_analysis.get('total_videos', 0)}
        - Уровень конкуренции: {comp_analysis.get('competition_level', 'Неизвестно')}
        - Средние просмотры: {int(comp_analysis.get('avg_views', 0)):,}
        - Медианные просмотры: {int(comp_analysis.get('median_views', 0)):,}
        - Просмотры топ-10: {int(comp_analysis.get('top_10_avg_views', 0)):,}
        - Вовлеченность: {comp_analysis.get('engagement_rate', 0):.2f}%
        - Процент Shorts: {comp_analysis.get('shorts_percentage', 0):.1f}%
        - Уникальных каналов: {comp_analysis.get('unique_channels', 0)}
        - Видео за неделю: {comp_analysis.get('videos_last_week', 0)}

        **ТРЕНДЫ GOOGLE:**
        {trends_info}

        **ТОП-5 ЗАГОЛОВКОВ КОНКУРЕНТОВ:**
        {chr(10).join(f"• {title}" for title in top_titles[:5])}

        **ВЕДУЩИЕ КАНАЛЫ:**
        {chr(10).join(f"• {channel}: {count} видео" for channel, count in list(top_channels.items())[:3])}

        **ЗАДАНИЕ:**
        Создай подробную стратегию в формате Markdown с следующими разделами:

        1. **🎯 Стратегический вердикт** - оценка перспектив ниши и главная рекомендация (2-3 предложения)
        2. **📊 Анализ возможностей** - детальный разбор сильных и слабых сторон ниши
        3. **🎬 Контент-стратегия** - 7 конкретных идей для видео с форматами (Заголовок, Краткое описание, Формат, Примерная длительность)
        4. **🚀 Тактика роста** - конкретные действия для первых 30 дней
        5. **💰 Монетизация** - 3 способа заработка + потенциальные риски
        6. **🏷️ SEO и оптимизация** - рекомендации по тегам, превью, времени публикации

        Будь конкретным, креативным и практичным. Фокусируйся на actionable советах.
        """

        try:
            response = self.client.chat.completions.create(model=self.model, messages=[{"role": "user", "content": prompt}], temperature=0.7, max_tokens=2000)
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Ошибка вызова OpenAI: {e}", exc_info=True)
            return f"❌ Ошибка при обращении к OpenAI: {e}"

# --- 5. ГЛАВНЫЙ ИНТЕРФЕЙС ---

def main():
    st.markdown('<h1 class="main-header">YouTube AI Strategist 🧠</h1>', unsafe_allow_html=True)
    
    with st.sidebar:
        st.header("⚙️ Настройки")
        st.subheader("🔑 YouTube API")
        youtube_api_key = st.text_input("YouTube API Key", type="password", help="Получите ключ в Google Cloud Console", key="youtube_api_key")
        
        if youtube_api_key:
            if validate_youtube_api_key(youtube_api_key): st.success("✅ YouTube API ключ выглядит корректно")
            else: st.warning("⚠️ Формат ключа может быть неверным")
        
        st.markdown("---")
        st.subheader("🤖 AI-стратег")
        use_openai = st.toggle("Включить AI-анализ (OpenAI)", value=True, key="use_openai")
        openai_api_key, openai_model = "", "gpt-4o-mini"
        if use_openai:
            openai_api_key = st.text_input("OpenAI API Key", type="password", help="Ключ для генерации AI-стратегий", key="openai_api_key")
            if openai_api_key:
                if validate_openai_api_key(openai_api_key): st.success("✅ OpenAI API ключ валиден")
                else: st.error("❌ Неверный OpenAI API ключ")
            openai_model = st.selectbox("Модель OpenAI", ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo"], index=1, help="gpt-4o-mini - быстрее и дешевле", key="openai_model")
        
        st.markdown("---")
        st.subheader("🏷️ Анализ тегов")
        use_serpapi = st.toggle("Включить продвинутый анализ тегов (SerpAPI)", value=False, key="use_serpapi")
        serpapi_key = ""
        if use_serpapi:
            serpapi_key = st.text_input("SerpAPI Key", type="password", help="Ключ для детального анализа тегов и конкурентов", key="serpapi_key")
            if serpapi_key:
                if validate_serpapi_key(serpapi_key): st.success("✅ SerpAPI ключ выглядит корректно")
                else: st.warning("⚠️ Формат ключа может быть неверным")
        
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
    
    col1, col2, col3 = st.columns(3)
    examples = ["python для начинающих", "монтаж видео", "инвестиции в акции"]
    if col1.button(f"📌 {examples[0]}", use_container_width=True): st.session_state.keyword_input = examples[0]; st.rerun()
    if col2.button(f"📌 {examples[1]}", use_container_width=True): st.session_state.keyword_input = examples[1]; st.rerun()
    if col3.button(f"📌 {examples[2]}", use_container_width=True): st.session_state.keyword_input = examples[2]; st.rerun()
            
    if st.button("🚀 Глубокий анализ!", type="primary", use_container_width=True, disabled=not keyword):
        try:
            analyzer = YouTubeAnalyzer(youtube_api_key, cache)
            if not analyzer.test_connection(): st.stop()
            
            spinner_text = "🌊 Анализирую YouTube..."
            if use_openai and openai_api_key and validate_openai_api_key(openai_api_key): spinner_text += " Привлекаю AI..."

            with st.spinner(spinner_text):
                published_after_date = (datetime.now() - timedelta(days=days_limit)).isoformat("T") + "Z" if days_limit else None
                videos = analyzer.search_videos(keyword, max_results, published_after=published_after_date)
                
                if not videos:
                    st.warning(f"🔍 Не найдено видео по запросу '{keyword}'. Попробуйте изменить ключевое слово или период анализа.")
                    st.stop()
                
                comp_analysis, df = analyzer.analyze_competition(videos)
                trends_analyzer = AdvancedTrendsAnalyzer(cache)
                trends_data = trends_analyzer.analyze_keyword_trends(keyword)
                
                strategist = ContentStrategist(openai_api_key if use_openai and validate_openai_api_key(openai_api_key) else None, openai_model if use_openai else None)
                strategy_output = strategist.get_strategy(keyword, comp_analysis, trends_data, df, cache)

            st.markdown(f"# 📊 Анализ ниши: **{keyword}**")
            
            cols = st.columns(5)
            cols[0].metric("📹 Видео", f"{len(df)}")
            cols[1].metric("🏆 Конкуренция", comp_analysis.get('competition_level', 'N/A').split()[0])
            cols[2].metric("👀 Сред. просмотры", safe_format_number(int(comp_analysis.get('avg_views', 0))))
            cols[3].metric("💬 Активность", f"{comp_analysis.get('engagement_rate', 0):.1f}%")
            cols[4].metric("📺 Каналов", comp_analysis.get('unique_channels', 0))

            tab1, tab2, tab3, tab4, tab5 = st.tabs(["🎯 AI Советы", "🏷️ Анализ тегов", "📈 Популярность", "🏆 Топ видео", "📊 Статистика"])

            with tab1:
                css_class = "openai-result" if strategist.use_openai else "custom-container"
                st.markdown(f'<div class="{css_class}">{strategy_output}</div>', unsafe_allow_html=True)
                
            with tab2:
                all_tags = [tag.lower() for v in videos if v.get('tags') for tag in v['tags']]
                if all_tags:
                    tag_popularity = Counter(all_tags)
                    st.markdown("#### Выберите теги для детального анализа")
                    selected_tags = st.multiselect("Популярные теги:", [t for t, c in tag_popularity.most_common(20)], default=[t for t,c in tag_popularity.most_common(5)])
                    custom_tags = st.text_input("Добавьте свои теги через запятую:")
                    if custom_tags: selected_tags.extend([t.strip().lower() for t in custom_tags.split(',') if t.strip()])
                    
                    if st.button("🔍 Анализировать выбранные теги", type="secondary"):
                        unique_tags_to_analyze = list(set(selected_tags))[:20]
                        tag_analyzer = YouTubeTagAnalyzer(serpapi_key if use_serpapi and validate_serpapi_key(serpapi_key) else None, cache)
                        with st.spinner(f"🏷️ Анализирую {len(unique_tags_to_analyze)} тегов..."):
                            tag_results = tag_analyzer.analyze_multiple_keywords(unique_tags_to_analyze)
                        if tag_results:
                            results_df = pd.DataFrame([vars(r) for r in tag_results])
                            st.dataframe(results_df[['keyword', 'search_volume', 'competition_score', 'seo_score', 'overall_score', 'difficulty']].rename(columns={'keyword':'Тег','search_volume':'Объем','competition_score':'Конкуренция','seo_score':'SEO','overall_score':'Оценка','difficulty':'Сложность'}), hide_index=True)
                else:
                    st.warning("Теги не найдены в проанализированных видео.")

            with tab3:
                if trends_data and 'interest_df' in trends_data and not trends_data['interest_df'].empty:
                    fig = px.line(trends_data['interest_df'], x=trends_data['interest_df'].index, y=keyword, title=f'Популярность темы: "{keyword}"')
                    fig.update_layout(template='plotly_dark')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("📈 Данные Google Trends недоступны.")

            with tab4:
                st.markdown("### 🏆 Топ видео по просмотрам")
                if not df.empty:
                    for _, video in df.nlargest(10, 'views').iterrows():
                        with st.container(border=True):
                            col1, col2 = st.columns([1, 4])
                            with col1: st.image(video.get('thumbnail', ''))
                            # ИЗМЕНЕНО: Добавлена информация о подписчиках
                            with col2: st.markdown(f"""
                                **[{video['title']}]({video['video_url']})**<br>
                                📺 **{video['channel']}** ({video['subscribers_formatted']} подписчиков)<br>
                                👀 {video['views_formatted']} • 👍 {video['likes_formatted']} • ⏱️ {video['duration_formatted']}
                                """, unsafe_allow_html=True)
            
            with tab5:
                st.markdown("### 📊 Детальная статистика по найденным видео")
                if not df.empty:
                    # ИЗМЕНЕНО: Добавлен столбец 'subscribers' и переименован
                    display_df = df[['title', 'channel', 'subscribers', 'views', 'likes', 'duration_formatted', 'published']]
                    st.dataframe(display_df.rename(columns={
                        'title':'Заголовок',
                        'channel':'Канал',
                        'subscribers': 'Подписчики',
                        'views':'Просмотры',
                        'likes':'Лайки',
                        'duration_formatted':'Длительность',
                        'published':'Дата'
                    }), use_container_width=True, hide_index=True)

                    csv_data = df.to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Скачать полные данные (CSV)", csv_data, f'youtube_analysis_{keyword.replace(" ", "_")}.csv', 'text/csv')

        except Exception as e:
            st.error(f"❌ Произошла непредвиденная ошибка: {str(e)}")
            logger.error(f"Критическая ошибка в main(): {e}", exc_info=True)

if __name__ == "__main__":
    main()