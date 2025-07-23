# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
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
from pytrends.request import TrendReq
import openai
import numpy as np
from collections import Counter
import requests
import json
from dataclasses import dataclass
from urllib.parse import quote_plus


# --- 1. КОНФИГУРАЦИЯ СТРАНИЦЫ И СТИЛИ ---
st.set_page_config(
    page_title="YouTube AI Strategist 🧠",
    page_icon="🚀",
    layout="wide"
)
warnings.filterwarnings('ignore')

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

# --- 2. УТИЛИТЫ И ВАЛИДАЦИЯ ---

def validate_youtube_api_key(api_key: str) -> bool:
    """Проверка формата YouTube API ключа"""
    if not api_key:
        return False
    
    # YouTube API ключи обычно начинаются с AIza и имеют длину 39 символов
    if api_key.startswith('AIza') and len(api_key) == 39:
        return True
    
    # Более мягкая проверка - если ключ длинный и содержит нужные символы
    if len(api_key) > 30 and all(c.isalnum() or c in '-_' for c in api_key):
        return True
    
    return False

def validate_openai_api_key(api_key: str) -> bool:
    """Проверка валидности OpenAI API ключа"""
    if not api_key:
        return False
    
    # OpenAI ключи начинаются с sk-
    return api_key.startswith('sk-')

def format_number(num):
    """Форматирование чисел для читаемости"""
    if num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    return str(num)

def extract_keywords_from_titles(titles: list) -> list:
    """Извлечение ключевых слов из заголовков"""
    all_words = []
    stop_words = {'и', 'в', 'на', 'с', 'по', 'для', 'как', 'что', 'это', 'не', 'за', 'от', 'до', 'из', 'к', 'о', 'у', 'же', 'еще', 'уже', 'или', 'так', 'но', 'а', 'их', 'его', 'её', 'мой', 'твой', 'наш', 'ваш', 'который', 'которая', 'которое', 'если', 'чтобы', 'когда', 'где', 'why', 'how', 'what', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
    
    for title in titles:
        words = re.findall(r'\b[а-яё]{3,}|[a-z]{3,}\b', title.lower())
        words = [word for word in words if word not in stop_words]
        all_words.extend(words)
    
    word_counts = Counter(all_words)
    return word_counts.most_common(10)

# --- 3. КЛАССЫ-АНАЛИЗАТОРЫ ---

class CacheManager:
    def __init__(self, cache_dir: str = "data/cache"):
        self.db_path = Path(cache_dir) / "youtube_ai_cache.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self._init_sqlite()
        self.ttl_map = {
            'search': 3600*4,       # 4 часа
            'channels': 3600*24*7,  # 7 дней
            'trends': 3600*8,       # 8 часов
            'openai': 3600*24       # 1 день
        }
        self.stats = {'hits': 0, 'misses': 0}

    def _init_sqlite(self):
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY, 
                        value BLOB, 
                        expires_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                # Создаем индекс для быстрого поиска по времени истечения
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_expires_at ON cache(expires_at)')
                conn.commit()
                conn.close()
            except Exception as e:
                st.error(f"Ошибка инициализации кэша: {e}")

    def get(self, key: str):
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute("SELECT value, expires_at FROM cache WHERE key = ?", (key,))
                result = cursor.fetchone()
                conn.close()
                
                if result and datetime.fromisoformat(result[1]) > datetime.now():
                    self.stats['hits'] += 1
                    return pickle.loads(result[0])
                elif result:
                    self.delete(key)
                
                self.stats['misses'] += 1
                return None
            except Exception as e:
                st.warning(f"Ошибка чтения кэша: {e}")
                self.stats['misses'] += 1
                return None

    def set(self, key: str, value: any, category: str):
        with self.lock:
            try:
                ttl = self.ttl_map.get(category, 3600)
                expires_at = datetime.now() + timedelta(seconds=ttl)
                value_blob = pickle.dumps(value)
                
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO cache (key, value, expires_at) VALUES (?, ?, ?)",
                    (key, value_blob, expires_at.isoformat())
                )
                conn.commit()
                conn.close()
            except Exception as e:
                st.warning(f"Ошибка записи в кэш: {e}")

    def delete(self, key: str):
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM cache WHERE key = ?", (key,))
                conn.commit()
                conn.close()
            except Exception as e:
                st.warning(f"Ошибка удаления из кэша: {e}")

    def clean_expired(self):
        """Очистка истекших записей"""
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM cache WHERE expires_at < ?", (datetime.now().isoformat(),))
                deleted_count = cursor.rowcount
                conn.commit()
                conn.close()
                return deleted_count
            except Exception as e:
                st.warning(f"Ошибка очистки кэша: {e}")
                return 0

    def generate_key(self, *args) -> str:
        return hashlib.md5("".join(map(str, args)).encode('utf-8')).hexdigest()

class YouTubeAnalyzer:
    def __init__(self, api_key: str, cache: CacheManager):
        try:
            self.youtube = build('youtube', 'v3', developerKey=api_key)
            self.cache = cache
            self.api_key = api_key
        except Exception as e:
            st.error(f"Ошибка инициализации YouTube API: {e}")
            raise

    def test_connection(self) -> bool:
        """Мягкое тестирование соединения с YouTube API"""
        try:
            # Просто проверяем, что объект создался без ошибок
            return hasattr(self.youtube, 'search')
        except Exception as e:
            st.warning(f"Предупреждение при инициализации API: {e}")
            return True  # Возвращаем True, чтобы позволить продолжить

    def get_channel_stats(self, channel_ids: list):
        """Получает статистику каналов (включая подписчиков) пачками по 50."""
        if not channel_ids:
            return {}
            
        cache_key = self.cache.generate_key('channels', sorted(channel_ids))
        if cached_data := self.cache.get(cache_key):
            return cached_data
        
        channel_stats = {}
        try:
            for i in range(0, len(channel_ids), 50):
                chunk_ids = channel_ids[i:i+50]
                request = self.youtube.channels().list(
                    part="statistics,snippet", 
                    id=",".join(chunk_ids)
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    stats = item.get('statistics', {})
                    snippet = item.get('snippet', {})
                    
                    channel_stats[item['id']] = {
                        'subscribers': int(stats.get('subscriberCount', 0)),
                        'total_views': int(stats.get('viewCount', 0)),
                        'video_count': int(stats.get('videoCount', 0)),
                        'title': snippet.get('title', 'Неизвестно'),
                        'description': snippet.get('description', ''),
                        'published_at': snippet.get('publishedAt', '')
                    }
                
                # Добавляем небольшую задержку между запросами
                time.sleep(0.1)
            
            self.cache.set(cache_key, channel_stats, 'channels')
            return channel_stats
            
        except Exception as e:
            st.warning(f"Не удалось получить данные о каналах: {e}")
            return {}

    def search_videos(self, keyword: str, max_results: int = 100, published_after=None):
        cache_key = self.cache.generate_key('search', keyword, max_results, published_after)
        if cached_data := self.cache.get(cache_key):
            st.toast("🚀 Результаты поиска загружены из кэша!", icon="⚡️")
            return cached_data
        
        try:
            video_snippets = []
            next_page_token = None
            search_params = {
                'q': keyword,
                'part': 'snippet',
                'type': 'video',
                'order': 'relevance',
                'regionCode': 'RU'
            }
            
            if published_after:
                search_params['publishedAfter'] = published_after

            progress_bar = st.progress(0)
            status_text = st.empty()

            while len(video_snippets) < max_results:
                search_params['maxResults'] = min(50, max_results - len(video_snippets))
                if next_page_token:
                    search_params['pageToken'] = next_page_token
                
                status_text.text(f"🔍 Найдено видео: {len(video_snippets)}/{max_results}")
                progress_bar.progress(len(video_snippets) / max_results)
                
                search_response = self.youtube.search().list(**search_params).execute()
                new_items = search_response.get('items', [])
                
                if not new_items:
                    break
                    
                video_snippets.extend(new_items)
                next_page_token = search_response.get('nextPageToken')
                
                if not next_page_token:
                    break
                
                # Небольшая задержка между запросами
                time.sleep(0.1)

            progress_bar.progress(1.0)
            status_text.text(f"✅ Найдено {len(video_snippets)} видео")

            if not video_snippets:
                return []

            video_ids = [item['id']['videoId'] for item in video_snippets]
            channel_ids = list(set([item['snippet']['channelId'] for item in video_snippets]))

            # Получаем статистику видео и каналов
            status_text.text("📊 Получаем статистику...")
            channel_stats = self.get_channel_stats(channel_ids)
            
            videos = []
            for i in range(0, len(video_ids), 50):
                chunk_ids = video_ids[i:i+50]
                stats_response = self.youtube.videos().list(
                    part='statistics,contentDetails,snippet', 
                    id=','.join(chunk_ids)
                ).execute()
                
                video_details_map = {item['id']: item for item in stats_response.get('items', [])}
                
                for snippet in video_snippets[i:i+50]:
                    video_id = snippet['id']['videoId']
                    details = video_details_map.get(video_id)
                    
                    if not details:
                        continue
                    
                    stats = details.get('statistics', {})
                    content_details = details.get('contentDetails', {})
                    video_snippet = details.get('snippet', {})
                    
                    duration = self._parse_duration(content_details.get('duration', 'PT0S'))
                    channel_id = snippet['snippet']['channelId']
                    channel_info = channel_stats.get(channel_id, {})
                    
                    # Извлекаем теги
                    tags = video_snippet.get('tags', [])
                    
                    video_data = {
                        'video_id': video_id,
                        'title': snippet['snippet']['title'],
                        'channel': snippet['snippet']['channelTitle'],
                        'channel_id': channel_id,
                        'subscribers': channel_info.get('subscribers', 0),
                        'channel_total_views': channel_info.get('total_views', 0),
                        'channel_video_count': channel_info.get('video_count', 0),
                        'published': snippet['snippet']['publishedAt'],
                        'views': int(stats.get('viewCount', 0)),
                        'likes': int(stats.get('likeCount', 0)),
                        'comments': int(stats.get('commentCount', 0)),
                        'duration': duration,
                        'is_short': duration <= 1.05,
                        'tags': tags,
                        'description': video_snippet.get('description', ''),
                        'definition': content_details.get('definition', 'sd').upper(),
                        'category_id': video_snippet.get('categoryId', ''),
                        'language': video_snippet.get('defaultLanguage', 'ru')
                    }
                    videos.append(video_data)
                
                time.sleep(0.1)
            
            progress_bar.empty()
            status_text.empty()
            
            self.cache.set(cache_key, videos, 'search')
            return videos
            
        except Exception as e:
            st.error(f"Ошибка при поиске видео: {e}")
            return None

    def _parse_duration(self, duration_str: str) -> float:
        """Парсинг продолжительности видео в минутах"""
        if not duration_str:
            return 0
            
        match = re.search(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
        if not match:
            return 0
            
        h, m, s = (int(g or 0) for g in match.groups())
        return h * 60 + m + s / 60
    
    def analyze_competition(self, videos: list):
        """Расширенный анализ конкуренции"""
        if not videos:
            return {}, pd.DataFrame()
            
        df = pd.DataFrame(videos)
        df['published'] = pd.to_datetime(df['published'], errors='coerce').dt.tz_localize(None)
        df['views'] = df['views'].replace(0, 1)
        df['days_ago'] = (datetime.now() - df['published']).dt.days
        df['engagement_rate'] = ((df['likes'] + df['comments']) / df['views']) * 100
        df['views_per_subscriber'] = df['views'] / (df['subscribers'] + 1)

        # Квартили для анализа
        view_quartiles = df['views'].quantile([0.25, 0.5, 0.75])
        
        analysis = {
            'total_videos': len(df),
            'avg_views': df['views'].mean(),
            'median_views': df['views'].median(),
            'top_10_avg_views': df.nlargest(10, 'views')['views'].mean(),
            'top_25_percent_views': view_quartiles[0.75],
            'engagement_rate': df['engagement_rate'].mean(),
            'videos_last_week': len(df[df['days_ago'] <= 7]),
            'videos_last_month': len(df[df['days_ago'] <= 30]),
            'shorts_percentage': df['is_short'].mean() * 100 if not df.empty else 0,
            'avg_days_to_top_10': df.nlargest(10, 'views')['days_ago'].mean() if not df.empty else 0,
            'unique_channels': df['channel'].nunique(),
            'avg_channel_subscribers': df['subscribers'].mean(),
            'avg_duration': df[~df['is_short']]['duration'].mean(),
            'hd_percentage': (df['definition'] == 'HD').mean() * 100 if 'definition' in df.columns else 0
        }

        # Определение уровня конкуренции (улучшенная формула)
        score = 0
        
        # Анализ просмотров
        if analysis['top_10_avg_views'] < 30000:
            score += 3
        elif analysis['top_10_avg_views'] < 100000:
            score += 2
        elif analysis['top_10_avg_views'] < 500000:
            score += 1
        
        # Анализ активности
        if analysis['videos_last_week'] < 3:
            score += 2
        elif analysis['videos_last_week'] < 10:
            score += 1
        
        # Анализ уникальности каналов
        if analysis['unique_channels'] < 20:
            score += 1
        
        # Анализ вовлеченности
        if analysis['engagement_rate'] < 2:
            score += 1
        
        competition_levels = {
            0: 'Очень высокая 🔴',
            1: 'Очень высокая 🔴',
            2: 'Высокая 🟠',
            3: 'Высокая 🟠',
            4: 'Средняя 🟡',
            5: 'Средняя 🟡',
            6: 'Низкая 🟢',
            7: 'Очень низкая 🟢'
        }
        
        analysis['competition_level'] = competition_levels.get(score, 'Очень высокая 🔴')
        analysis['competition_score'] = score
        
        return analysis, df

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
            # Основной тренд за 12 месяцев
            pytrends.build_payload([keyword], timeframe='today 12-m', geo='RU')
            interest_12m = pytrends.interest_over_time()
            
            if interest_12m.empty:
                return None
            
            # Дополнительный анализ за 5 лет для долгосрочного тренда
            try:
                pytrends.build_payload([keyword], timeframe='today 5-y', geo='RU')
                interest_5y = pytrends.interest_over_time()
            except:
                interest_5y = pd.DataFrame()
            
            # Анализ связанных запросов
            try:
                related_queries = pytrends.related_queries()
                rising_queries = related_queries.get(keyword, {}).get('rising', pd.DataFrame())
                top_queries = related_queries.get(keyword, {}).get('top', pd.DataFrame())
            except:
                rising_queries = pd.DataFrame()
                top_queries = pd.DataFrame()
            
            # Анализ тренда
            series = interest_12m[keyword]
            recent_avg = series.tail(4).mean()  # Последние 4 недели
            previous_avg = series.iloc[-8:-4].mean()  # Предыдущие 4 недели
            overall_avg = series.mean()
            
            # Определение направления тренда
            if recent_avg > previous_avg * 1.2:
                trend_direction = "Быстро растущий 🚀"
            elif recent_avg > previous_avg * 1.1:
                trend_direction = "Растущий 📈"
            elif recent_avg < previous_avg * 0.8:
                trend_direction = "Падающий 📉"
            elif recent_avg < previous_avg * 0.9:
                trend_direction = "Слабо падающий 📉"
            else:
                trend_direction = "Стабильный ➡️"
            
            # Сезонность (простой анализ)
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

class ContentStrategist:
    def __init__(self, openai_key=None, openai_model=None):
        self.use_openai = bool(openai_key and openai_model)
        if self.use_openai:
            self.client = openai.OpenAI(api_key=openai_key)
            self.model = openai_model

    def get_strategy(self, keyword: str, comp_analysis: dict, trends_data: dict, df: pd.DataFrame, cache: CacheManager):
        cache_key = None
        if self.use_openai:
            cache_key = cache.generate_key('openai_v3', keyword, self.model, str(comp_analysis)[:100])
            if cached_strategy := cache.get(cache_key):
                st.toast("🤖 AI Стратегия загружена из кэша!", icon="🧠")
                return cached_strategy
        
        if self.use_openai:
            strategy = self._get_ai_strategy(keyword, comp_analysis, trends_data, df)
        else:
            strategy = self._get_rule_based_strategy(keyword, comp_analysis, df)
        
        if self.use_openai and cache_key and "Ошибка" not in strategy:
            cache.set(cache_key, strategy, 'openai')
        
        return strategy

    def _get_rule_based_strategy(self, keyword: str, comp_analysis: dict, df: pd.DataFrame):
        """Улучшенная базовая стратегия без AI"""
        
        # Анализ популярных слов в заголовках
        if not df.empty:
            titles = df['title'].tolist()
            popular_words = extract_keywords_from_titles(titles)
            top_words = [word for word, count in popular_words[:5]]
        else:
            top_words = []
        
        # Анализ конкуренции
        competition_level = comp_analysis.get('competition_level', 'Неизвестно')
        avg_views = comp_analysis.get('avg_views', 0)
        shorts_percentage = comp_analysis.get('shorts_percentage', 0)
        
        strategy_parts = []
        
        # Вердикт
        if 'низкая' in competition_level.lower():
            verdict = "🎯 **ОТЛИЧНАЯ ВОЗМОЖНОСТЬ!** Низкая конкуренция дает хорошие шансы для роста."
        elif 'средняя' in competition_level.lower():
            verdict = "⚡ **ХОРОШИЕ ПЕРСПЕКТИВЫ** с правильным подходом. Нужна качественная стратегия."
        else:
            verdict = "🔥 **ВЫСОКАЯ КОНКУРЕНЦИЯ** - требуется уникальный подход и высокое качество контента."
        
        strategy_parts.append(f"### 🎯 Вердикт\n{verdict}")
        
        # Ключевые инсайты
        insights = []
        if avg_views < 50000:
            insights.append("💡 Средние просмотры невысокие - есть возможность выделиться качеством")
        if shorts_percentage > 50:
            insights.append("📱 Много Shorts в нише - рассмотрите этот формат")
        if top_words:
            insights.append(f"🔤 Популярные слова в заголовках: {', '.join(top_words[:3])}")
        
        strategy_parts.append("### 🔍 Ключевые инсайты\n" + "\n".join(insights))
        
        # Идеи контента
        content_ideas = [
            f"**Полное руководство по {keyword}** - подробный туториал для начинающих",
            f"**Топ-5 ошибок в {keyword}** - разбор частых проблем",
            f"**{keyword}: до и после** - кейсы и результаты",
            f"**Как начать в {keyword} без опыта** - пошаговый план",
            f"**Секреты {keyword}, о которых не говорят** - инсайдерская информация"
        ]
        
        if shorts_percentage > 30:
            content_ideas.extend([
                f"**{keyword} за 60 секунд** - короткие обучающие видео",
                f"**Быстрые советы по {keyword}** - серия коротких роликов"
            ])
        
        strategy_parts.append("### 💡 Идеи для контента\n" + "\n".join(content_ideas))
        
        # Рекомендации по оптимизации
        optimization_tips = [
            "🎨 **Яркие превью** - используйте контрастные цвета и четкий текст",
            "⏰ **Оптимальное время публикации** - тестируйте 18:00-21:00 по МСК",
            "🎯 **Цепляющие заголовки** - используйте числа, вопросы, интригу",
            "📝 **Подробные описания** - добавьте тайм-коды и полезные ссылки",
            "🏷️ **Правильные теги** - микс популярных и нишевых тегов"
        ]
        
        strategy_parts.append("### 🚀 Рекомендации по оптимизации\n" + "\n".join(optimization_tips))
        
        return "\n\n".join(strategy_parts)
    
    def _get_ai_strategy(self, keyword: str, comp_analysis: dict, trends_data: dict, df: pd.DataFrame):
        st.toast("🤖 Отправляю данные на анализ в OpenAI...", icon="🧠")
        
        # Подготовка данных для промпта
        top_titles = []
        top_channels = []
        if not df.empty:
            top_videos = df.nlargest(10, 'views')
            top_titles = top_videos['title'].tolist()
            top_channels = top_videos['channel'].value_counts().head(5).to_dict()
        
        trends_info = "Нет данных"
        if trends_data:
            trends_info = f"{trends_data.get('trend_direction', 'Неизвестно')}"
            if 'recent_avg' in trends_data:
                trends_info += f" (текущий интерес: {trends_data['recent_avg']:.0f})"
        
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

        3. **🎬 Контент-стратегия** - 7 конкретных идей для видео с форматами:
           - Заголовок
           - Краткое описание (1-2 предложения)
           - Формат (туториал/обзор/кейс/и т.д.)
           - Примерная длительность

        4. **🚀 Тактика роста** - конкретные действия для первых 30 дней

        5. **💰 Монетизация** - 3 способа заработка + потенциальные риски

        6. **🏷️ SEO и оптимизация** - рекомендации по тегам, превью, времени публикации

        Будь конкретным, креативным и практичным. Фокусируйся на actionable советах.
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=2000
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"❌ Ошибка при обращении к OpenAI: {e}"

# --- 4. АНАЛИЗАТОР ТЕГОВ ---

@dataclass
class TagScore:
    keyword: str
    search_volume: int
    competition_score: int  # 0-100 (0 = low, 100 = high)
    seo_score: int         # 0-100 
    overall_score: int     # 0-100
    difficulty: str        # "Very Low", "Low", "Medium", "High", "Very High"

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
            if cached_data := self.cache.get(cache_key):
                return cached_data
        
        try:
            params = {
                'api_key': self.serpapi_key,
                'engine': 'youtube',
                'search_query': keyword,
                'gl': 'us',
                'hl': 'en'
            }
            
            response = requests.get(self.base_serpapi, params=params, timeout=10)
            data = response.json()
            
            # Эстимация на основе количества результатов
            if 'video_results' in data:
                result_count = len(data['video_results'])
                volume = min(result_count * 150, 100000)  # Улучшенная формула
                
                if cache_key and self.cache:
                    self.cache.set(cache_key, volume, 'search')
                return volume
                
        except Exception as e:
            st.warning(f"Ошибка SerpAPI для '{keyword}': {e}")
            
        return self._estimate_search_volume_basic(keyword)
    
    def _estimate_search_volume_basic(self, keyword: str) -> int:
        """Базовая эстимация без внешних API"""
        # Простая эстимация на основе длины и популярности слов
        word_count = len(keyword.split())
        char_count = len(keyword)
        
        # Базовая формула
        base_volume = max(1000, 5000 - (word_count * 500) - (char_count * 10))
        
        # Бонусы за популярные слова
        popular_words = {
            'как', 'что', 'зачем', 'почему', 'обзор', 'урок', 'туториал',
            'guide', 'tutorial', 'how', 'what', 'review', 'tips'
        }
        
        bonus = sum(300 for word in keyword.lower().split() if word in popular_words)
        
        return min(base_volume + bonus, 50000)
    
    def analyze_competition_serpapi(self, keyword: str) -> dict:
        """Анализ конкуренции через SerpAPI"""
        if not self.use_serpapi:
            return self._analyze_competition_basic(keyword)
            
        cache_key = self.cache.generate_key('serpapi_competition', keyword) if self.cache else None
        if cache_key and self.cache:
            if cached_data := self.cache.get(cache_key):
                return cached_data
        
        try:
            params = {
                'api_key': self.serpapi_key,
                'engine': 'youtube',
                'search_query': keyword,
                'gl': 'us',
                'hl': 'en'
            }
            
            response = requests.get(self.base_serpapi, params=params, timeout=10)
            data = response.json()
            
            if 'video_results' not in data:
                return self._analyze_competition_basic(keyword)
            
            videos = data['video_results'][:20]
            analysis = self._process_competition_data(videos, keyword)
            
            if cache_key and self.cache:
                self.cache.set(cache_key, analysis, 'search')
            
            return analysis
            
        except Exception as e:
            st.warning(f"Ошибка анализа конкуренции: {e}")
            return self._analyze_competition_basic(keyword)
    
    def _analyze_competition_basic(self, keyword: str) -> dict:
        """Базовый анализ конкуренции (без внешних API)"""
        # Простая эстимация конкуренции
        word_count = len(keyword.split())
        
        if word_count == 1:
            competition_level = "High"
            optimized_ratio = 0.8
        elif word_count == 2:
            competition_level = "Medium"
            optimized_ratio = 0.6
        else:
            competition_level = "Low"
            optimized_ratio = 0.4
        
        return {
            'total_videos': 20,
            'optimized_titles': int(20 * optimized_ratio),
            'high_view_videos': max(1, int(20 * (1 - optimized_ratio))),
            'verified_channels': max(1, int(20 * 0.3)),
            'avg_views': 15000 if word_count == 1 else 8000,
            'keyword_in_title': int(20 * optimized_ratio),
            'recent_videos': 3,
            'competition_level': competition_level
        }
    
    def _process_competition_data(self, videos: list, keyword: str) -> dict:
        """Обработка данных конкуренции"""
        analysis = {
            'total_videos': len(videos),
            'optimized_titles': 0,
            'high_view_videos': 0,
            'verified_channels': 0,
            'avg_views': 0,
            'keyword_in_title': 0,
            'recent_videos': 0
        }
        
        total_views = 0
        keyword_lower = keyword.lower()
        
        for video in videos:
            title = video.get('title', '').lower()
            if any(word in title for word in keyword_lower.split()):
                analysis['optimized_titles'] += 1
                analysis['keyword_in_title'] += 1
            
            views = self._extract_views(video.get('views', '0'))
            total_views += views
            
            if views > 100000:
                analysis['high_view_videos'] += 1
            
            channel = video.get('channel', {})
            if 'verified' in str(channel).lower():
                analysis['verified_channels'] += 1
            
            published = video.get('published_date', '')
            if self._is_recent(published):
                analysis['recent_videos'] += 1
        
        if analysis['total_videos'] > 0:
            analysis['avg_views'] = total_views // analysis['total_videos']
        
        return analysis
    
    def _extract_views(self, views_str: str) -> int:
        """Извлекает количество просмотров"""
        if not views_str:
            return 0
        
        clean = ''.join(c for c in str(views_str) if c.isdigit() or c in [',', '.'])
        try:
            number_str = clean.replace(',', '').replace('.', '')
            return int(number_str) if number_str else 0
        except:
            return 0
    
    def _is_recent(self, date_str: str) -> bool:
        """Проверка свежести видео"""
        if not date_str:
            return False
        
        recent_indicators = ['day', 'days', 'week', 'weeks', 'hour', 'hours']
        return any(indicator in date_str.lower() for indicator in recent_indicators)
    
    def calculate_scores(self, keyword: str, analysis: dict, search_volume: int) -> TagScore:
        """Рассчитывает все оценки для тега"""
        # Оценка конкуренции
        total = analysis['total_videos']
        if total == 0:
            competition_score = 50
        else:
            optimized_ratio = analysis['optimized_titles'] / total
            high_views_ratio = analysis['high_view_videos'] / total
            verified_ratio = analysis['verified_channels'] / total
            
            # Нормализация средних просмотров
            avg_views_factor = min(analysis['avg_views'] / 500000, 1.0)
            
            competition_score = min(int((
                optimized_ratio * 0.3 +
                high_views_ratio * 0.25 +
                verified_ratio * 0.2 +
                avg_views_factor * 0.25
            ) * 100), 100)
        
        # SEO оценка (обратно пропорциональна оптимизации конкурентов)
        if total > 0:
            keyword_optimization = analysis['keyword_in_title'] / total
            seo_score = max(int((1.0 - keyword_optimization) * 100), 10)
        else:
            seo_score = 50
        
        # Общая оценка
        import math
        volume_score = min(math.log10(max(search_volume, 1)) * 25, 100)
        competition_inverted = 100 - competition_score
        
        overall_score = min(int(
            volume_score * 0.4 +
            competition_inverted * 0.35 +
            seo_score * 0.25
        ), 100)
        
        # Уровень сложности
        if competition_score <= 20:
            difficulty = "Очень низкая 🟢"
        elif competition_score <= 40:
            difficulty = "Низкая 🟢"
        elif competition_score <= 60:
            difficulty = "Средняя 🟡"
        elif competition_score <= 80:
            difficulty = "Высокая 🟠"
        else:
            difficulty = "Очень высокая 🔴"
        
        return TagScore(
            keyword=keyword,
            search_volume=search_volume,
            competition_score=competition_score,
            seo_score=seo_score,
            overall_score=overall_score,
            difficulty=difficulty
        )
    
    def analyze_keyword(self, keyword: str) -> TagScore:
        """Полный анализ ключевого слова"""
        search_volume = self.get_search_volume_serpapi(keyword)
        competition_analysis = self.analyze_competition_serpapi(keyword)
        
        return self.calculate_scores(keyword, competition_analysis, search_volume)
    
    def analyze_multiple_keywords(self, keywords: list) -> list:
        """Анализ нескольких ключевых слов"""
        results = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, keyword in enumerate(keywords):
            try:
                status_text.text(f"🏷️ Анализирую тег: {keyword}")
                progress_bar.progress((i + 1) / len(keywords))
                
                result = self.analyze_keyword(keyword)
                results.append(result)
                
                # Задержка между запросами
                if self.use_serpapi:
                    time.sleep(1)
                    
            except Exception as e:
                st.warning(f"Ошибка анализа '{keyword}': {e}")
                continue
        
        progress_bar.empty()
        status_text.empty()
        
        return sorted(results, key=lambda x: x.overall_score, reverse=True)

def validate_serpapi_key(api_key: str) -> bool:
    """Проверка формата SerpAPI ключа"""
    if not api_key:
        return False
    
    # SerpAPI ключи обычно длинные и содержат буквы и цифры
    if len(api_key) > 30 and all(c.isalnum() for c in api_key):
        return True
    
    return False

# --- 5. ГЛАВНЫЙ ИНТЕРФЕЙС ---

def main():
    st.markdown('<h1 class="main-header">YouTube AI Strategist 🧠</h1>', unsafe_allow_html=True)
    
    # Боковая панель
    with st.sidebar:
        st.header("⚙️ Настройки")
        
        # YouTube API
        st.subheader("🔑 YouTube API")
        youtube_api_key = st.text_input(
            "YouTube API Key", 
            type="password",
            help="Получите ключ в Google Cloud Console"
        )
        
        if youtube_api_key:
            if validate_youtube_api_key(youtube_api_key):
                st.success("✅ YouTube API ключ выглядит корректно")
            else:
                st.warning("⚠️ Формат ключа может быть неверным")
                st.info("💡 YouTube ключи обычно начинаются с 'AIza...' и содержат 39 символов")
        
        st.markdown("---")
        
        # SerpAPI настройки для анализа тегов
        st.subheader("🏷️ Анализ тегов")
        use_serpapi = st.toggle("Включить расширенный анализ тегов (SerpAPI)", value=False)
        
        serpapi_key = ""
        if use_serpapi:
            serpapi_key = st.text_input(
                "SerpAPI Key", 
                type="password",
                help="Ключ для детального анализа тегов и конкуренции"
            )
            
            if serpapi_key:
                if validate_serpapi_key(serpapi_key):
                    st.success("✅ SerpAPI ключ выглядит корректно")
                else:
                    st.warning("⚠️ Формат ключа может быть неверным")
            
            st.info("💡 SerpAPI дает 100 бесплатных запросов/месяц")
        
        st.markdown("---")
        
        # OpenAI настройки
        st.subheader("🤖 AI-стратег")
        use_openai = st.toggle("Включить AI-анализ (OpenAI)", value=True)
        
        openai_api_key = ""
        openai_model = "gpt-4o-mini"
        
        if use_openai:
            openai_api_key = st.text_input(
                "OpenAI API Key", 
                type="password",
                help="Ключ для генерации AI-стратегий"
            )
            
            if openai_api_key:
                if validate_openai_api_key(openai_api_key):
                    st.success("✅ OpenAI API ключ валиден")
                else:
                    st.error("❌ Неверный OpenAI API ключ")
                    st.info("💡 Ключ должен начинаться с 'sk-'")
            
            openai_model = st.selectbox(
                "Модель OpenAI", 
                ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo"], 
                index=1,
                help="gpt-4o-mini - быстрее и дешевле"
            )
        
        st.markdown("---")
        
        # Параметры анализа
        st.subheader("🔍 Параметры анализа")
        max_results = st.slider("Видео для анализа", 20, 200, 100, 10)
        
        date_range_options = {
            "За все время": None,
            "За последний год": 365,
            "За 6 месяцев": 180,
            "За 3 месяца": 90,
            "За месяц": 30
        }
        
        selected_date_range = st.selectbox(
            "Период анализа:", 
            list(date_range_options.keys()), 
            index=1
        )
        days_limit = date_range_options[selected_date_range]
        
        # Проверка API ключей
        if not youtube_api_key:
            st.warning("👆 Введите YouTube API ключ для начала работы")
            st.info("📚 [Как получить API ключ](https://developers.google.com/youtube/v3/getting-started)")
            st.stop()
        
        # Инициализация компонентов
        cache = CacheManager()
        
        # Статистика кэша и управление
        st.markdown("---")
        st.subheader("💾 Кэш")
        st.info(f"**Попадания:** {cache.stats['hits']} | **Промахи:** {cache.stats['misses']}")
        
        if st.button("🧹 Очистить устаревший кэш"):
            deleted = cache.clean_expired()
            st.success(f"Удалено {deleted} устаревших записей")
        
        # Контакты
        st.markdown("---")
        st.subheader("👨‍💻 Автор")
        st.markdown("""
        **Связаться:**
        - 💬 [Telegram](https://t.me/i_gma)
        - 📢 [Канал о AI](https://t.me/igm_a)
        - 🔗 [GitHub](https://github.com/yourusername)
        """)

    # Главная область
    st.markdown("### 🎯 Введите тему для анализа")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        keyword = st.text_input(
            "",
            placeholder="Например: n8n автоматизация, фотография для начинающих, криптовалюты...",
            help="Введите ключевое слово или фразу для анализа YouTube ниши"
        )
    
    with col2:
        analyze_button = st.button(
            "🚀 Глубокий анализ!", 
            type="primary", 
            use_container_width=True,
            disabled=not keyword
        )

    # Примеры запросов
    st.markdown("**💡 Примеры запросов:**")
    example_cols = st.columns(3)
    
    examples = [
        "python для начинающих",
        "монтаж видео",
        "инвестиции в акции"
    ]
    
    for i, example in enumerate(examples):
        if example_cols[i % 3].button(f"📌 {example}", key=f"example_{i}"):
            keyword = example
            analyze_button = True

    # Основной анализ
    if analyze_button and keyword:
        try:
            # Инициализация анализаторов
            analyzer = YouTubeAnalyzer(youtube_api_key, cache)
            trends_analyzer = AdvancedTrendsAnalyzer(cache)
            
            # Мягкая проверка соединения (не блокирующая)
            analyzer.test_connection()
            
            # Определение спиннера
            spinner_text = "🌊 Анализирую YouTube..."
            if use_openai and openai_api_key and validate_openai_api_key(openai_api_key):
                spinner_text += " Привлекаю AI..."

            with st.spinner(spinner_text):
                # Подготовка даты
                published_after_date = None
                if days_limit:
                    published_after_date = (datetime.now() - timedelta(days=days_limit)).isoformat("T") + "Z"
                
                # Поиск видео
                videos = analyzer.search_videos(keyword, max_results, published_after=published_after_date)
                
                if videos is None:
                    st.error("❌ Ошибка при получении данных из YouTube API")
                    st.stop()
                
                if not videos:
                    st.warning("🔍 Не найдено видео по данному запросу. Попробуйте:")
                    st.markdown("""
                    - Изменить ключевое слово
                    - Увеличить период анализа
                    - Использовать более общие термины
                    """)
                    st.stop()
                
                # Анализ конкуренции
                comp_analysis, df = analyzer.analyze_competition(videos)
                
                # Анализ трендов
                trends_data = trends_analyzer.analyze_keyword_trends(keyword)
                
                # Генерация стратегии
                strategist = ContentStrategist(
                    openai_api_key if use_openai and validate_openai_api_key(openai_api_key) else None,
                    openai_model if use_openai else None
                )
                strategy_output = strategist.get_strategy(keyword, comp_analysis, trends_data, df, cache)

            # Отображение результатов
            st.markdown("---")
            st.markdown(f"# 📊 Анализ ниши: **{keyword}**")
            
            # Основные метрики
            st.markdown("### 🎯 Ключевые показатели")
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric(
                    "📹 Видео", 
                    f"{len(df)}",
                    help="Количество проанализированных видео"
                )
            
            with col2:
                competition_level = comp_analysis['competition_level']
                st.metric(
                    "🏆 Конкуренция", 
                    competition_level.split()[0],
                    help=f"Полный статус: {competition_level}"
                )
            
            with col3:
                avg_views = comp_analysis['avg_views']
                st.metric(
                    "👀 Средние просмотры", 
                    format_number(int(avg_views)),
                    help=f"Точное значение: {int(avg_views):,}"
                )
            
            with col4:
                engagement = comp_analysis['engagement_rate']
                st.metric(
                    "💬 Вовлеченность", 
                    f"{engagement:.1f}%",
                    help="Соотношение лайков и комментариев к просмотрам"
                )
            
            with col5:
                channels = comp_analysis['unique_channels']
                st.metric(
                    "📺 Каналов", 
                    channels,
                    help="Количество уникальных каналов в выборке"
                )

            # Дополнительная статистика
            st.markdown("### 📈 Детальная статистика")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric(
                    "🔥 Топ-10 видео",
                    format_number(int(comp_analysis['top_10_avg_views'])),
                    help="Средние просмотры лучших 10 видео"
                )
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric(
                    "📱 Shorts",
                    f"{comp_analysis['shorts_percentage']:.0f}%",
                    help="Процент коротких видео (до 1 минуты)"
                )
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col3:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric(
                    "🗓️ За неделю",
                    f"{comp_analysis['videos_last_week']} шт.",
                    help="Количество видео, опубликованных за последнюю неделю"
                )
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col4:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                if 'avg_duration' in comp_analysis and comp_analysis['avg_duration'] > 0:
                    duration_str = f"{comp_analysis['avg_duration']:.1f} мин"
                else:
                    duration_str = "N/A"
                st.metric(
                    "⏱️ Средняя длина",
                    duration_str,
                    help="Средняя продолжительность видео (без Shorts)"
                )
                st.markdown('</div>', unsafe_allow_html=True)

            # Табы с результатами
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "🎯 AI Стратегия", 
                "🏷️ Анализ тегов",
                "📈 Тренды", 
                "🏆 Топ видео", 
                "📊 Аналитика"
            ])

            with tab1:
                css_class = "openai-result" if strategist.use_openai else "custom-container"
                st.markdown(f'<div class="{css_class}">', unsafe_allow_html=True)
                st.markdown(strategy_output)
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Дополнительные инсайты
                if not df.empty:
                    st.markdown("### 🔍 Дополнительные инсайты")
                    
                    # Анализ популярных слов
                    titles = df['title'].tolist()
                    popular_words = extract_keywords_from_titles(titles)
                    
                    if popular_words:
                        st.markdown("**🏷️ Популярные слова в заголовках:**")
                        words_cols = st.columns(5)
                        for i, (word, count) in enumerate(popular_words[:5]):
                            words_cols[i].metric(word, count)
                    
                    # Анализ каналов
                    top_channels = df.nlargest(20, 'views').groupby('channel').agg({
                        'views': 'mean',
                        'subscribers': 'first',
                        'video_id': 'count'
                    }).round(0).sort_values('views', ascending=False)
                    
                    if not top_channels.empty:
                        st.markdown("**📺 Ведущие каналы в нише:**")
                        st.dataframe(
                            top_channels.head(10).rename(columns={
                                'views': 'Средние просмотры',
                                'subscribers': 'Подписчиков',
                                'video_id': 'Видео в выборке'
                            }),
                            use_container_width=True
                        )

            with tab2:
                st.markdown("### 🏷️ Анализ эффективности тегов")
                
                # Извлекаем теги из найденных видео
                all_tags = []
                for video in videos:
                    if 'tags' in video and video['tags']:
                        all_tags.extend(video['tags'])
                
                # Добавляем слова из заголовков как потенциальные теги
                title_words = []
                for video in videos:
                    words = re.findall(r'\b[а-яё]{3,}|[a-z]{3,}\b', video['title'].lower())
                    title_words.extend(words)
                
                # Объединяем и фильтруем
                stop_words = {'как', 'что', 'для', 'это', 'все', 'еще', 'где', 'так', 'или', 'уже', 'при', 'его', 'они', 'был', 'the', 'and', 'for', 'you', 'are', 'not', 'can', 'but', 'all', 'any', 'had', 'her', 'was', 'one', 'our', 'out', 'day', 'get', 'use', 'man', 'new', 'now', 'way', 'may'}
                
                potential_tags = list(set(all_tags + title_words))
                potential_tags = [tag for tag in potential_tags if len(tag) > 2 and tag.lower() not in stop_words]
                
                # Анализ популярности тегов
                tag_popularity = Counter(all_tags)
                popular_tags = [tag for tag, count in tag_popularity.most_common(20) if count > 1]
                
                if popular_tags:
                    st.markdown("#### 📊 Популярные теги в нише")
                    
                    # Выбор тегов для анализа
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**🔥 Топ теги конкурентов:**")
                        selected_tags = []
                        for i, (tag, count) in enumerate(tag_popularity.most_common(10)):
                            if st.checkbox(f"{tag} ({count} раз)", key=f"tag_{i}"):
                                selected_tags.append(tag)
                    
                    with col2:
                        st.markdown("**➕ Добавить свои теги:**")
                        custom_tags = st.text_area(
                            "Введите теги через запятую:",
                            placeholder="автоматизация, программирование, python",
                            help="Добавьте свои теги для анализа"
                        )
                        
                        if custom_tags:
                            custom_list = [tag.strip() for tag in custom_tags.split(',') if tag.strip()]
                            selected_tags.extend(custom_list)
                    
                    # Кнопка анализа
                    if selected_tags and st.button("🔍 Анализировать выбранные теги", type="primary"):
                        # Инициализация анализатора тегов
                        tag_analyzer = YouTubeTagAnalyzer(
                            serpapi_key if use_serpapi and serpapi_key else None,
                            cache
                        )
                        
                        with st.spinner("🏷️ Анализирую эффективность тегов..."):
                            tag_results = tag_analyzer.analyze_multiple_keywords(selected_tags[:10])  # Лимит 10 тегов
                        
                        if tag_results:
                            st.markdown("#### 🎯 Результаты анализа тегов")
                            
                            # Таблица результатов
                            results_data = []
                            for result in tag_results:
                                results_data.append({
                                    'Тег': result.keyword,
                                    'Поисковый объем': f"{result.search_volume:,}",
                                    'Конкуренция': f"{result.competition_score}/100",
                                    'SEO возможности': f"{result.seo_score}/100",
                                    'Общая оценка': f"{result.overall_score}/100",
                                    'Сложность': result.difficulty
                                })
                            
                            results_df = pd.DataFrame(results_data)
                            st.dataframe(results_df, use_container_width=True, hide_index=True)
                            
                            # Топ-3 рекомендации
                            st.markdown("#### 💡 Рекомендуемые теги")
                            
                            top_3 = tag_results[:3]
                            cols = st.columns(3)
                            
                            for i, result in enumerate(top_3):
                                with cols[i]:
                                    st.markdown(f"""
                                    <div class="metric-card">
                                    <h4>🏆 #{i+1}: {result.keyword}</h4>
                                    <p><strong>Оценка:</strong> {result.overall_score}/100</p>
                                    <p><strong>Объем:</strong> {result.search_volume:,}</p>
                                    <p><strong>Сложность:</strong> {result.difficulty}</p>
                                    </div>
                                    """, unsafe_allow_html=True)
                            
                            # Инсайты и рекомендации
                            st.markdown("#### 🎯 Стратегические инсайты")
                            
                            if tag_results:
                                avg_competition = sum(r.competition_score for r in tag_results) / len(tag_results)
                                avg_seo_score = sum(r.seo_score for r in tag_results) / len(tag_results)
                                
                                insights = []
                                
                                if avg_competition < 40:
                                    insights.append("✅ **Низкая конкуренция** - отличная возможность для входа в нишу")
                                elif avg_competition > 70:
                                    insights.append("⚠️ **Высокая конкуренция** - нужен уникальный подход и высокое качество")
                                
                                if avg_seo_score > 60:
                                    insights.append("🎯 **Хорошие SEO возможности** - конкуренты слабо оптимизируют теги")
                                
                                best_tag = max(tag_results, key=lambda x: x.overall_score)
                                insights.append(f"🏆 **Лучший тег**: '{best_tag.keyword}' (оценка {best_tag.overall_score}/100)")
                                
                                for insight in insights:
                                    st.markdown(insight)
                            
                            # Экспорт результатов
                            csv_tags = pd.DataFrame([{
                                'Keyword': r.keyword,
                                'Search_Volume': r.search_volume,
                                'Competition_Score': r.competition_score,
                                'SEO_Score': r.seo_score,
                                'Overall_Score': r.overall_score,
                                'Difficulty': r.difficulty
                            } for r in tag_results])
                            
                            csv_tags_export = csv_tags.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                "📥 Скачать анализ тегов (CSV)",
                                csv_tags_export,
                                f'tag_analysis_{keyword.replace(" ", "_")}.csv',
                                'text/csv'
                            )
                    
                    # Если SerpAPI не используется, показываем подсказку
                    if not use_serpapi:
                        st.info("💡 **Совет**: Включите SerpAPI в настройках для получения более точных данных о поисковом объеме и детальной конкуренции!")
                
                else:
                    st.warning("🏷️ Теги не найдены в проанализированных видео. Попробуйте:")
                    st.markdown("""
                    - Увеличить количество видео для анализа
                    - Изменить ключевое слово на более популярное
                    - Добавить свои теги в поле выше
                    """)

            with tab3:
                if trends_data and 'interest_df' in trends_data and not trends_data['interest_df'].empty:
                    st.markdown("### 📈 Динамика интереса (Google Trends)")
                    
                    # Основной график
                    interest_df = trends_data['interest_df']
                    
                    fig_trends = go.Figure()
                    fig_trends.add_trace(go.Scatter(
                        x=interest_df.index,
                        y=interest_df[keyword],
                        mode='lines+markers',
                        name='Интерес',
                        line=dict(color='#1f77b4', width=3),
                        marker=dict(size=6),
                        hovertemplate='<b>%{x}</b><br>Интерес: %{y}<extra></extra>'
                    ))
                    
                    fig_trends.update_layout(
                        title=f'Динамика поискового интереса: "{keyword}"',
                        xaxis_title='Дата',
                        yaxis_title='Индекс интереса',
                        hovermode='x unified',
                        template='plotly_dark'
                    )
                    
                    st.plotly_chart(fig_trends, use_container_width=True)
                    
                    # Статистика трендов
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric(
                            "📊 Направление тренда",
                            trends_data.get('trend_direction', 'Неизвестно')
                        )
                    
                    with col2:
                        current_interest = trends_data.get('current_interest', 0)
                        st.metric(
                            "🎯 Текущий интерес",
                            f"{current_interest:.0f}/100"
                        )
                    
                    with col3:
                        trend_strength = trends_data.get('trend_strength', 0) * 100
                        st.metric(
                            "⚡ Сила тренда",
                            f"{trend_strength:.1f}%"
                        )
                    
                    # Связанные запросы
                    if 'top_queries' in trends_data and not trends_data['top_queries'].empty:
                        st.markdown("### 🔍 Связанные запросы")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**📊 Топ запросы:**")
                            top_queries = trends_data['top_queries'].head(10)
                            for idx, row in top_queries.iterrows():
                                st.write(f"• {row['query']} ({row['value']}%)")
                        
                        with col2:
                            if 'rising_queries' in trends_data and not trends_data['rising_queries'].empty:
                                st.markdown("**🚀 Растущие запросы:**")
                                rising_queries = trends_data['rising_queries'].head(10)
                                for idx, row in rising_queries.iterrows():
                                    growth = row['value']
                                    if growth == 'Breakout':
                                        growth = '🔥 Взрыв'
                                    st.write(f"• {row['query']} (+{growth})")
                
                else:
                    st.markdown('<div class="warning-alert">', unsafe_allow_html=True)
                    st.warning("📈 Не удалось загрузить данные Google Trends. Возможные причины:")
                    st.markdown("""
                    - Временные ограничения API
                    - Слишком специфичный запрос
                    - Проблемы с подключением
                    """)
                    st.markdown('</div>', unsafe_allow_html=True)

            with tab3:
                st.markdown(f"### 🏆 Топ-50 видео по теме '{keyword}'")
                
                # Фильтры
                filter_col1, filter_col2, filter_col3 = st.columns(3)
                
                with filter_col1:
                    channels = ['Все каналы'] + sorted(df['channel'].unique().tolist())
                    selected_channel = st.selectbox("Фильтр по каналу:", channels)
                
                with filter_col2:
                    title_keyword = st.text_input("Поиск в заголовках:")
                
                with filter_col3:
                    min_views = st.number_input("Мин. просмотров:", min_value=0, value=0, step=1000)
                
                # Применение фильтров
                df_filtered = df.copy()
                
                if selected_channel != 'Все каналы':
                    df_filtered = df_filtered[df_filtered['channel'] == selected_channel]
                
                if title_keyword:
                    df_filtered = df_filtered[
                        df_filtered['title'].str.contains(title_keyword, case=False, na=False)
                    ]
                
                if min_views > 0:
                    df_filtered = df_filtered[df_filtered['views'] >= min_views]
                
                # Подготовка данных для отображения
                df_display = df_filtered.copy()
                if not df_display.empty:
                    df_display['published'] = pd.to_datetime(df_display['published']).dt.strftime('%Y-%m-%d')
                    df_display['views_formatted'] = df_display['views'].apply(format_number)
                    df_display['likes_formatted'] = df_display['likes'].apply(format_number)
                    df_display['duration_formatted'] = df_display['duration'].apply(
                        lambda x: f"{int(x)}:{int((x % 1) * 60):02d}" if x >= 1 else f"0:{int(x * 60):02d}"
                    )
                    
                    # Выбор колонок для отображения
                    display_columns = {
                        'title': 'Заголовок',
                        'channel': 'Канал',
                        'views_formatted': 'Просмотры',
                        'likes_formatted': 'Лайки',
                        'comments': 'Комментарии',
                        'duration_formatted': 'Длительность',
                        'published': 'Дата публикации'
                    }
                    
                    df_show = df_display[list(display_columns.keys())].rename(columns=display_columns)
                    df_show = df_show.sort_values('Просмотры', key=lambda x: df_filtered['views'], ascending=False)
                    
                    st.dataframe(
                        df_show.head(50),
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # Кнопка скачивания
                    csv = df_filtered.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📥 Скачать данные (CSV)",
                        csv,
                        f'youtube_analysis_{keyword.replace(" ", "_")}.csv',
                        'text/csv'
                    )
                else:
                    st.info("🔍 Видео не найдены с текущими фильтрами")

            with tab4:
                st.markdown("### 📊 Подробная аналитика")
                
                if not df.empty:
                    # График распределения просмотров
                    fig_views = px.histogram(
                        df, 
                        x='views', 
                        nbins=30,
                        title='Распределение просмотров',
                        labels={'views': 'Просмотры', 'count': 'Количество видео'}
                    )
                    fig_views.update_layout(template='plotly_dark')
                    st.plotly_chart(fig_views, use_container_width=True)
                    
                    # График просмотров по дням
                    if len(df) > 10:
                        df_time = df.copy()
                        df_time['published'] = pd.to_datetime(df_time['published'])
                        df_time['week'] = df_time['published'].dt.to_period('W')
                        
                        weekly_stats = df_time.groupby('week').agg({
                            'views': 'mean',
                            'video_id': 'count'
                        }).reset_index()
                        
                        weekly_stats['week_str'] = weekly_stats['week'].astype(str)
                        
                        fig_weekly = go.Figure()
                        
                        fig_weekly.add_trace(go.Scatter(
                            x=weekly_stats['week_str'],
                            y=weekly_stats['views'],
                            mode='lines+markers',
                            name='Средние просмотры',
                            yaxis='y',
                            line=dict(color='#1f77b4', width=2)
                        ))
                        
                        fig_weekly.add_trace(go.Bar(
                            x=weekly_stats['week_str'],
                            y=weekly_stats['video_id'],
                            name='Количество видео',
                            yaxis='y2',
                            opacity=0.6,
                            marker_color='#ff7f0e'
                        ))
                        
                        fig_weekly.update_layout(
                            title='Активность по неделям',
                            xaxis_title='Неделя',
                            yaxis=dict(title='Средние просмотры', side='left'),
                            yaxis2=dict(title='Количество видео', side='right', overlaying='y'),
                            template='plotly_dark'
                        )
                        
                        st.plotly_chart(fig_weekly, use_container_width=True)
                    
                    # Анализ корреляций
                    st.markdown("### 🔗 Корреляционный анализ")
                    
                    numeric_columns = ['views', 'likes', 'comments', 'duration', 'subscribers']
                    correlation_data = df[numeric_columns].corr()
                    
                    fig_corr = px.imshow(
                        correlation_data,
                        title='Корреляция между метриками',
                        color_continuous_scale='RdBu',
                        aspect='auto'
                    )
                    fig_corr.update_layout(template='plotly_dark')
                    st.plotly_chart(fig_corr, use_container_width=True)
                    
                    # Интерпретация корреляций
                    st.markdown("**💡 Ключевые корреляции:**")
                    
                    views_likes_corr = correlation_data.loc['views', 'likes']
                    views_subs_corr = correlation_data.loc['views', 'subscribers']
                    
                    st.write(f"• Просмотры ↔ Лайки: {views_likes_corr:.2f}")
                    st.write(f"• Просмотры ↔ Подписчики канала: {views_subs_corr:.2f}")
                    
                    if views_likes_corr > 0.7:
                        st.success("✅ Высокая корреляция просмотров и лайков - активная аудитория")
                    elif views_likes_corr < 0.3:
                        st.warning("⚠️ Низкая корреляция просмотров и лайков - пассивная аудитория")
                
                else:
                    st.info("📊 Недостаточно данных для аналитики")

        except Exception as e:
            st.error(f"❌ Произошла ошибка при анализе: {str(e)}")
            st.info("🔄 Попробуйте:")
            st.markdown("""
            - Проверить корректность API ключей
            - Изменить ключевое слово
            - Уменьшить количество видео для анализа
            - Обратиться в поддержку если проблема повторяется
            """)

if __name__ == "__main__":
    main()
