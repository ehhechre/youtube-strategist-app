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
from pytrends.request import TrendReq
from prophet import Prophet
import numpy as np
import openai

# --- 1. КОНФИГУРАЦИЯ СТРАНИЦЫ И СТИЛИ ---
st.set_page_config(
    page_title="YouTube AI Strategist 🧠",
    page_icon="🚀",
    layout="wide"
)
warnings.filterwarnings('ignore')

st.markdown("""
<style>
    .main-header { font-size: 2.8rem; color: #FF0000; text-align: center; margin-bottom: 2rem; font-weight: bold; }
    .stButton>button { border-radius: 8px; font-weight: bold; }
    .custom-container {
        background-color: rgba(42, 57, 62, 0.5); padding: 1.5rem; border-radius: 10px;
        border-left: 5px solid #00a0dc; margin-top: 1rem;
    }
    .openai-result {
        background-color: rgba(26, 142, 95, 0.1); padding: 1.5rem; border-radius: 10px;
        border-left: 5px solid #1a8e5f; margin-top: 1rem;
    }
    .insight-box {
        background-color: #262730; padding: 1rem; border-radius: 10px; margin-top: 1rem;
        border: 1px solid #444;
    }
</style>
""", unsafe_allow_html=True)


# --- 2. КЛАССЫ-АНАЛИЗАТОРЫ (ВСЯ ЛОГИКА) ---

class CacheManager:
    # ... (код без изменений)
    def __init__(self, cache_dir: str = "data/cache"):
        self.db_path = Path(cache_dir) / "youtube_ai_cache.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self._init_sqlite()
        self.ttl_map = {'search': 3600*4, 'trends': 3600*8, 'openai': 3600*24}
        self.stats = {'hits': 0, 'misses': 0}

    def _init_sqlite(self):
        with self.lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute('CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value BLOB, expires_at TIMESTAMP)')
            conn.commit()
            conn.close()

    def get(self, key: str):
        with self.lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute("SELECT value, expires_at FROM cache WHERE key = ?", (key,))
            result = cursor.fetchone()
            conn.close()
            if result and datetime.fromisoformat(result[1]) > datetime.now():
                self.stats['hits'] += 1
                return pickle.loads(result[0])
            elif result: self.delete(key)
            self.stats['misses'] += 1
            return None

    def set(self, key: str, value: any, category: str):
        with self.lock:
            ttl = self.ttl_map.get(category, 3600)
            expires_at = datetime.now() + timedelta(seconds=ttl)
            value_blob = pickle.dumps(value)
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO cache (key, value, expires_at) VALUES (?, ?, ?)",
                           (key, value_blob, expires_at.isoformat()))
            conn.commit()
            conn.close()

    def delete(self, key: str):
        with self.lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM cache WHERE key = ?", (key,))
            conn.commit()
            conn.close()

    def generate_key(self, *args) -> str:
        return hashlib.md5("".join(map(str, args)).encode('utf-8')).hexdigest()

class YouTubeAnalyzer:
    # ... (код без изменений, кроме publishedAfter в search_videos)
    def __init__(self, api_key: str, cache: CacheManager):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.cache = cache

    def search_videos(self, keyword: str, max_results: int = 100, published_after=None):
        cache_key = self.cache.generate_key('search', keyword, max_results, published_after)
        if cached_data := self.cache.get(cache_key):
            st.toast("🚀 Результаты поиска загружены из кэша!", icon="⚡️")
            return cached_data
        
        try:
            video_ids = []
            next_page_token = None
            search_params = {
                'q': keyword, 'part': 'id', 'type': 'video', 'order': 'relevance', 'regionCode': 'RU',
            }
            if published_after:
                search_params['publishedAfter'] = published_after

            while len(video_ids) < max_results:
                search_params['maxResults'] = min(50, max_results - len(video_ids))
                search_params['pageToken'] = next_page_token
                search_request = self.youtube.search().list(**search_params)
                search_response = search_request.execute()
                video_ids.extend([item['id']['videoId'] for item in search_response.get('items', []) if item.get('id', {}).get('kind') == 'youtube#video'])
                next_page_token = search_response.get('nextPageToken')
                if not next_page_token: break

            if not video_ids: return []

            videos = []
            for i in range(0, len(video_ids), 50):
                chunk_ids = video_ids[i:i+50]
                stats_request = self.youtube.videos().list(part='statistics,snippet,contentDetails', id=','.join(chunk_ids))
                stats_response = stats_request.execute()
                for item in stats_response.get('items', []):
                    stats = item.get('statistics', {})
                    duration = self._parse_duration(item['contentDetails']['duration'])
                    videos.append({
                        'title': item['snippet']['title'], 'channel': item['snippet']['channelTitle'],
                        'published': item['snippet']['publishedAt'], 'views': int(stats.get('viewCount', 0)),
                        'likes': int(stats.get('likeCount', 0)), 'comments': int(stats.get('commentCount', 0)),
                        'duration': duration, 'is_short': duration <= 1.05
                    })
            self.cache.set(cache_key, videos, 'search')
            return videos
        except Exception as e:
            st.error(f"Ошибка при поиске видео: {str(e)}")
            if "quotaExceeded" in str(e): st.error("🚨 Ваша дневная квота запросов к YouTube API исчерпана!")
            return None

    def _parse_duration(self, duration_str: str) -> float:
        match = re.search(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
        if not match: return 0
        h, m, s = (int(g or 0) for g in match.groups())
        return h * 60 + m + s / 60
    
    def analyze_competition(self, videos: list):
        if not videos: return {}, pd.DataFrame()
        df = pd.DataFrame(videos)
        df['published'] = pd.to_datetime(df['published'], errors='coerce').dt.tz_localize(None)
        df['views'] = df['views'].replace(0, 1)
        df['days_ago'] = (datetime.now() - df['published']).dt.days

        analysis = {
            'avg_views': df['views'].mean(),
            'top_10_avg_views': df.nlargest(10, 'views')['views'].mean(),
            'engagement_rate': ((df['likes'] + df['comments']) / df['views']).mean() * 100,
            'videos_last_week': len(df[df['days_ago'] <= 7]),
            'shorts_percentage': df['is_short'].mean() * 100 if not df.empty else 0,
            'avg_days_to_top_10': df.nlargest(10, 'views')['days_ago'].mean() if not df.empty else 0
        }
        score = 0
        if analysis['top_10_avg_views'] < 50000: score += 2
        elif analysis['top_10_avg_views'] < 250000: score += 1
        if analysis['videos_last_week'] < 5: score += 1
        
        level_map = {0: 'Высокая', 1: 'Высокая', 2: 'Средняя', 3: 'Низкая'}
        analysis['competition_level'] = level_map.get(score, 'Высокая')
        return analysis, df

class AdvancedTrendsAnalyzer:
    # ... (код без изменений)
    def __init__(self, cache: CacheManager):
        self.pytrends = TrendReq(hl='ru-RU', tz=180, timeout=(10, 25))
        self.cache = cache

    def analyze_keyword_trends(self, keyword: str):
        cache_key = self.cache.generate_key('advanced_trends', keyword)
        if cached_data := self.cache.get(cache_key):
            st.toast("📈 Данные трендов загружены из кэша!", icon="⚡️")
            return cached_data
        try:
            self.pytrends.build_payload([keyword], timeframe='today 12-m', geo='RU')
            interest = self.pytrends.interest_over_time()
            if interest.empty: return None
            
            series = interest[keyword]
            fh_avg, sh_avg = series.iloc[:len(series)//2].mean(), series.iloc[len(series)//2:].mean()
            trend = "Растущий 📈" if sh_avg > fh_avg * 1.15 else ("Падающий 📉" if fh_avg > sh_avg * 1.15 else "Стабильный ➡️")
            
            result = {'interest_df': interest, 'trend_direction': trend}
            self.cache.set(cache_key, result, 'trends')
            return result
        except Exception as e:
            st.warning(f"Не удалось получить данные из Google Trends: {str(e)}")
            return None

class ContentStrategist:
    def __init__(self, openai_key=None, openai_model=None):
        self.use_openai = bool(openai_key and openai_model)
        if self.use_openai:
            # Важно: сам ключ теперь не устанавливается глобально, а используется для создания клиента
            self.api_key = openai_key 
            self.model = openai_model

    def get_strategy(self, keyword: str, comp_analysis: dict, trends_data: dict, df: pd.DataFrame):
        cache_key = None
        if self.use_openai:
            cache_key = cache.generate_key('openai', keyword, self.model, str(comp_analysis), str(trends_data))
            if cached_strategy := cache.get(cache_key):
                st.toast("🤖 AI Стратегия загружена из кэша!", icon="🧠")
                return cached_strategy
        
        strategy = self._get_ai_strategy(keyword, comp_analysis, trends_data, df) if self.use_openai else self._get_rule_based_strategy(keyword, comp_analysis, df)
        
        if self.use_openai and cache_key and "Ошибка" not in strategy:
            cache.set(cache_key, strategy, 'openai')
        
        return strategy

    def _get_rule_based_strategy(self, keyword, comp_analysis, df):
        patterns = self._find_viral_patterns(df)
        ideas = [f"Полное руководство по {keyword} для начинающих в {datetime.now().year}", f"Топ-5 неочевидных фишек в {keyword}"]
        return f"""
        ### 💡 Базовая стратегия (на основе правил)
        
        **Ключевые паттерны в нише:**
        {patterns}
        ---
        **Идеи для видео:**
        {chr(10).join(f'- {i}' for i in ideas)}
        """

    def _find_viral_patterns(self, df: pd.DataFrame):
        if len(df) < 20: return "Недостаточно данных для поиска паттернов."
        df['engagement'] = (df['likes'] + df['comments']) / df['views'].replace(0, 1)
        viral = df[df['views'] > df['views'].quantile(0.8)]
        patterns = []
        viral_dur, regular_dur = viral['duration'].mean(), df['duration'].mean()
        if abs(viral_dur - regular_dur) > 1:
            patterns.append(f"- **Длительность:** Вирусные видео в среднем {'длиннее' if viral_dur > regular_dur else 'короче'} ({viral_dur:.0f} мин).")
        return "\n".join(patterns) if patterns else "Четких вирусных паттернов не найдено."

    def _get_ai_strategy(self, keyword, comp_analysis, trends_data, df):
        st.toast("🤖 Отправляю данные на анализ в OpenAI...", icon="🧠")
        top_titles = "\n".join([f"- {title}" for title in df.nlargest(5, 'views')['title']])
        prompt = f"""
        Ты — ведущий YouTube-стратег. Проведи глубокий и креативный анализ ниши по данным. Будь кратким, дерзким и давай действенные советы.

        АНАЛИЗИРУЕМАЯ ТЕМА: "{keyword}"
        ДАННЫЕ:
        - Уровень конкуренции: {comp_analysis['competition_level']}
        - Средние просмотры: {int(comp_analysis['avg_views']):,}
        - Вовлеченность: {comp_analysis['engagement_rate']:.2f}%
        - Тренд в Google: {trends_data['trend_direction'] if trends_data else 'Нет данных'}
        - Топ-5 заголовков конкурентов:
        {top_titles}

        ЗАДАНИЕ (в формате Markdown):
        1.  **🚀 Вердикт и Потенциал:** Оцени потенциал ниши одним предложением.
        2.  **💡 3 Креативные Идеи для Видео:** Придумай 3 неочевидные, цепляющие идеи с броскими заголовками.
        3.  **🎣 Уникальный "Крючок":** Какой формат поможет "взломать" алгоритм в этой нише?
        4.  **💣 Скрытый Риск:** Назови один неочевидный риск для автора в этой теме.
        """
        try:
            # --- ИЗМЕНЕННЫЙ БЛОК ---
            client = openai.OpenAI(api_key=self.api_key)
            response = client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=800
            )
            return response.choices[0].message.content
            # --- КОНЕЦ ИЗМЕНЕННОГО БЛОКА ---
        except Exception as e:
            return f"❌ Ошибка при обращении к OpenAI: {e}"

# --- 3. UI ПРИЛОЖЕНИЯ (STREAMLIT) ---

st.markdown('<h1 class="main-header">YouTube AI Strategist 🧠</h1>', unsafe_allow_html=True)

with st.sidebar:
    st.header("⚙️ Настройки")
    youtube_api_key = st.text_input("YouTube API Key", type="password")
    
    st.markdown("---")
    use_openai = st.toggle("🤖 Включить AI-стратега (OpenAI)", value=True)
    openai_api_key, openai_model = "", ""
    if use_openai:
        openai_api_key = st.text_input("OpenAI API Key", type="password")
        openai_model = st.selectbox("Модель OpenAI", ["gpt-4o", "gpt-4o-mini"], index=1)

    st.markdown("---")
    st.header("🔍 Параметры Анализа")
    max_results = st.slider("Видео для анализа", 10, 100, 80, 10)
    
    # --- НОВЫЙ ФИЛЬТР ПО ДАТЕ ---
    date_range_options = {"За все время": None, "За последний год": 365, "За 6 месяцев": 180, "За 3 месяца": 90}
    selected_date_range = st.selectbox("Анализировать видео за:", list(date_range_options.keys()), index=1)
    days_limit = date_range_options[selected_date_range]
    
    if not youtube_api_key: st.warning("👆 Введите YouTube API ключ."); st.stop()

    cache = CacheManager()
    st.markdown("---")
    st.info(f"**Кэш:** {cache.stats['hits']} попаданий / {cache.stats['misses']} промахов")

keyword = st.text_input("🔍 Введите ключевое слово или тему для анализа", placeholder="Например: n8n автоматизация")
if st.button("🚀 Глубокий анализ!", type="primary", use_container_width=True) and keyword:
    analyzer = YouTubeAnalyzer(youtube_api_key, cache)
    trends_analyzer = AdvancedTrendsAnalyzer(cache)
    
    spinner_text = "🌊 Погружаюсь в анализ..."
    if use_openai and openai_api_key: spinner_text += " Привлекаю AI..."

    with st.spinner(spinner_text):
        published_after_date = (datetime.now() - timedelta(days=days_limit)).isoformat("T") + "Z" if days_limit else None
        videos = analyzer.search_videos(keyword, max_results, published_after=published_after_date)
        if videos is None or not videos: 
            st.error("Не удалось получить данные о видео. Попробуйте изменить период анализа или ключевое слово.")
            st.stop()
        
        comp_analysis, df = analyzer.analyze_competition(videos)
        trends_data = trends_analyzer.analyze_keyword_trends(keyword)
        
        strategist = ContentStrategist(openai_api_key if use_openai else None, openai_model if use_openai else None)
        strategy_output = strategist.get_strategy(keyword, comp_analysis, trends_data, df)

    st.markdown("---")
    st.header(f"📊 Результаты анализа: **{keyword}**")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Проанализировано видео", f"{len(df)} шт.")
    col2.metric("Конкуренция", comp_analysis['competition_level'])
    col3.metric("Средние просмотры", f"{int(comp_analysis['avg_views']):,}")
    col4.metric("Вовлеченность", f"{comp_analysis['engagement_rate']:.2f}%")

    # --- НОВЫЙ БЛОК "КЛЮЧЕВЫЕ ИНСАЙТЫ" ---
    with st.container(border=True):
        st.markdown("#### 🔑 Ключевые инсайты")
        c1, c2, c3 = st.columns(3)
        c1.metric("Новых видео за неделю", f"{comp_analysis['videos_last_week']} шт.")
        c2.metric("Процент Shorts", f"{comp_analysis['shorts_percentage']:.0f}%")
        c3.metric("Средний 'возраст' топ-видео", f"{int(comp_analysis['avg_days_to_top_10'])} дней")


    tab1, tab2, tab3 = st.tabs(["🎯 Стратегия", "📈 Тренды", "🏆 Топ Видео и Данные"])

    with tab1:
        css_class = "openai-result" if strategist.use_openai else "custom-container"
        with st.container():
            st.markdown(f'<div class="{css_class}">{strategy_output}</div>', unsafe_allow_html=True)
    
    with tab2:
        if trends_data and 'interest_df' in trends_data:
            st.subheader("📈 Динамика интереса в Google Trends")
            fig_trends = px.line(trends_data['interest_df'], y=keyword, title=f'Интерес к "{keyword}" за 12 мес.')
            st.plotly_chart(fig_trends, use_container_width=True)
        else:
            st.warning("Не удалось загрузить данные из Google Trends.")

    with tab3:
        st.subheader(f"🏆 Топ-50 видео по теме '{keyword}'")
        
        # --- НОВЫЕ ИНТЕРАКТИВНЫЕ ФИЛЬТРЫ ---
        df_display = df.copy()
        df_display['published'] = pd.to_datetime(df_display['published']).dt.strftime('%Y-%m-%d')
        
        filter1, filter2 = st.columns(2)
        with filter1:
            # Фильтр по каналу
            channels = ['Все каналы'] + list(df_display['channel'].unique())
            selected_channel = st.selectbox("Фильтр по каналу:", channels)
            if selected_channel != 'Все каналы':
                df_display = df_display[df_display['channel'] == selected_channel]
        with filter2:
             # Фильтр по слову в заголовке
            title_keyword = st.text_input("Фильтр по слову в заголовке:")
            if title_keyword:
                df_display = df_display[df_display['title'].str.contains(title_keyword, case=False, na=False)]

        st.dataframe(df_display.nlargest(50, 'views'), use_container_width=True)
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Скачать все данные (CSV)", csv, f'youtube_data_{keyword}.csv', 'text/csv')
