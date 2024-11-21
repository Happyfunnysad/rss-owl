import warnings
import feedparser
import sqlite3
from datetime import datetime, timezone, timedelta
import time
from typing import Dict, Any
import html
import requests
from urllib.parse import quote
import json
import os

warnings.filterwarnings('ignore', category=DeprecationWarning)

class TelegramRSSParser:
    def __init__(self, db_name: str = "tg-posts.db", config_file: str = "config.json"):
        self.db_name = db_name
        self.config = self.load_config(config_file)
        self.channels = self.config.get('channels', [])
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        self.rss_sources = [
            "https://tg.i-c-a.su/rss/{channel}",
            "https://rsshub.app/telegram/channel/{channel}",
            "https://telegram.meta.ua/rss/{channel}",
            "https://tg.i-c-a.su/rss/{channel}?format=html",

        ]
        self.init_db()

    def load_config(self, config_file: str) -> dict:
        """Загрузка конфигурации из JSON файла"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            # Установка значений по умолчанию, если их нет в конфиге
            if 'check_intervals' not in config:
                config['check_intervals'] = {
                    'initial': 30,
                    'min': 15,
                    'max': 60,
                    'increment': 5
                }
            return config
        except Exception as e:
            print(f"Ошибка загрузки конфигурации: {e}")
            return {
                'channels': [],
                'check_intervals': {
                    'initial': 30,
                    'min': 15,
                    'max': 60,
                    'increment': 5
                }
            }

    def init_db(self):
        """Инициализация БД с поддержкой UTC"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id TEXT UNIQUE,
                    content TEXT,
                    published_date TIMESTAMP,
                    source_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_post_id ON posts(post_id)')
            conn.commit()

    def clean_text(self, text: str) -> str:
        """Улучшенная очистка текста поста"""
        try:
            # Загружаем настройки очистки
            cleanup_config = self.config.get('text_cleanup', {})
            remove_phrases = cleanup_config.get('remove_phrases', [])
            remove_patterns = cleanup_config.get('remove_patterns', [])

            # Удаляем HTML теги, но сохраняем структуру
            text = text.replace('<br>', '\n')
            text = text.replace('<br/>', '\n')
            text = text.replace('<p>', '\n')
            text = text.replace('</p>', '\n')
            
            # Обработка forwarded сообщений
            if 'Forwarded From' in text:
                text = text.split('</b>')[-1].strip()
            
            # Удаляем div контейнеры
            if '<div class="tgme_widget_message_text' in text:
                text = text.split('dir="auto">')[-1]
                text = text.split('</div>')[0]
                
            # Удаляем все оставшиеся HTML теги
            import re
            text = re.sub('<[^<]+?>', '', text)
            
            # Удаляем заданные фразы
            for phrase in remove_phrases:
                text = text.replace(phrase, '')
            
            # Удаляем паттерны по регулярным выражениям
            for pattern in remove_patterns:
                text = re.sub(pattern, '', text)
            
            # Декодируем HTML сущности
            text = html.unescape(text)
            
            # Очищаем пустые строки и лишние пробелы
            lines = []
            for line in text.splitlines():
                line = line.strip()
                if line and not any(phrase in line for phrase in remove_phrases):
                    lines.append(line)
            
            text = '\n'.join(lines)
            
            # Удаляем множественные переносы строк
            text = re.sub(r'\n\s*\n', '\n\n', text)
            
            return text.strip()
            
        except Exception as e:
            print(f"⚠️ Ошибка при очистке текста: {e}")
            return text

    def get_feed_data(self, channel_name: str) -> dict:
        """Получение данных RSS из всех источников"""
        all_entries = []
        successful_source = None

        print(f"\n{'='*50}")
        print(f"Канал: {channel_name}")
        print(f"{'='*50}")

        for source_url in self.rss_sources:
            try:
                url = source_url.format(channel=channel_name)
                print(f"📡 {url.split('/')[2]}: ", end='')
                
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    timeout=10,
                    verify=True
                )
                
                if response.status_code == 200:
                    feed = feedparser.parse(response.text)
                    
                    if hasattr(feed, 'entries') and len(feed.entries) > 0:
                        all_entries.extend(feed.entries)
                        successful_source = url
                        print(f"✅ {len(feed.entries)} записей")
                    else:
                        print("❌ Пустой фид")
                else:
                    print(f"❌ Ошибка {response.status_code}")
                    
            except Exception as e:
                print(f"❌ {str(e)[:50]}...")
                continue

        if all_entries:
            unique_entries = {}
            for entry in all_entries:
                post_id = entry.link.split('/')[-1]
                if post_id not in unique_entries:
                    unique_entries[post_id] = entry

            print(f"\n📊 Итого уникальных записей: {len(unique_entries)}")
            feed = type('obj', (object,), {'entries': list(unique_entries.values())})
            return feed

        print("\n❌ Не удалось получить данные")
        return None

    def save_post(self, post_data: Dict[str, Any]) -> bool:
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO posts 
                    (post_id, content, published_date, source_url)
                    VALUES (?, ?, ?, ?)
                ''', (
                    post_data['post_id'],
                    post_data['content'],
                    post_data['published_date'],
                    post_data['source_url']
                ))
                if cursor.rowcount > 0:
                    self.save_to_txt(post_data)
                    return True
                return False
        except sqlite3.Error as e:
            print(f"⚠️ Ошибка при сохранении в БД: {e}")
            return False

    def save_to_txt(self, post_data: Dict[str, Any]):
        """Сохранение поста в текстовый файл с сортировкой"""
        try:
            # Читаем существующие записи
            entries = []
            if os.path.exists('tg-posts.txt'):
                with open('tg-posts.txt', 'r', encoding='utf-8') as f:
                    content = f.read()
                    raw_entries = content.split('\n\n')
                    for entry in raw_entries:
                        if entry.strip() and not entry.startswith('='): 
                            entries.append(entry.strip())

            # Добавляем новую запись
            new_entry = f"[{post_data['published_date']}] {post_data['content']}\nИсточник: {post_data['source_url']}"
            entries.append(new_entry)

            # Сортируем записи по дате
            def extract_date(entry):
                try:
                    date_str = entry[1:entry.find(']')]
                    # Приводим все даты к naive datetime для корректного сравнения
                    dt = datetime.fromisoformat(date_str)
                    if dt.tzinfo:
                        dt = dt.replace(tzinfo=None)
                    return dt
                except:
                    return datetime.min
            
            entries.sort(key=extract_date, reverse=True)

            # Записываем отсортированные записи
            with open('tg-posts.txt', 'w', encoding='utf-8') as f:
                f.write('\n\n'.join(entries))
            
            # Обновляем сводку
            self.update_txt_summary()
                
        except Exception as e:
            print(f"⚠️ Ошибка при сохранении в файл: {e}")

    def parse_date(self, date_str: str) -> datetime:
        """Улучшенный парсинг даты с поддержкой разных форматов"""
        try:
            # Список поддерживаемых форматов дат
            date_formats = [
                '%a, %d %b %Y %H:%M:%S %Z',     # Thu, 21 Nov 2024 09:02:50 GMT
                '%a, %d %b %Y %H:%M:%S %z',     # Thu, 21 Nov 2024 09:02:50 +0000
                '%Y-%m-%dT%H:%M:%S.%fZ',        # 2024-11-21T09:02:50.000Z
                '%Y-%m-%dT%H:%M:%SZ',           # 2024-11-21T09:02:50Z
                '%Y-%m-%d %H:%M:%S',            # 2024-11-21 09:02:50
            ]

            # Предварительная обработка строки даты
            date_str = date_str.strip()
            
            # Пробуем каждый формат
            for date_format in date_formats:
                try:
                    # Для форматов с временной зоной
                    if '%Z' in date_format:
                        # Заменяем GMT на +0000 для корректного парсинга
                        if 'GMT' in date_str:
                            date_str = date_str.replace('GMT', '+0000')
                        parsed_date = datetime.strptime(date_str, date_format)
                        return parsed_date.replace(tzinfo=timezone.utc)
                    else:
                        parsed_date = datetime.strptime(date_str, date_format)
                        # Добавляем UTC для дат без временной зоны
                        if not parsed_date.tzinfo:
                            parsed_date = parsed_date.replace(tzinfo=timezone.utc)
                        return parsed_date
                except ValueError:
                    continue

            # Если не удалось распарсить, логируем и возвращаем текущее время
            print(f"📅 Не удалось распарсить дату '{date_str}', используем текущее время")
            return datetime.now(timezone.utc)

        except Exception as e:
            print(f"⚠️ Ошибка при парсинге даты '{date_str}': {e}")
            return datetime.now(timezone.utc)

    def check_duplicate(self, post_id: str) -> bool:
        """Проверка на существование поста в БД"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT id FROM posts WHERE post_id = ?', (post_id,))
                return cursor.fetchone() is not None
        except Exception as e:
            print(f"Ошибка при проверке дубликата: {e}")
            return False

    def parse_feed(self, feed: dict, last_check_time: datetime = None) -> int:
        try:
            new_posts = 0
            existing_posts = 0
            
            for entry in feed.entries:
                try:
                    post_id = entry.link.split('/')[-1]
                    
                    if self.check_duplicate(post_id):
                        existing_posts += 1
                        continue
                    
                    content = self.clean_text(entry.description)
                    published_date = self.parse_date(entry.published)
                    
                    if last_check_time and published_date <= last_check_time:
                        continue
                    
                    post_data = {
                        'post_id': post_id,
                        'content': content,
                        'published_date': published_date,
                        'source_url': entry.link
                    }
                    
                    if self.save_post(post_data):
                        new_posts += 1
                    
                except Exception as e:
                    print(f"⚠️ Ошибка обработки поста {post_id}: {e}")
                    continue
            
            return new_posts
            
        except Exception as e:
            print(f"❌ Ошибка парсинга: {e}")
            return 0

    def get_latest_posts(self, limit: int = 10) -> list:
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT post_id, content, published_date
                FROM posts
                ORDER BY published_date DESC
                LIMIT ?
            ''', (limit,))
            return cursor.fetchall()

    def get_db_stats(self) -> dict:
        """Получение статистики базы данных"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                
                # Общее количество постов
                cursor.execute('SELECT COUNT(*) FROM posts')
                total_posts = cursor.fetchone()[0]
                
                # Последние посты
                cursor.execute('''
                    SELECT post_id, published_date, content 
                    FROM posts 
                    ORDER BY published_date DESC 
                    LIMIT 5
                ''')
                latest_posts = cursor.fetchall()
                
                # Статистика по датам
                cursor.execute('''
                    SELECT DATE(published_date) as date, COUNT(*) as count 
                    FROM posts 
                    GROUP BY DATE(published_date) 
                    ORDER BY date DESC 
                    LIMIT 5
                ''')
                posts_by_date = cursor.fetchall()
                
                return {
                    'total_posts': total_posts,
                    'latest_posts': latest_posts,
                    'posts_by_date': posts_by_date
                }
                
        except Exception as e:
            print(f"Ошибка при получении статистики: {e}")
            return None

    def print_db_stats(self):
        """Вывод статистики в консоль"""
        stats = self.get_db_stats()
        if not stats:
            return
        
        print("\n=== Статистика базы данных ===")
        print(f"Всего постов: {stats['total_posts']}")
        
        print("\nПоследние посты:")
        for post in stats['latest_posts']:
            post_id, date, content = post
            print(f"ID: {post_id}")
            print(f"Дата: {date}")
            print(f"Контент: {content[:100]}...")
            print("-" * 50)
        
        print("\nПосты по датам:")
        for date, count in stats['posts_by_date']:
            print(f"{date}: {count} постов")

    def get_posts_stats(self) -> dict:
        """олучение статистики по постам за последние дни"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT 
                        DATE(published_date) as date,
                        COUNT(*) as count
                    FROM posts
                    WHERE published_date >= DATE('now', '-7 days')
                    GROUP BY DATE(published_date)
                    ORDER BY date DESC
                    LIMIT 7
                ''')
                return dict(cursor.fetchall())
        except Exception as e:
            print(f"⚠️ Ошибка при получении статистики: {e}")
            return {}

    def print_cycle_stats(self, new_posts: int):
        """Вывод статистики после цикла проверки"""
        if new_posts > 0:
            print("\n📊 Статистика обновления:")
            print(f"➕ Добавлено новых постов: {new_posts}")
            
            # Статистика по дням
            stats = self.get_posts_stats()
            if stats:
                print("\n📅 Посты по датам:")
                for date, count in stats.items():
                    print(f"   {date}: {count:3d} постов")
        else:
            print("\n💤 Новых постов не обнаружено")

    def generate_summary(self) -> str:
        """Генерация сводки по всем собранным данным"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                
                # Общее количество постов
                cursor.execute('SELECT COUNT(*) FROM posts')
                total_posts = cursor.fetchone()[0]
                
                # Статистика по каналам с правильной группировкой
                cursor.execute('''
                    WITH channel_data AS (
                        SELECT 
                            CASE 
                                WHEN source_url LIKE '%t.me/%' 
                                THEN substr(source_url, instr(source_url, 't.me/') + 5)
                                ELSE source_url
                            END as full_channel
                        FROM posts
                    )
                    SELECT 
                        substr(full_channel, 1, instr(full_channel || '/', '/') - 1) as channel,
                        COUNT(*) as count
                    FROM channel_data
                    GROUP BY substr(full_channel, 1, instr(full_channel || '/', '/') - 1)
                    ORDER BY count DESC
                ''')
                channel_stats = cursor.fetchall()
                
                # Формирум текст сводки
                summary = [
                    "\n\n" + "="*50,
                    "СВОДКА ПО СОБРАННЫМ ДАННЫМ",
                    "="*50,
                    f"\nВсего собрано постов: {total_posts}",
                    "\nСтатистика по каналам:"
                ]
                
                # Добавляем статистику по каналам
                for channel, count in channel_stats:
                    # Правильное склонение слова "пост"
                    posts_word = "постов"
                    if count % 10 == 1 and count % 100 != 11:
                        posts_word = "пост"
                    elif count % 10 in [2, 3, 4] and count % 100 not in [12, 13, 14]:
                        posts_word = "поста"
                    
                    summary.append(f"- {channel}: {count} {posts_word}")
                
                summary.extend(["\n" + "="*50])
                return "\n".join(summary)
                
        except Exception as e:
            print(f"⚠️ Ошибка при генерации сводки: {e}")
            return ""

    def update_txt_summary(self):
        """Обновление сводки в конце файла"""
        try:
            # Проверяем существование файла
            if not os.path.exists('tg-posts.txt'):
                with open('tg-posts.txt', 'w', encoding='utf-8') as f:
                    f.write("")
                    return

            # Читаем файл
            with open('tg-posts.txt', 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Оставляем только посты, удаляя все промежуточные статистики
            posts = []
            for entry in content.split('\n\n'):
                # Пропускаем строки со статистикой
                if any(skip in entry for skip in [
                    "Всего собрано постов:",
                    "Статистика по каналам:",
                    "СВОДКА ПО СОБРАННЫМ ДАННЫМИ",
                    "="*10
                ]):
                    continue
                if entry.strip():
                    posts.append(entry.strip())
            
            # Генерируем новую сводку
            summary = self.generate_summary()
            
            # Записываем очищенный контент со сводкой
            with open('tg-posts.txt', 'w', encoding='utf-8') as f:
                if posts:
                    f.write('\n\n'.join(posts) + summary)
                else:
                    f.write(summary)
                
        except Exception as e:
            print(f"⚠️ Ошибка при обновлении сводки: {e}")

def main():
    parser = TelegramRSSParser()
    intervals = parser.config.get('check_intervals', {})
    check_interval = intervals.get('initial', 30)
    last_check_times = {channel: None for channel in parser.channels}
    
    while True:
        try:
            current_time = datetime.now(timezone.utc)
            print(f"\n🕒 Проверка: {current_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            
            total_new_posts = 0
            for channel in parser.channels:
                feed = parser.get_feed_data(channel)
                if feed:
                    new_posts = parser.parse_feed(feed, last_check_times[channel])
                    if new_posts > 0:
                        last_check_times[channel] = current_time
                        total_new_posts += new_posts
            
            # Выводим статистику после каждого цикла
            parser.print_cycle_stats(total_new_posts)
            
            # Корректируем интервал проверки согласно настройкам
            if total_new_posts > 0:
                check_interval = intervals.get('min', 15)
            else:
                check_interval = min(
                    check_interval + intervals.get('increment', 5),
                    intervals.get('max', 60)
                )
                
        except Exception as e:
            print(f"❌ Ошибк: {e}")
            check_interval = intervals.get('max', 60)
        
        print(f"\n⏳ Следующая проверка через {check_interval} сек...")
        time.sleep(check_interval)

if __name__ == "__main__":
    main()

#"alarmukraine",