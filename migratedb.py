import sqlite3
import glob
import os
from datetime import datetime
import shutil
import re
import json
import html

def load_cleanup_config(config_file: str = "config.json") -> dict:
    """Загрузка правил очистки из конфига"""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
            return config.get('text_cleanup', {})
    except Exception as e:
        print(f"⚠️ Ошибка загрузки конфига: {e}")
        return {}

def clean_text(text: str, cleanup_config: dict) -> str:
    """Очистка текста согласно правилам"""
    try:
        remove_phrases = cleanup_config.get('remove_phrases', [])
        remove_patterns = cleanup_config.get('remove_patterns', [])

        # Удаляем HTML теги
        text = text.replace('<br>', '\n')
        text = text.replace('<br/>', '\n')
        text = text.replace('<p>', '\n')
        text = text.replace('</p>', '\n')
        text = re.sub('<[^<]+?>', '', text)
        
        # Декодируем HTML сущности
        text = html.unescape(text)
        
        # Удаляем заданные фразы
        for phrase in remove_phrases:
            text = text.replace(phrase, '')
        
        # Удаляем паттерны
        for pattern in remove_patterns:
            text = re.sub(pattern, '', text)
        
        # Очищаем пустые строки и лишние пробелы
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        text = '\n'.join(lines)
        
        # Удаляем множественные переносы строк
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        return text.strip()
    except Exception as e:
        print(f"⚠️ Ошибка при очистке текста: {e}")
        return text

def cleanup_database(db_path: str, cleanup_config: dict):
    """Очистка текста в базе данных"""
    print("\n🧹 Начинаем очистку текста в базе данных...")
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Получаем все записи
            cursor.execute('SELECT id, content FROM posts')
            records = cursor.fetchall()
            
            cleaned_count = 0
            total_records = len(records)
            
            for record_id, content in records:
                cleaned_text = clean_text(content, cleanup_config)
                if cleaned_text != content:
                    cursor.execute('''
                        UPDATE posts 
                        SET content = ? 
                        WHERE id = ?
                    ''', (cleaned_text, record_id))
                    cleaned_count += 1
                
                # Показываем прогресс
                if cleaned_count % 100 == 0:
                    print(f"✨ Обработано {cleaned_count}/{total_records} записей...")
            
            conn.commit()
            
        print(f"\n✅ Очистка завершена! Обновлено записей: {cleaned_count}")
        
    except Exception as e:
        print(f"❌ Ошибка при очистке базы данных: {e}")

def merge_databases(output_db: str = "tg-posts.db"):
    """
    Объединяет все .db файлы в текущей директории, удаляет дубликаты,
    удаляет старые базы данных и очищает текст
    """
    print("🔄 Начинаем объединение баз данных...")
    
    # Создаем директорию для бэкапа
    backup_dir = f"db_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(backup_dir, exist_ok=True)
    
    # Создаем новую БД с такой же структурой
    with sqlite3.connect(output_db) as new_conn:
        cursor = new_conn.cursor()
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
        new_conn.commit()

    # Получаем список всех .db файлов
    db_files = glob.glob("*.db")
    total_posts = 0
    merged_posts = 0
    processed_files = []

    for db_file in db_files:
        if db_file == output_db:
            continue

        print(f"\n📂 Обработка файла: {db_file}")
        
        try:
            with sqlite3.connect(db_file) as old_conn:
                old_cursor = old_conn.cursor()
                
                # Подсчитываем количество записей
                old_cursor.execute('SELECT COUNT(*) FROM posts')
                file_posts = old_cursor.fetchone()[0]
                total_posts += file_posts
                print(f"📊 Найдено записей: {file_posts}")

                # Получаем все записи из старой БД
                old_cursor.execute('''
                    SELECT post_id, content, published_date, source_url 
                    FROM posts
                ''')
                
                # Вставляем записи в новую БД с игнорированием дубликатов
                with sqlite3.connect(output_db) as new_conn:
                    new_cursor = new_conn.cursor()
                    for row in old_cursor:
                        try:
                            new_cursor.execute('''
                                INSERT OR IGNORE INTO posts 
                                (post_id, content, published_date, source_url)
                                VALUES (?, ?, ?, ?)
                            ''', row)
                            if new_cursor.rowcount > 0:
                                merged_posts += 1
                    
                        except sqlite3.Error as e:
                            print(f"⚠️ Ошибка при вставке записи: {e}")
                            continue
                    
                    new_conn.commit()
            
            processed_files.append(db_file)

        except sqlite3.Error as e:
            print(f"❌ Ошибка при обработке {db_file}: {e}")
            continue

    # Проверяем успешность объединения
    if merged_posts > 0:
        print("\n🔄 Создание бэкапа и удаление старых баз данных...")
        for db_file in processed_files:
            try:
                # Создаем бэкап
                shutil.copy2(db_file, os.path.join(backup_dir, db_file))
                # Удаляем старый файл
                os.remove(db_file)
                print(f"✅ Удален файл: {db_file}")
            except Exception as e:
                print(f"⚠️ Ошибка при обработке файла {db_file}: {e}")

        # После успешного объединения запускаем очистку
        cleanup_config = load_cleanup_config()
        if cleanup_config:
            cleanup_database(output_db, cleanup_config)
        else:
            print("\n⚠️ Не удалось загрузить правила очистки текста")

    # Выводим статистику
    print("\n=== Итоговая статистика ===")
    print(f"📚 Обработано файлов: {len(processed_files)}")
    print(f"📝 Всего записей в исходных БД: {total_posts}")
    print(f"✅ Уникальных записей в новой БД: {merged_posts}")
    print(f"🔄 Удалено дубликатов: {total_posts - merged_posts}")
    print(f"💾 Результат сохранен в: {output_db}")
    print(f"📦 Бэкап старых БД создан в директории: {backup_dir}")

if __name__ == "__main__":
    print("⚠️ ВНИМАНИЕ! Этот скрипт:")
    print("1. Объединит все базы данных")
    print("2. Удалит исходные файлы (будет создана резервная копия)")
    print("3. Очистит текст согласно правилам из config.json")
    response = input("Продолжить? (y/n): ").lower()
    
    if response == 'y':
        merge_databases()
    else:
        print("❌ Операция отменена")
