import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import re
import seaborn as sns
from wordcloud import WordCloud
import numpy as np
import os

class TelegramAnalyzer:
    def __init__(self, db_path='tg-posts.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
        # Читаем данные с явным указанием формата даты
        self.df = pd.read_sql_query("""
            SELECT 
                id,
                post_id,
                content,
                strftime('%Y-%m-%d %H:%M:%S', published_date) as published_date,
                source_url,
                strftime('%Y-%m-%d %H:%M:%S', created_at) as created_at
            FROM posts
        """, self.conn)
        
        # Конвертируем даты
        self.df['published_date'] = pd.to_datetime(self.df['published_date'])
        self.df['created_at'] = pd.to_datetime(self.df['created_at'])

        # После конвертации добавляем UTC зону
        self.df['published_date'] = self.df['published_date'].dt.tz_localize('UTC')
        self.df['created_at'] = self.df['created_at'].dt.tz_localize('UTC')

    def basic_stats(self):
        """Базовая статистика по постам"""
        stats = {
            'Всего постов': len(self.df),
            'Уникальных каналов': len(self.df['source_url'].unique()),
            'Первый пост': self.df['published_date'].min(),
            'Последний пост': self.df['published_date'].max(),
            'Средняя длина поста (символов)': self.df['content'].str.len().mean(),
            'Медианная длина поста': self.df['content'].str.len().median(),
        }
        return stats

    def posts_by_channel(self):
        """Распределение постов по каналам"""
        channels = self.df['source_url'].apply(lambda x: x.split('/')[-1])
        return channels.value_counts()

    def posts_by_hour(self):
        """Распределение постов по часам"""
        return self.df['published_date'].dt.hour.value_counts().sort_index()

    def posts_by_date(self):
        """Количество постов по датам"""
        return self.df.groupby(self.df['published_date'].dt.date).size()

    def word_frequency(self, min_length=4):
        """Частота слов в постах"""
        words = ' '.join(self.df['content']).lower()
        words = re.findall(r'\b\w+\b', words)
        return Counter([w for w in words if len(w) >= min_length])

    def alert_keywords_analysis(self):
        """Анализ ключевых слов тревоги"""
        keywords = ['тревога', 'внимание', 'опасность', 'угроза', 'срочно' , 'fpv' , 'бпла' , 'Краснодар' , 'Краснодарский край'  , 'Ростов' , 'Ростовская область']
        stats = {}
        for keyword in keywords:
            mask = self.df['content'].str.contains(keyword, case=False)
            stats[keyword] = {
                'total_mentions': mask.sum(),
                'percentage': (mask.sum() / len(self.df)) * 100
            }
        return stats

    def response_time_analysis(self):
        """Анализ времени между публикацией и сохранением"""
        self.df['response_time'] = (self.df['created_at'] - self.df['published_date'])
        return {
            'mean_response': self.df['response_time'].mean(),
            'median_response': self.df['response_time'].median(),
            'min_response': self.df['response_time'].min(),
            'max_response': self.df['response_time'].max()
        }

    def location_analysis(self):
        """Анализ упоминаний городов и областей"""
        locations = {
            # Области
            'области': {
                'Брянская область': ['Брянск', 'Стародуб', 'Климово', 'Навля'],
                'Курская область': ['Курск', 'Обоянь'],
                'Белгородская область': ['Белгород', 'Валуйки', 'Борисовка', 'Ясные Зори'],
                'Ростовская область': ['Ростов', 'Таганрог'],
                'Орловская область': ['Орёл'],
                'Калужская область': ['Калуга'],
                'Краснодарский край': ['Краснодар', 'Ейск', 'Славянск-на-Кубани', 'Крымск', 'Темрюк'],
            },
            # Отдельные территории
            'территории': {
                'ЛДНР': ['ДНР', 'ЛНР', 'Донецк', 'Горловка', 'Енакиево', 'Волноваха', 
                        'Константиновка', 'Покровск', 'Любимовка'],
                'Приазовье': ['Бердянск', 'Мариуполь'],
            }
        }
        
        stats = {
            'области': {},
            'территории': {},
            'города': {}
        }
        
        # Подсчет упоминаний областей и территорий
        for category in ['области', 'территории']:
            for region, cities in locations[category].items():
                # Считаем упоминания региона
                region_mask = self.df['content'].str.contains(region, case=False)
                region_mentions = region_mask.sum()
                
                # Считаем упоминания городов региона
                cities_mentions = {}
                for city in cities:
                    city_mask = self.df['content'].str.contains(city, case=False)
                    city_mentions = city_mask.sum()
                    if city_mentions > 0:
                        cities_mentions[city] = {
                            'total_mentions': city_mentions,
                            'percentage': (city_mentions / len(self.df)) * 100
                        }
                
                if region_mentions > 0 or cities_mentions:
                    stats[category][region] = {
                        'total_mentions': region_mentions,
                        'percentage': (region_mentions / len(self.df)) * 100,
                        'cities': cities_mentions
                    }
        
        return stats

    def generate_plots(self, output_dir='analytics'):
        """Генерация графиков"""
        import os
        os.makedirs(output_dir, exist_ok=True)

        # График постов по датам
        plt.figure(figsize=(15, 5))
        self.posts_by_date().plot(kind='bar')
        plt.title('Посты по датам')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/posts_by_date.png')
        plt.close()

        # График постов по часам
        plt.figure(figsize=(10, 5))
        self.posts_by_hour().plot(kind='bar')
        plt.title('Распределение постов по часам')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/posts_by_hour.png')
        plt.close()

        # Облако слов
        wordcloud = WordCloud(width=1600, height=800, background_color='white').generate(
            ' '.join(self.df['content'])
        )
        plt.figure(figsize=(20,10))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('on')
        plt.tight_layout(pad=0)
        plt.savefig(f'{output_dir}/wordcloud.png')
        plt.close()

    def export_report(self, output_dir='analytics'):
        """Экспорт отчета в markdown в папку analytics"""
        # Создаем папку если её нет
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'analytics_report.md')
        
        basic = self.basic_stats()
        alerts = self.alert_keywords_analysis()
        response = self.response_time_analysis()
        locations = self.location_analysis()
        
        report = f"""# Анализ Telegram постов
        
## 📊 Базовая статистика
- 📝 Всего постов: {basic['Всего постов']}
- 📢 Уникальных каналов: {basic['Уникальных каналов']}
- 📅 Период: с {basic['Первый пост']} по {basic['Последний пост']}
- 📏 Средняя длина поста: {basic['Средняя длина поста (символов)']:.1f} символов

## 📈 Анализ ключевых слов тревоги
"""
        for word, stats in alerts.items():
            report += f"- {word}: {stats['total_mentions']} упоминаний ({stats['percentage']:.1f}%)\n"

        report += "\n## 📍 Анализ локаций\n"
        for category, regions in locations.items():
            report += f"\n### {category.title()}\n"
            for region, data in regions.items():
                report += f"\n#### {region}\n"
                report += f"- Всего упоминаний: {data['total_mentions']} ({data['percentage']:.1f}%)\n"
                
                if data['cities']:
                    report += "- Города:\n"
                    for city, city_data in data['cities'].items():
                        report += f"  - {city}: {city_data['total_mentions']} упоминаний "
                        report += f"({city_data['percentage']:.1f}%)\n"

        report += f"""
## ⏱️ Время обработки
- ⌛ Среднее время: {response['mean_response']}
- 📊 Медианное время: {response['median_response']}
- ⚡ Минимальное время: {response['min_response']}
- 🕒 Максимальное время: {response['max_response']}

## 📊 Графики
- [Посты по датам](posts_by_date.png)
- [Распределение по часам](posts_by_hour.png)
- [Облако слов](wordcloud.png)

---
*Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"📊 Отчет сохранен в {output_file}")

if __name__ == "__main__":
    analyzer = TelegramAnalyzer()
    
    # Генерируем все метрики и графики
    analyzer.generate_plots()
    analyzer.export_report()
    
    print("Анализ завершен! Проверьте папку 'analytics' для графиков и файл 'analytics_report.md' для отчета.")
