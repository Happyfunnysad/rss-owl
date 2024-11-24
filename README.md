# ENG

# RSS Parser for Telegram Channels

A tool for monitoring and archiving posts from Telegram channels via RSS feeds.
Russian alert channels about air raids are used as an example.

## Main Components

### 1. Rsspars.py
The main parser file that:
- Collects data from multiple RSS sources
- Removes duplicates
- Cleans text from unnecessary elements
- Saves posts to SQLite database and text file
- Generates statistics on collected data

### 2. migratedb.py
A utility for:
- Merging multiple databases
- Cleaning text according to specified rules
- Creating backups

### 3. config.json
Configuration file containing:
- List of channels to monitor
- Check intervals
- Text cleaning rules (phrases and patterns for removal)

### 4. analytic-md.py
Module for analyzing collected data from the database. It includes:
- Generating basic post statistics (number of posts, unique channels, average post length, etc.)
- Analyzing post distribution by time and channels
- Calculating keyword mention frequency and reaction time analysis
- Analyzing geographical location mentions
- Generating graphs and Markdown reports for data visualization

## Installation

```bash
git clone https://github.com/Happyfunnysad/rss-owl ;cd rss-owl;pip install -r requirements.txt
```

## Usage

1. Configure channels and rules in `config.json`
2. Run the main parser:
```bash
python rsspars.py
```

3. To merge databases use:
```bash
python migratedb.py
```

4. To analyze data run:
```bash
python analytic-md.py
```

## Data Format

### Database (SQLite)
- `post_id`: Unique post identifier
- `content`: Cleaned post text
- `published_date`: Publication date
- `source_url`: Original post URL

### Text File (tg-posts.txt)
Contains:
- Posts in chronological order
- Summary statistics by channels

## RSS Sources
- tg.i-c-a.su
- rsshub.app
- ru-element.ru
- telegram.meta.ua

## Analytics
- analytic-md.py (data analytics)

## Dependencies
- feedparser
- requests
- pandas
- matplotlib
- seaborn
- wordcloud
- numpy


-----------------------------------------

# Russian

# RSS Parser для Telegram-каналов

Инструмент для мониторинга и архивации постов из Telegram-каналов через RSS-фиды. 
В качестве примера использованы российские каналы оповещения о воздушной тревоге.

## Основные компоненты

### 1. Rsspars.py
Основной файл парсера, который:
- Собирает данные из нескольких RSS-источников
- Удаляет дубликаты
- Очищает текст от лишних элементов
- Сохраняет посты в SQLite базу данных и текстовый файл
- Генерирует статистику по собранным данным

### 2. migratedb.py
Утилита для:
- Объединения нескольких баз данных
- Очистки текста по заданным правилам
- Создания резервных копий

### 3. config.json
Конфигурационный файл, содержащий:
- Список каналов для мониторинга
- Интервалы проверки
- Правила очистки текста (фразы и паттерны для удаления)

### 4. analytic-md.py
Модуль для анализа собранных данных из базы данных. Он включает в себя:
- Генерацию базовой статистики по постам (количество постов, уникальные каналы, средняя длина поста и т.д.)
- Анализ распределения постов по времени и каналам
- Вычисление частоты упоминаний ключевых слов и анализа времени реакции
- Анализ упоминаний географических локаций
- Генерацию графиков и отчетов в формате Markdown для визуализации данных

## Установка

```bash
git clone [url-репозитория]
cd [папка-проекта]
pip install -r requirements.txt
```

## Использование

1. Настройте каналы и правила в `config.json`
2. Запустите основной парсер:
```bash
python rsspars.py
```

3. Для объединения баз данных используйте:
```bash
python migratedb.py
```

4. Для анализа данных запустите:
```bash
python analytic-md.py
```

## Формат данных

### База данных (SQLite)
- `post_id`: Уникальный идентификатор поста
- `content`: Очищенный текст поста
- `published_date`: Дата публикации
- `source_url`: Исходный URL поста

### Текстовый файл (tg-posts.txt)
Содержит:
- Посты в хронологическом порядке
- Сводную статистику по каналам

## RSS источники
- tg.i-c-a.su
- rsshub.app
- ru-element.ru
- telegram.meta.ua

## Аналитика
- analytic-md.py (аналитика данных)     

## Зависимости
- feedparser
- requests
- pandas
- matplotlib
- seaborn
- wordcloud
- numpy


# CN
用于 Telegram 频道的 RSS 解析器

用于通过 RSS 源监控和存档 Telegram 频道帖子的工具。以俄罗斯空袭警报频道为例。

## 主要组件

### 1. Rsspars.py
解析器的主要文件，它可以：
- 从多个 RSS 源收集数据
- 删除重复项
- 清理文本中的多余元素
- 将帖子保存到 SQLite 数据库和文本文件
- 生成收集数据的统计信息

### 2. migratedb.py
用于：
- 合并多个数据库
- 根据指定规则清理文本
- 创建备份

### 3. config.json
配置文件，包含：
- 要监控的频道列表
- 检查间隔
- 文本清理规则（要删除的短语和模式）

### 4. analytic-md.py
用于分析数据库中收集的数据的模块。它包括：
- 生成帖子的基本统计信息（帖子数量、唯一频道、平均帖子长度等）
- 分析按时间和频道的帖子分布
- 计算关键词提及频率和反应时间分析
- 分析地理位置提及
- 生成 Markdown 格式的图表和报告以可视化数据

## 安装

```bash
git clone [仓库 URL]
cd [项目文件夹]
pip install -r requirements.txt
```

## 使用

1. 在 `config.json` 中配置频道和规则
2. 运行主解析器：
```bash
python rsspars.py
```

3. 要合并数据库，请使用：
```bash
python migratedb.py
```

4. 要分析数据，请运行：
```bash
python analytic-md.py
```

## 数据格式

### 数据库（SQLite）
- `post_id`：帖子的唯一标识符
- `content`：清理后的帖子文本
- `published_date`：发布日期
- `source_url`：帖子的原始 URL

### 文本文件（tg-posts.txt）
包含：
- 按时间顺序排列的帖子
- 频道的汇总统计信息

## RSS 源
- tg.i-c-a.su
- rsshub.app
- ru-element.ru
- telegram.meta.ua

## 分析
- analytic-md.py（数据分析）

## 依赖
- feedparser
- requests
- pandas
- matplotlib
- seaborn
- wordcloud
- numpy
