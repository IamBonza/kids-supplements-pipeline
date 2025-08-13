# 🚀 Руководство по развертыванию на Render.com

## 📋 Подготовка к деплою

### 1️⃣ Файлы для развертывания

- ✅ `web_app.py` - веб интерфейс
- ✅ `pipeline_openai_complete.py` - основной pipeline
- ✅ `render.yaml` - конфигурация Render.com
- ✅ `requirements.txt` - зависимости Python
- ✅ `Kids Supplements Keywords.csv` - ключевые слова

### 2️⃣ API ключи (переменные окружения)

- `RAINFOREST_API_KEY` - ваш Rainforest API ключ
- `OPENAI_API_KEY` - ваш OpenAI API ключ

## 🌐 Деплой на Render.com

### Шаг 1: Создание Git репозитория

```bash
git init
git add .
git commit -m "Initial commit: Kids Supplements Pipeline"
git remote add origin YOUR_GITHUB_REPO_URL
git push -u origin main
```

### Шаг 2: Создание сервиса на Render.com

1. Зайдите на [render.com](https://render.com)
2. Нажмите "New +" → "Web Service"
3. Подключите ваш GitHub репозиторий
4. Render автоматически найдет `render.yaml`

### Шаг 3: Настройка переменных окружения

В настройках сервиса добавьте:

- `RAINFOREST_API_KEY` = ваш*rainforest*ключ
- `OPENAI_API_KEY` = ваш*openai*ключ

### Шаг 4: Деплой

- Нажмите "Deploy"
- Дождитесь завершения сборки (~5-10 минут)

## 🎯 Использование

### Web интерфейс

- Откройте URL вашего сервиса (например: https://kids-supplements-pipeline.onrender.com)
- Настройте параметры обработки
- Нажмите "Запустить обработку"
- Следите за прогрессом
- Скачайте результаты

### Возможности

- ⚙️ Настройка лимитов обработки
- 📊 Отслеживание прогресса в реальном времени
- 📥 Скачивание результатов в CSV
- 📈 Просмотр статистики
- 🔄 Resume логика (продолжение при перезапуске)

## 💰 Стоимость на Render.com

### Free Plan (бесплатно)

- ✅ 750 часов в месяц
- ✅ Автоматическое выключение при неактивности
- ✅ 1GB диск для результатов
- ⚠️ Холодный старт (может быть медленным)

### Paid Plans (от $7/месяц)

- ✅ Постоянная работа
- ✅ Быстрый запуск
- ✅ Больше ресурсов

## 🔧 Альтернативные варианты сохранения

Если Google Drive не работает, в `web_app.py` можно добавить:

### Email уведомления

```python
import smtplib
from email.mime.text import MIMEText

def send_results_email(csv_content):
    # Отправка результатов на email
    pass
```

### Dropbox API

```python
import dropbox

def upload_to_dropbox(file_path):
    # Загрузка на Dropbox
    pass
```

### Amazon S3

```python
import boto3

def upload_to_s3(file_path):
    # Загрузка на S3
    pass
```

## ⚠️ Важные замечания

1. **API лимиты**: Следите за расходом Rainforest и OpenAI кредитов
2. **Время выполнения**: Free план может отключаться при долгой обработке
3. **Хранение**: Результаты сохраняются только во время работы сервиса
4. **Безопасность**: API ключи хранятся в переменных окружения

## 🎉 Готово!

После деплоя у вас будет работающий веб сервис для автоматической обработки Kids Supplements данных!
