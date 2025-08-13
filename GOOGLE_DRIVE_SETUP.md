# 🔧 Настройка Google Drive API

Пошаговая инструкция для настройки автоматического сохранения результатов в Google Drive.

## 📋 Шаг 1: Создание Google Cloud проекта

1. **Перейдите в Google Cloud Console**

   - Откройте [console.cloud.google.com](https://console.cloud.google.com)
   - Войдите в свой Google аккаунт

2. **Создайте новый проект**
   - Нажмите на выпадающий список проектов (вверху)
   - Выберите "New Project" / "Новый проект"
   - Название: `Kids Supplements Pipeline`
   - Нажмите "Create" / "Создать"

## 📋 Шаг 2: Включение Google Drive API

1. **Откройте библиотеку API**

   - В левом меню: `APIs & Services` → `Library`
   - Или прямая ссылка: [console.cloud.google.com/apis/library](https://console.cloud.google.com/apis/library)

2. **Найдите и включите Google Drive API**
   - В поиске введите: `Google Drive API`
   - Кликните на результат
   - Нажмите `Enable` / `Включить`

## 📋 Шаг 3: Создание Service Account

1. **Перейдите в Credentials**

   - Левое меню: `APIs & Services` → `Credentials`
   - Или: [console.cloud.google.com/apis/credentials](https://console.cloud.google.com/apis/credentials)

2. **Создайте Service Account**

   - Нажмите `+ CREATE CREDENTIALS` → `Service account`
   - **Service account name**: `kids-supplements-uploader`
   - **Service account ID**: `kids-supplements-uploader` (автоматически)
   - **Description**: `Загрузка CSV результатов в Google Drive`
   - Нажмите `CREATE AND CONTINUE`

3. **Настройте роли (опционально)**
   - Можете пропустить, нажмите `CONTINUE`
   - И еще раз `DONE`

## 📋 Шаг 4: Создание ключа

1. **Найдите созданный Service Account**

   - В списке Credentials найдите `kids-supplements-uploader@...`
   - Кликните на него

2. **Создайте JSON ключ**

   - Перейдите на вкладку `Keys`
   - Нажмите `ADD KEY` → `Create new key`
   - Выберите тип: `JSON`
   - Нажмите `CREATE`

3. **Скачайте файл**
   - Автоматически скачается файл `kids-supplements-pipeline-xxxx.json`
   - **ВАЖНО**: Сохраните этот файл в безопасном месте!

## 📋 Шаг 5: Настройка доступа к Google Drive

1. **Откройте скачанный JSON файл**

   - Найдите поле `"client_email"`
   - Скопируйте email адрес (например: `kids-supplements-uploader@kids-supplements-pipeline.iam.gserviceaccount.com`)

2. **Дайте доступ к вашему Google Drive**
   - Откройте [drive.google.com](https://drive.google.com)
   - Создайте папку для результатов (например: "Kids Supplements Results")
   - Кликните правой кнопкой на папку → `Share` / `Поделиться`
   - Вставьте скопированный email
   - Выберите роль: `Editor` / `Редактор`
   - Нажмите `Send` / `Отправить`

## 📋 Шаг 6: Установка зависимостей

```bash
# Обновляем requirements.txt
pip install google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2
```

Или добавьте в `requirements.txt`:

```
google-api-python-client>=2.0.0
google-auth>=2.0.0
google-auth-oauthlib>=1.0.0
google-auth-httplib2>=0.1.0
```

## 📋 Шаг 7: Использование

1. **Поместите JSON файл в проект**

   ```bash
   # Переименуйте скачанный файл
   mv ~/Downloads/kids-supplements-pipeline-xxxx.json ./google_credentials.json
   ```

2. **Запустите pipeline**

   ```bash
   python3 pipeline_with_google_drive.py \
     --rainforest-key YOUR_RAINFOREST_KEY \
     --openai-key YOUR_OPENAI_KEY \
     --google-credentials ./google_credentials.json \
     --detail-limit 5
   ```

3. **Проверьте результаты**
   - Откройте [drive.google.com](https://drive.google.com)
   - Найдите папку "Kids Supplements Results"
   - Там будут CSV файлы с результатами!

## 🔒 Безопасность

⚠️ **ВАЖНО**:

- Файл `google_credentials.json` содержит секретные ключи
- НЕ добавляйте его в git репозиторий
- НЕ делитесь им с другими
- Храните в безопасном месте

Добавьте в `.gitignore`:

```
google_credentials.json
*.json
```

## 🌐 Для Render.com деплоя

1. **Загрузите credentials как переменную окружения**

   ```bash
   # Конвертируйте JSON в base64
   cat google_credentials.json | base64 -w 0
   ```

2. **В Render.com добавьте переменную**

   - Key: `GOOGLE_CREDENTIALS_BASE64`
   - Value: (результат команды выше)

3. **Модифицируйте код для чтения из переменной**

   ```python
   import base64
   import json
   import tempfile

   # Декодируем credentials из переменной окружения
   credentials_base64 = os.getenv('GOOGLE_CREDENTIALS_BASE64')
   credentials_json = base64.b64decode(credentials_base64).decode('utf-8')

   # Создаем временный файл
   with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
       f.write(credentials_json)
       credentials_path = f.name
   ```

## ✅ Проверка настройки

После настройки запустите тест:

```python
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# Проверяем подключение
credentials = Credentials.from_service_account_file('google_credentials.json')
service = build('drive', 'v3', credentials=credentials)

# Получаем список файлов
results = service.files().list(pageSize=10).execute()
print("✅ Google Drive API работает!")
```

## 🆘 Проблемы и решения

### ❌ "Permission denied"

- Убедитесь что дали доступ Service Account к папке
- Проверьте что email скопирован правильно

### ❌ "Invalid credentials"

- Проверьте путь к JSON файлу
- Убедитесь что файл не поврежден

### ❌ "API not enabled"

- Включите Google Drive API в Google Cloud Console
- Подождите несколько минут для активации

### ❌ "Quota exceeded"

- Google Drive API имеет лимиты запросов
- Добавьте паузы между загрузками файлов

## 🎯 Готово!

После настройки ваш pipeline будет автоматически:

- ✅ Сохранять результаты локально
- ✅ Загружать в Google Drive
- ✅ Создавать ссылки для скачивания
- ✅ Обновлять файлы при повторном запуске

Файлы будут доступны в Google Drive даже после остановки сервера! 🚀
