#!/usr/bin/env python3
"""
Тест подключения к Google Drive API
"""
import os
import sys

def test_google_drive():
    """Проверяет подключение к Google Drive API"""
    print("🔍 Проверка Google Drive API...")
    
    # Проверяем наличие файла credentials
    if not os.path.exists('service-account-key.json'):
        print("❌ Файл service-account-key.json не найден!")
        print("   Скачайте JSON файл из Google Cloud Console")
        print("   Переместите его в эту папку как service-account-key.json")
        return False
    
    try:
        # Импортируем необходимые модули
        from googleapiclient.discovery import build
        from google.oauth2.service_account import Credentials
        
        print("✅ Google API модули импортированы")
        
        # Настраиваем credentials
        SCOPES = ['https://www.googleapis.com/auth/drive.file']
        credentials = Credentials.from_service_account_file(
            'service-account-key.json', 
            scopes=SCOPES
        )
        
        print("✅ Credentials загружены")
        
        # Создаем сервис
        service = build('drive', 'v3', credentials=credentials)
        
        print("✅ Google Drive service создан")
        
        # Тестируем доступ
        results = service.files().list(pageSize=1).execute()
        
        print("✅ Подключение к Google Drive успешно!")
        print(f"🎉 API работает! Можно запускать pipeline.")
        
        return True
        
    except ImportError as e:
        print(f"❌ Отсутствуют зависимости: {e}")
        print("   Установите: pip install google-api-python-client google-auth")
        return False
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        print("   Проверьте правильность настройки credentials")
        return False

if __name__ == "__main__":
    success = test_google_drive()
    sys.exit(0 if success else 1)
