#!/usr/bin/env python3
"""
Production Pipeline с автоматическим сохранением в Google Drive
Загружает CSV результаты прямо в ваш Google Drive
"""

import pandas as pd
import requests
import easyocr
from PIL import Image
import io
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import argparse
import sys
import json
import time
from datetime import datetime
import os
import re
import openai
import base64
import logging
from pathlib import Path

# Google Drive API
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials
import tempfile

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoogleDriveUploader:
    """Класс для загрузки файлов в Google Drive"""
    
    def __init__(self, credentials_path: str, folder_name: str = "Kids Supplements Results"):
        """
        Инициализация Google Drive API
        
        Args:
            credentials_path: Путь к файлу credentials.json
            folder_name: Название папки в Google Drive для результатов
        """
        self.credentials_path = credentials_path
        self.folder_name = folder_name
        self.service = None
        self.folder_id = None
        
        self._initialize_drive_service()
        self._create_or_get_folder()
        
    def _initialize_drive_service(self):
        """Инициализация Google Drive API сервиса"""
        try:
            # Области доступа для Google Drive
            SCOPES = ['https://www.googleapis.com/auth/drive.file']
            
            # Загружаем credentials
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Файл credentials не найден: {self.credentials_path}")
            
            credentials = Credentials.from_service_account_file(
                self.credentials_path, 
                scopes=SCOPES
            )
            
            # Создаем сервис
            self.service = build('drive', 'v3', credentials=credentials)
            logger.info("✅ Google Drive API инициализирован")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Google Drive API: {e}")
            raise
    
    def _create_or_get_folder(self):
        """Создает или находит папку для результатов"""
        try:
            # Ищем существующую папку
            query = f"name='{self.folder_name}' and mimeType='application/vnd.google-apps.folder'"
            results = self.service.files().list(q=query).execute()
            folders = results.get('files', [])
            
            if folders:
                self.folder_id = folders[0]['id']
                logger.info(f"📁 Найдена существующая папка: {self.folder_name}")
            else:
                # Создаем новую папку
                folder_metadata = {
                    'name': self.folder_name,
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                
                folder = self.service.files().create(body=folder_metadata).execute()
                self.folder_id = folder.get('id')
                logger.info(f"📁 Создана новая папка: {self.folder_name}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка работы с папкой: {e}")
            self.folder_id = None
    
    def upload_file(self, file_path: str, drive_filename: str = None) -> Optional[str]:
        """
        Загружает файл в Google Drive
        
        Args:
            file_path: Локальный путь к файлу
            drive_filename: Имя файла в Drive (по умолчанию как локальный)
            
        Returns:
            URL файла в Google Drive или None при ошибке
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"❌ Файл не найден: {file_path}")
                return None
                
            if not drive_filename:
                drive_filename = os.path.basename(file_path)
            
            # Метаданные файла
            file_metadata = {
                'name': drive_filename,
                'parents': [self.folder_id] if self.folder_id else []
            }
            
            # Загружаем файл
            media = MediaFileUpload(file_path, resumable=True)
            
            # Проверяем, существует ли файл с таким именем
            existing_file = self._find_existing_file(drive_filename)
            
            if existing_file:
                # Обновляем существующий файл
                file = self.service.files().update(
                    fileId=existing_file['id'],
                    media_body=media
                ).execute()
                logger.info(f"🔄 Обновлен файл в Google Drive: {drive_filename}")
            else:
                # Создаем новый файл
                file = self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                logger.info(f"📤 Загружен новый файл в Google Drive: {drive_filename}")
            
            # Получаем ссылку на файл
            file_id = file.get('id')
            
            # Делаем файл доступным для просмотра (опционально)
            self.service.permissions().create(
                fileId=file_id,
                body={'role': 'reader', 'type': 'anyone'}
            ).execute()
            
            # Возвращаем ссылку
            drive_url = f"https://drive.google.com/file/d/{file_id}/view"
            logger.info(f"🔗 Ссылка на файл: {drive_url}")
            
            return drive_url
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки в Google Drive: {e}")
            return None
    
    def _find_existing_file(self, filename: str) -> Optional[Dict]:
        """Ищет существующий файл в папке"""
        try:
            parent_query = f"'{self.folder_id}' in parents" if self.folder_id else ""
            name_query = f"name='{filename}'"
            
            query = f"{name_query} and {parent_query}" if parent_query else name_query
            
            results = self.service.files().list(q=query).execute()
            files = results.get('files', [])
            
            return files[0] if files else None
            
        except Exception as e:
            logger.error(f"❌ Ошибка поиска файла: {e}")
            return None
    
    def get_folder_url(self) -> str:
        """Возвращает ссылку на папку с результатами"""
        if self.folder_id:
            return f"https://drive.google.com/drive/folders/{self.folder_id}"
        return "Папка не создана"


class OpenAISupplementFactsAnalyzer:
    """AI анализ через OpenAI Vision API (копия из предыдущего pipeline)"""
    
    def __init__(self, api_key: str):
        self.client = openai.OpenAI(api_key=api_key)
        self.cache = {}
        self.total_cost = 0.0
        self.request_count = 0
        
        # Создаем папку для кэша
        self.cache_dir = Path("cache")
        self.cache_dir.mkdir(exist_ok=True)
        
        logger.info("✅ OpenAI Vision API инициализирован")
    
    def _extract_image_id(self, url: str) -> str:
        """Извлекает уникальный ID изображения из Amazon URL"""
        try:
            match = re.search(r'/([A-Z0-9]{10,11})[._-]', url)
            return match.group(1) if match else ""
        except:
            return ""
    
    def _download_image(self, url: str, max_retries: int = 3) -> bytes:
        """Скачивает изображение с retry логикой"""
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                response.raise_for_status()
                
                if len(response.content) < 1000:
                    raise ValueError("Изображение слишком маленькое")
                
                return response.content
                
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1}/{max_retries} загрузки изображения не удалась: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
    
    def _encode_image_base64(self, image_bytes: bytes) -> str:
        """Кодирует изображение в base64"""
        return base64.b64encode(image_bytes).decode('utf-8')
    
    def _save_cache(self, image_id: str, data: dict):
        """Сохраняет результат в кэш"""
        if image_id:
            cache_file = self.cache_dir / f"{image_id}.json"
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _load_cache(self, image_id: str) -> Optional[dict]:
        """Загружает результат из кэша"""
        if not image_id:
            return None
        
        cache_file = self.cache_dir / f"{image_id}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return None
    
    def analyze_supplement_facts(self, url: str, product_name: str = "", brand: str = "") -> tuple:
        """
        Основной метод анализа Supplement Facts через OpenAI Vision
        Возвращает: (ingredients, dosages, age_group, form)
        """
        logger.info(f"🤖 Анализ Supplement Facts: {url[:50]}...")
        
        # Проверяем кэш
        image_id = self._extract_image_id(url)
        cached_result = self._load_cache(image_id)
        
        if cached_result:
            logger.info(f"✅ Найден в кэше: {image_id}")
            return (
                cached_result.get("ingredients", ""),
                cached_result.get("dosages", ""),
                cached_result.get("age_group", ""),
                cached_result.get("form", "")
            )
        
        try:
            # Скачиваем и кодируем изображение
            image_bytes = self._download_image(url)
            image_base64 = self._encode_image_base64(image_bytes)
            
            # Системный промпт для максимальной точности
            system_prompt = """Ты эксперт по анализу этикеток пищевых добавок и витаминов. 

Проанализируй изображение Supplement Facts и извлеки следующую информацию:

1. ИНГРЕДИЕНТЫ: Все активные ингредиенты из таблицы Supplement Facts
2. ДОЗИРОВКИ: Точные дозировки с единицами измерения для каждого ингредиента
3. ВОЗРАСТНАЯ ГРУППА: Информация о возрасте (2+, 4+, 6+ и т.д.)
4. ФОРМА ВЫПУСКА: Тип продукта (Gummies, Chewable, Tablets, Capsules, Liquid, Drops, Powder, Softgels)

Верни результат СТРОГО в JSON формате:
{
  "ingredients": "список всех ингредиентов через запятую",
  "dosages": "пары 'Ингредиент: дозировка единица' через точку с запятой",
  "age_group": "возрастная группа в формате X+",
  "form": "точная форма выпуска"
}

ВАЖНО:
- Если информация не найдена, оставь пустую строку ""
- Включи ВСЕ ингредиенты из таблицы Supplement Facts
- Сохрани точные дозировки с единицами (mg, mcg, IU, etc.)
- НЕ добавляй комментарии, ТОЛЬКО чистый JSON"""
            
            # Запрос к OpenAI Vision
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": system_prompt
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": f"Анализируй Supplement Facts для: {brand} {product_name}"
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{image_base64}",
                                    "detail": "high"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=1500,
                temperature=0
            )
            
            self.request_count += 1
            self.total_cost += 0.02
            
            # Обработка ответа
            content = response.choices[0].message.content.strip()
            
            # Очистка от markdown
            if content.startswith("```json"):
                content = content[7:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            # Парсинг JSON
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                logger.error(f"Ошибка парсинга JSON от OpenAI: {content}")
                return "", "", "", ""
            
            ingredients = data.get("ingredients", "")
            dosages = data.get("dosages", "")
            age_group = data.get("age_group", "")
            form = data.get("form", "")
            
            # Сохраняем в кэш
            cache_data = {
                "ingredients": ingredients,
                "dosages": dosages,
                "age_group": age_group,
                "form": form,
                "analyzed_at": datetime.now().isoformat(),
                "product_name": product_name,
                "brand": brand
            }
            self._save_cache(image_id, cache_data)
            
            logger.info(f"✅ OpenAI анализ завершен. Ингредиентов: {len(ingredients.split(',')) if ingredients else 0}")
            return ingredients, dosages, age_group, form
            
        except Exception as e:
            logger.error(f"❌ Ошибка OpenAI анализа: {e}")
            return "", "", "", ""
    
    def get_stats(self) -> dict:
        """Возвращает статистику использования OpenAI"""
        return {
            'requests': self.request_count,
            'estimated_cost': self.total_cost
        }


class GoogleDrivePipelineProcessor:
    """Pipeline процессор с автоматической загрузкой в Google Drive"""
    
    def __init__(self, rainforest_api_key: str, openai_api_key: str, 
                 google_credentials_path: str, output_dir: str = "output"):
        self.rainforest_api_key = rainforest_api_key
        self.base_url = "https://api.rainforestapi.com/request"
        
        # Создаем выходную директорию
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Инициализация компонентов
        logger.info("🔍 Инициализация OCR...")
        self.ocr_reader = easyocr.Reader(['en'], gpu=False, verbose=False)
        
        logger.info("🤖 Инициализация OpenAI Vision...")
        self.ai_analyzer = OpenAISupplementFactsAnalyzer(openai_api_key)
        
        logger.info("☁️ Инициализация Google Drive...")
        self.drive_uploader = GoogleDriveUploader(google_credentials_path)
        
        # Сессия для HTTP запросов
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        })
        
        # Ключевые слова для поиска Supplement Facts
        self.supplement_keywords = [
            'supplement facts', 'supplement fact', 'nutrition facts',
            'serving size', 'servings per container', 'amount per serving',
            'daily value', '% daily value', '%dv', 'vitamin', 'mineral'
        ]
        
        # Статистика
        self.stats = {
            'total_api_calls': 0,
            'search_calls': 0,
            'product_calls': 0,
            'rainforest_credits': 0,
            'products_found': 0,
            'supplement_facts_found': 0,
            'successful_extractions': 0,
            'drive_uploads': 0,
            'start_time': datetime.now()
        }
        
        logger.info("✅ Google Drive Pipeline инициализирован")
        logger.info(f"📁 Папка в Google Drive: {self.drive_uploader.get_folder_url()}")

    def load_existing_data(self, output_file: str) -> pd.DataFrame:
        """Загружает существующие данные или создает пустую таблицу"""
        try:
            df = pd.read_csv(output_file, dtype=str).fillna("")
            logger.info(f"📂 Загружена существующая таблица: {len(df)} записей")
            return df
        except FileNotFoundError:
            logger.info(f"📂 Файл {output_file} не найден, создается новая таблица")
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"⚠️ Ошибка загрузки {output_file}: {e}")
            return pd.DataFrame()
    
    def check_search_term_processed(self, df: pd.DataFrame, search_term: str) -> bool:
        """Проверяет, обработан ли уже данный поисковый запрос"""
        if df.empty or 'Search Term' not in df.columns:
            return False
        
        existing_records = df[df['Search Term'] == search_term]
        if len(existing_records) > 0:
            logger.info(f"🔍 Найдено {len(existing_records)} записей для '{search_term}'")
            return True
        return False
    
    def is_product_processed(self, row) -> bool:
        """Проверяет, обработан ли уже товар"""
        bsr_filled = bool(str(row.get('BSR', '')).strip() and str(row.get('BSR', '')).strip() != 'nan')
        category_filled = bool(str(row.get('Категория', '')).strip())
        
        return bsr_filled or category_filled
    
    def create_products_dataframe(self, products: List[Dict], search_term: str, existing_df: pd.DataFrame = None) -> pd.DataFrame:
        """Создает DataFrame с товарами или дополняет существующий"""
        
        logger.info(f"📦 Обработка товаров ({len(products)} записей) для '{search_term}'")
        
        if existing_df is not None and not existing_df.empty:
            df = existing_df.copy()
            start_index = len(df) + 1
        else:
            df = pd.DataFrame()
            start_index = 1
        
        records = []
        for i, product in enumerate(products, start_index):
            price_raw = ""
            if product['price']:
                price_raw = product['price'].get('raw', '')
            
            bsr_info = ""
            if product['bestsellers_rank']:
                bsr_list = []
                for rank in product['bestsellers_rank']:
                    category = rank.get('category', 'Unknown')
                    rank_num = rank.get('rank', 'N/A')
                    bsr_list.append(f"{category}: #{rank_num}")
                bsr_info = "; ".join(bsr_list)
            
            record = {
                '№': i,
                'Search Term': search_term,
                'ASIN': product['asin'],
                'Название продукта (Title)': product['title'],
                'Бренд': product['brand'],
                'Цена (USD)': price_raw,
                'Возрастная группа': '',
                'Форма выпуска': '',
                'Все ингредиенты (из Supplement Facts)': '',
                'Дозировки (мг/ед.)': '',
                'Claims (sugar free, organic и т.д.)': '',
                'Кол-во отзывов': product['ratings_total'],
                'Рейтинг': product['rating'],
                'BSR': bsr_info,
                'Категория': '',
                'Ссылка на товар': product['link'],
                'Ссылка на Supplement Facts (изображение)': ''
            }
            records.append(record)
        
        new_df = pd.DataFrame(records)
        if not df.empty:
            df = pd.concat([df, new_df], ignore_index=True)
        else:
            df = new_df
        
        logger.info(f"✅ Таблица обновлена: {len(new_df)} новых записей, всего {len(df)} записей")
        return df
    
    def process_detailed_products(self, df: pd.DataFrame, limit: int = None) -> pd.DataFrame:
        """Обрабатывает детальные данные товаров с OpenAI Vision"""
        
        total_products = len(df) if limit is None else min(limit, len(df))
        
        logger.info(f"\n🔍 Детальная обработка товаров (лимит: {total_products})")
        
        processed_count = 0
        skipped_count = 0
        
        for i in range(len(df)):
            if limit and processed_count >= limit:
                logger.info(f"🛑 Достигнут лимит обработки: {limit}")
                break
                
            asin = df.iloc[i]['ASIN']
            
            if self.is_product_processed(df.iloc[i]):
                logger.info(f"⏭️  Товар {i+1}: {asin} - уже обработан")
                skipped_count += 1
                continue
            
            logger.info(f"\n📦 Товар {i+1}: {asin}")
            
            details = self.get_product_details(asin)
            
            if not details['success']:
                logger.warning(f"      ❌ Ошибка получения данных: {details.get('error', 'Unknown')}")
                continue
            
            product_data = details['product']
            
            # Обновляем основные данные
            if not df.iloc[i]['Бренд'] and product_data.get('brand'):
                df.iloc[i, df.columns.get_loc('Бренд')] = product_data['brand']
            
            # Обновляем категорию
            categories = product_data.get('categories', [])
            if categories:
                category_names = [cat.get('name', '') for cat in categories]
                df.iloc[i, df.columns.get_loc('Категория')] = ' > '.join(category_names)
            
            # Обновляем BSR из детальных данных
            bestsellers_rank = product_data.get('bestsellers_rank', [])
            if bestsellers_rank:
                first_rank = bestsellers_rank[0]
                bsr_number = first_rank.get('rank', 'N/A')
                bsr_category = first_rank.get('category', 'Unknown')
                
                df.iloc[i, df.columns.get_loc('BSR')] = str(bsr_number)
                logger.info(f"      📊 BSR: #{bsr_number} ({bsr_category})")
            
            # Ищем Supplement Facts
            supplement_image = self.find_supplement_facts_image(product_data)
            
            if supplement_image:
                df.iloc[i, df.columns.get_loc('Ссылка на Supplement Facts (изображение)')] = supplement_image
                logger.info(f"      🎯 Supplement Facts найден!")
                
                # OpenAI Vision анализ
                try:
                    product_title = df.iloc[i]['Название продукта (Title)']
                    brand = df.iloc[i]['Бренд']
                    
                    ingredients, dosages, age_group, form = self.ai_analyzer.analyze_supplement_facts(
                        supplement_image, product_title, brand
                    )
                    
                    self.stats['successful_extractions'] += 1
                    
                    # Заполняем извлеченные данные
                    df.iloc[i, df.columns.get_loc('Все ингредиенты (из Supplement Facts)')] = ingredients
                    df.iloc[i, df.columns.get_loc('Дозировки (мг/ед.)')] = dosages
                    
                    if age_group:
                        df.iloc[i, df.columns.get_loc('Возрастная группа')] = age_group
                        logger.info(f"         📅 Возраст: {age_group}")
                    
                    if form:
                        df.iloc[i, df.columns.get_loc('Форма выпуска')] = form
                        logger.info(f"         💊 Форма: {form}")
                    
                    logger.info(f"      ✅ OpenAI Vision анализ завершен!")
                    
                except Exception as e:
                    logger.error(f"      ❌ Ошибка OpenAI анализа: {e}")
                
            else:
                logger.warning(f"      ❌ Supplement Facts не найден")
            
            processed_count += 1
            time.sleep(1)
        
        logger.info(f"\n📊 Обработка завершена: {processed_count} обработано, {skipped_count} пропущено")
        return df
    
    def get_product_details(self, asin: str) -> Dict:
        """Получение детальных данных товара"""
        
        logger.info(f"   📦 Получение данных: {asin}")
        
        params = {
            'api_key': self.rainforest_api_key,
            'type': 'product',
            'amazon_domain': 'amazon.com',
            'asin': asin
        }
        
        try:
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            self.stats['total_api_calls'] += 1
            self.stats['product_calls'] += 1
            
            if 'request_info' in data:
                credits_used = data['request_info'].get('credits_used_this_request', 1)
                self.stats['rainforest_credits'] += credits_used
                logger.info(f"      💳 Кредитов: {credits_used}")
            
            if not data.get('request_info', {}).get('success', False):
                return {'success': False, 'error': 'API request failed'}
            
            product_data = data.get('product', {})
            
            if not product_data:
                return {'success': False, 'error': 'Product data not found'}
            
            logger.info(f"      ✅ Данные получены")
            return {'success': True, 'product': product_data}
            
        except Exception as e:
            logger.error(f"      ❌ Ошибка: {e}")
            return {'success': False, 'error': str(e)}
    
    def analyze_image_for_supplement_facts(self, image_url: str) -> Dict:
        """Анализ изображения на Supplement Facts (OCR валидация)"""
        result = {
            'url': image_url,
            'accessible': False,
            'contains_supplement_facts': False,
            'confidence': 0.0,
            'keywords_found': [],
            'error': None
        }
        
        try:
            response = requests.get(image_url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            if response.status_code != 200:
                result['error'] = f"HTTP {response.status_code}"
                return result
            
            result['accessible'] = True
            
            # OCR анализ
            image = Image.open(io.BytesIO(response.content))
            
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            image_array = np.array(image)
            ocr_results = self.ocr_reader.readtext(image_array)
            
            # Извлекаем весь текст
            full_text = ' '.join([item[1] for item in ocr_results]).lower()
            
            # Подсчитываем релевантные ключевые слова
            found_keywords = []
            confidence_score = 0.0
            
            for keyword in self.supplement_keywords:
                if keyword in full_text:
                    found_keywords.append(keyword)
                    if keyword == 'supplement facts':
                        confidence_score += 0.5
                    elif keyword in ['serving size', 'servings per container']:
                        confidence_score += 0.2
                    elif keyword in ['daily value', '% daily value', '%dv']:
                        confidence_score += 0.15
                    else:
                        confidence_score += 0.05
            
            result['keywords_found'] = found_keywords
            result['confidence'] = min(confidence_score, 1.0)
            result['contains_supplement_facts'] = confidence_score >= 0.3
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def find_supplement_facts_image(self, product_data: Dict) -> Optional[str]:
        """Поиск изображения с Supplement Facts"""
        
        images = product_data.get('images', [])
        if not images:
            return None
            
        logger.info(f"      🔍 Анализ {len(images)} изображений...")
        
        best_image = None
        best_confidence = 0.0
        
        for image_data in images:
            image_url = image_data.get('link', '')
            
            if not image_url:
                continue
            
            analysis = self.analyze_image_for_supplement_facts(image_url)
            
            if analysis['accessible'] and analysis['contains_supplement_facts']:
                logger.info(f"         🎉 НАЙДЕН! Уверенность: {analysis['confidence']:.2f}")
                
                if analysis['confidence'] > best_confidence:
                    best_confidence = analysis['confidence']
                    best_image = image_url
        
        if best_image:
            self.stats['supplement_facts_found'] += 1
            return best_image
        
        return None

    # Методы поиска и обработки (копируем из предыдущего pipeline)
    def load_keywords(self, keywords_file: str) -> List[str]:
        """Загружает ключевые запросы из CSV файла"""
        logger.info(f"📂 Загрузка ключевых запросов: {keywords_file}")
        
        try:
            if not os.path.exists(keywords_file):
                raise FileNotFoundError(f"Файл не найден: {keywords_file}")
            
            df = pd.read_csv(keywords_file)
            keywords = df.iloc[:, 1].dropna().tolist()
            
            if not keywords:
                raise ValueError("Ключевые запросы не найдены")
            
            logger.info(f"✅ Загружено {len(keywords)} ключевых запросов")
            return keywords
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки ключевых запросов: {e}")
            return []

    def search_products_multiple_pages(self, search_term: str, max_pages: int = 2) -> List[Dict]:
        """Поиск товаров с улучшенной обработкой ошибок"""
        
        logger.info(f"🔍 Поиск товаров: '{search_term}' (до {max_pages} страниц)")
        
        all_products = []
        
        for page in range(1, max_pages + 1):
            logger.info(f"   📄 Обработка страницы {page}/{max_pages}")
            
            params = {
                'api_key': self.rainforest_api_key,
                'type': 'search',
                'amazon_domain': 'amazon.com',
                'search_term': search_term,
                'page': page
            }
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.session.get(self.base_url, params=params, timeout=60)
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    self.stats['total_api_calls'] += 1
                    self.stats['search_calls'] += 1
                    
                    if 'request_info' in data:
                        credits = data['request_info'].get('credits_used_this_request', 1)
                        self.stats['rainforest_credits'] += credits
                        logger.info(f"      💳 Использовано кредитов: {credits}")
                    
                    if not data.get('request_info', {}).get('success', False):
                        error_msg = data.get('request_info', {}).get('message', 'Unknown error')
                        logger.error(f"      ❌ API ошибка: {error_msg}")
                        break
                    
                    search_results = data.get('search_results', [])
                    logger.info(f"      ✅ Найдено товаров: {len(search_results)}")
                    
                    # Обработка результатов
                    for result in search_results:
                        product = {
                            'asin': result.get('asin', ''),
                            'title': result.get('title', ''),
                            'link': result.get('link', ''),
                            'rating': result.get('rating', 0),
                            'ratings_total': result.get('ratings_total', 0),
                            'price': result.get('price', {}),
                            'image': result.get('image', ''),
                            'is_prime': result.get('is_prime', False),
                            'brand': result.get('brand', ''),
                            'bestsellers_rank': result.get('bestsellers_rank', []),
                            'search_term': search_term,
                            'page_found': page
                        }
                        all_products.append(product)
                    
                    break  # Успешный запрос
                    
                except Exception as e:
                    logger.warning(f"      ⚠️ Попытка {attempt + 1}/{max_retries} не удалась: {e}")
                    if attempt == max_retries - 1:
                        logger.error(f"      ❌ Все попытки исчерпаны для страницы {page}")
                    else:
                        time.sleep(2 ** attempt)
            
            if page < max_pages:
                time.sleep(2)
        
        self.stats['products_found'] += len(all_products)
        logger.info(f"   🎯 Итого найдено товаров: {len(all_products)}")
        return all_products

    def save_results_to_drive(self, df: pd.DataFrame, filename: str) -> str:
        """Сохраняет результаты локально и загружает в Google Drive"""
        try:
            # Создаем timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Локальное сохранение
            local_file = self.output_dir / filename
            df.to_csv(local_file, index=False, encoding='utf-8')
            
            # Загрузка в Google Drive
            drive_filename = f"kids_supplements_{timestamp}.csv"
            drive_url = self.drive_uploader.upload_file(str(local_file), drive_filename)
            
            if drive_url:
                self.stats['drive_uploads'] += 1
                logger.info(f"☁️ Файл загружен в Google Drive: {drive_url}")
                
                # Также загружаем основной файл (без timestamp)
                self.drive_uploader.upload_file(str(local_file), "kids_supplements_latest.csv")
                
                return drive_url
            else:
                logger.error("❌ Ошибка загрузки в Google Drive")
                return ""
                
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения результатов: {e}")
            return ""

    def print_final_stats(self):
        """Выводит финальную статистику включая Google Drive"""
        duration = datetime.now() - self.stats['start_time']
        openai_stats = self.ai_analyzer.get_stats()
        
        print("\n" + "=" * 80)
        print("📊 ФИНАЛЬНАЯ СТАТИСТИКА GOOGLE DRIVE PIPELINE")
        print("=" * 80)
        print(f"🕐 Время работы: {duration}")
        print(f"📞 Всего API вызовов: {self.stats['total_api_calls']}")
        print(f"🔍 Поисковых запросов: {self.stats['search_calls']}")
        print(f"📦 Запросов товаров: {self.stats['product_calls']}")
        print(f"💳 Rainforest кредитов: {self.stats['rainforest_credits']}")
        print(f"🤖 OpenAI запросов: {openai_stats['requests']}")
        print(f"💰 Примерная стоимость OpenAI: ${openai_stats['estimated_cost']:.2f}")
        print(f"☁️ Загрузок в Google Drive: {self.stats['drive_uploads']}")
        print(f"🛍️ Найдено товаров: {self.stats['products_found']}")
        print(f"🎯 Найдено Supplement Facts: {self.stats['supplement_facts_found']}")
        print(f"✅ Успешных извлечений: {self.stats['successful_extractions']}")
        print(f"📁 Папка Google Drive: {self.drive_uploader.get_folder_url()}")
        print("=" * 80)


def main():
    """Главная функция Google Drive pipeline"""
    parser = argparse.ArgumentParser(description='Pipeline с автоматической загрузкой в Google Drive')
    parser.add_argument('--rainforest-key', required=True, help='Rainforest API ключ')
    parser.add_argument('--openai-key', required=True, help='OpenAI API ключ')
    parser.add_argument('--google-credentials', required=True, help='Путь к Google credentials.json')
    parser.add_argument('--keywords-file', default='Kids Supplements Keywords.csv', help='Файл с ключевыми запросами')
    parser.add_argument('--output-dir', default='output', help='Директория для временных файлов')
    parser.add_argument('--max-pages', type=int, default=2, help='Максимум страниц поиска')
    parser.add_argument('--detail-limit', type=int, default=10, help='Лимит товаров для детальной обработки')
    parser.add_argument('--keyword-limit', type=int, default=None, help='Лимит ключевых запросов')
    
    args = parser.parse_args()
    
    logger.info("🚀 ЗАПУСК GOOGLE DRIVE PIPELINE")
    logger.info("=" * 80)
    
    try:
        # Инициализация процессора
        processor = GoogleDrivePipelineProcessor(
            args.rainforest_key, 
            args.openai_key,
            args.google_credentials,
            args.output_dir
        )
        
        # Загружаем ключевые запросы
        keywords = processor.load_keywords(args.keywords_file)
        if not keywords:
            logger.error("❌ Не удалось загрузить ключевые запросы")
            return 1
        
        if args.keyword_limit:
            keywords = keywords[:args.keyword_limit]
            logger.info(f"🎯 Ограничение: будет обработано {len(keywords)} ключевых запросов")
        
        # Загружаем существующие данные
        output_file = 'kids_supplements.csv'
        existing_df = processor.load_existing_data(output_file)
        
        # Обрабатываем ключевые запросы
        df = existing_df
        
        for keyword_idx, search_term in enumerate(keywords):
            logger.info(f"\n🎯 Обработка ключевого запроса {keyword_idx+1}/{len(keywords)}: '{search_term}'")
            
            if not existing_df.empty and processor.check_search_term_processed(existing_df, search_term):
                logger.info(f"⏭️  Поисковый запрос '{search_term}' уже обработан, пропускаем поиск")
            else:
                products = processor.search_products_multiple_pages(search_term, args.max_pages)
                
                if not products:
                    logger.warning(f"❌ Товары не найдены для '{search_term}'")
                    continue
                
                df = processor.create_products_dataframe(products, search_term, df)
                
                # Сохраняем промежуточные результаты в Google Drive
                drive_url = processor.save_results_to_drive(df, output_file)
                logger.info(f"💾 Промежуточное сохранение: {len(df)} записей → {drive_url}")
        
        if df.empty:
            logger.error("❌ Нет данных для обработки")
            return 1
        
        # Обрабатываем детальные данные (Supplement Facts с OpenAI)
        df = processor.process_detailed_products(df, args.detail_limit)
        
        # Финальное сохранение в Google Drive
        drive_url = processor.save_results_to_drive(df, output_file)
        
        logger.info(f"\n🎉 GOOGLE DRIVE PIPELINE ЗАВЕРШЕН УСПЕШНО!")
        logger.info(f"☁️ Результаты сохранены в Google Drive: {drive_url}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("\n⏹️ Pipeline прерван пользователем")
        return 1
    except Exception as e:
        logger.error(f"\n❌ Критическая ошибка: {e}")
        return 1
    finally:
        if 'processor' in locals():
            processor.print_final_stats()


if __name__ == "__main__":
    sys.exit(main())
