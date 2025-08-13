#!/usr/bin/env python3
"""
Полный pipeline для обработки детских витаминов с OpenAI Vision API:
1. Читает ключевые запросы
2. Делает поиск товаров (2 страницы)  
3. Заполняет основную таблицу
4. Обрабатывает детальные данные с Supplement Facts через OpenAI Vision
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

class OpenAISupplementFactsAI:
    """Класс для AI анализа Supplement Facts изображений через OpenAI Vision API"""
    
    def __init__(self, api_key: str):
        self.client = openai.OpenAI(api_key=api_key)
        self.known_extractions = {
            # Кэш для быстрой обработки известных изображений
        }
    
    def extract_image_id(self, url: str) -> str:
        """Извлекает ID изображения из Amazon URL"""
        try:
            match = re.search(r'/([A-Z0-9]{10,11})[._-]', url)
            if match:
                return match.group(1)
            return ""
        except:
            return ""
    
    def download_image(self, url: str) -> bytes:
        """Скачивает изображение по URL"""
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.content
    
    def encode_image_base64(self, image_bytes: bytes) -> str:
        """Кодирует изображение в base64"""
        return base64.b64encode(image_bytes).decode('utf-8')
    
    def analyze_supplement_facts(self, url: str, product_name: str = "", brand: str = "") -> tuple:
        """
        AI анализ изображения с извлечением данных через OpenAI Vision.
        Возвращает: (ingredients, dosages, age_group, form)
        """
        print(f"         🤖 OpenAI Vision анализирует Supplement Facts...")
        
        # Проверяем кэш
        image_id = self.extract_image_id(url)
        if image_id in self.known_extractions:
            print(f"         ✅ Найден в кэше: {image_id}")
            result = self.known_extractions[image_id]
            return (
                result["ingredients"], 
                result["dosages"],
                result.get("age_group", ""),
                result.get("form", "")
            )
        
        try:
            # Скачиваем изображение
            image_bytes = self.download_image(url)
            image_base64 = self.encode_image_base64(image_bytes)
            
            # Системный промпт
            system_prompt = """Ты эксперт по анализу этикеток пищевых добавок. Извлеки данные из изображения Supplement Facts и верни ТОЛЬКО JSON в следующем формате:

{
  "ingredients": "список всех ингредиентов через запятую",
  "dosages": "пары Ингредиент: дозировка через точку с запятой",
  "age_group": "возрастная группа в формате 2+, 4+ и т.д.",
  "form": "форма выпуска: Gummies, Chewable, Tablets, Capsules, Liquid, Drops, Powder, Softgels"
}

Если какое-то поле не найдено, оставь пустую строку. НЕ добавляй никаких комментариев, ТОЛЬКО JSON."""
            
            # Запрос к OpenAI
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
                                "text": f"Проанализируй Supplement Facts для продукта: {brand} {product_name}"
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
                max_tokens=1000,
                temperature=0
            )
            
            # Парсим ответ
            content = response.choices[0].message.content.strip()
            
            # Убираем markdown форматирование
            if content.startswith("```json"):
                content = content[7:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            # Парсим JSON
            data = json.loads(content)
            
            ingredients = data.get("ingredients", "")
            dosages = data.get("dosages", "")
            age_group = data.get("age_group", "")
            form = data.get("form", "")
            
            # Сохраняем в кэш
            if image_id:
                self.known_extractions[image_id] = {
                    "ingredients": ingredients,
                    "dosages": dosages,
                    "age_group": age_group,
                    "form": form
                }
            
            print(f"         ✅ OpenAI анализ завершен")
            return ingredients, dosages, age_group, form
            
        except json.JSONDecodeError as e:
            print(f"         ❌ Ошибка парсинга JSON: {e}")
            return "", "", "", ""
            
        except Exception as e:
            print(f"         ❌ Ошибка OpenAI анализа: {e}")
            return "", "", "", ""

class FullPipelineProcessor:
    """Полный процессор для комплексной обработки витаминов с OpenAI Vision"""
    
    def __init__(self, rainforest_api_key: str, openai_api_key: str):
        self.rainforest_api_key = rainforest_api_key
        self.openai_api_key = openai_api_key
        self.base_url = "https://api.rainforestapi.com/request"
        
        print("🔍 Инициализация OCR...")
        self.ocr_reader = easyocr.Reader(['en'], gpu=False)
        print("✅ OCR готов")
        
        print("🤖 Инициализация OpenAI Vision...")
        self.ai_analyzer = OpenAISupplementFactsAI(openai_api_key)
        print("✅ OpenAI Vision готов")
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        })
        
        # Ключевые слова для поиска Supplement Facts
        self.supplement_keywords = [
            'supplement facts', 'supplement fact', 'nutrition facts',
            'serving size', 'servings per container', 'amount per serving',
            'daily value', '% daily value', '%dv'
        ]
        
        # Статистика
        self.stats = {
            'total_api_calls': 0,
            'search_calls': 0,
            'product_calls': 0,
            'credits_used': 0,
            'products_found': 0,
            'supplement_facts_found': 0,
            'openai_calls': 0,
            'start_time': datetime.now()
        }
    
    def load_keywords(self, keywords_file: str) -> List[str]:
        """Загружает ключевые запросы из CSV файла"""
        print(f"📂 Загрузка ключевых запросов из: {keywords_file}")
        
        try:
            df = pd.read_csv(keywords_file)
            keywords = df.iloc[:, 1].dropna().tolist()
            print(f"✅ Загружено {len(keywords)} ключевых запросов")
            return keywords
        except Exception as e:
            print(f"❌ Ошибка загрузки ключевых запросов: {e}")
            return []
    
    def search_products_multiple_pages(self, search_term: str, max_pages: int = 2) -> List[Dict]:
        """Поиск товаров с пагинацией"""
        
        print(f"\n🔍 Поиск товаров: '{search_term}' (до {max_pages} страниц)")
        
        all_products = []
        
        for page in range(1, max_pages + 1):
            print(f"   �� Страница {page}/{max_pages}")
            
            params = {
                'api_key': self.rainforest_api_key,
                'type': 'search',
                'amazon_domain': 'amazon.com',
                'search_term': search_term,
                'page': page
            }
            
            try:
                response = self.session.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                self.stats['total_api_calls'] += 1
                self.stats['search_calls'] += 1
                
                if 'request_info' in data:
                    credits_used = data['request_info'].get('credits_used_this_request', 1)
                    self.stats['credits_used'] += credits_used
                    print(f"      💳 Кредитов: {credits_used}")
                
                if not data.get('request_info', {}).get('success', False):
                    print(f"      ❌ Ошибка страницы {page}: {data}")
                    continue
                
                search_results = data.get('search_results', [])
                print(f"      ✅ Найдено: {len(search_results)} товаров")
                
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
                
                if page < max_pages:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"      ❌ Ошибка страницы {page}: {e}")
                continue
        
        self.stats['products_found'] += len(all_products)
        print(f"   🎯 Всего найдено товаров: {len(all_products)}")
        return all_products
    
    def get_product_details(self, asin: str) -> Dict:
        """Получение детальных данных товара"""
        
        print(f"   📦 Получение данных: {asin}")
        
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
                self.stats['credits_used'] += credits_used
                print(f"      💳 Кредитов: {credits_used}")
            
            if not data.get('request_info', {}).get('success', False):
                return {'success': False, 'error': 'API request failed'}
            
            product_data = data.get('product', {})
            
            if not product_data:
                return {'success': False, 'error': 'Product data not found'}
            
            print(f"      ✅ Данные получены")
            return {'success': True, 'product': product_data}
            
        except Exception as e:
            print(f"      ❌ Ошибка: {e}")
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
            response = self.session.get(image_url, timeout=10)
            response.raise_for_status()
            
            result['accessible'] = True
            
            if len(response.content) < 10000:
                result['error'] = 'Файл слишком маленький'
                return result
            
            # OCR анализ
            image = Image.open(io.BytesIO(response.content))
            image_np = np.array(image)
            
            ocr_results = self.ocr_reader.readtext(image_np)
            all_text = ' '.join([result[1] for result in ocr_results]).lower()
            
            # Поиск ключевых слов
            found_keywords = []
            confidence = 0.0
            
            if 'supplement facts' in all_text:
                confidence += 0.9
                found_keywords.append('supplement facts')
            elif 'supplement fact' in all_text:
                confidence += 0.8
                found_keywords.append('supplement fact')
            
            additional = [
                ('serving size', 0.2), ('servings per container', 0.2),
                ('daily value', 0.15), ('% daily value', 0.15),
                ('amount per serving', 0.1)
            ]
            
            for keyword, weight in additional:
                if keyword in all_text:
                    confidence += weight
                    found_keywords.append(keyword)
            
            result['confidence'] = min(confidence, 1.0)
            result['keywords_found'] = found_keywords
            result['contains_supplement_facts'] = confidence > 0.6
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            return result
    
    def find_supplement_facts_image(self, product_data: Dict) -> Optional[str]:
        """Поиск изображения с Supplement Facts"""
        
        images = product_data.get('images', [])
        if not images:
            return None
            
        print(f"      🔍 Анализ {len(images)} изображений...")
        
        best_image = None
        best_confidence = 0.0
        
        for image_data in images:
            image_url = image_data.get('link', '')
            
            if not image_url:
                continue
            
            analysis = self.analyze_image_for_supplement_facts(image_url)
            
            if analysis['accessible'] and analysis['contains_supplement_facts']:
                print(f"         🎉 НАЙДЕН! Уверенность: {analysis['confidence']:.2f}")
                
                if analysis['confidence'] > best_confidence:
                    best_confidence = analysis['confidence']
                    best_image = image_url
        
        if best_image:
            self.stats['supplement_facts_found'] += 1
            return best_image
        
        return None
    
    def load_existing_data(self, output_file: str) -> pd.DataFrame:
        """Загружает существующие данные или создает пустую таблицу"""
        try:
            df = pd.read_csv(output_file, dtype=str).fillna("")
            print(f"📂 Загружена существующая таблица: {len(df)} записей")
            return df
        except FileNotFoundError:
            print(f"📂 Файл {output_file} не найден, создается новая таблица")
            return pd.DataFrame()
        except Exception as e:
            print(f"⚠️ Ошибка загрузки {output_file}: {e}")
            return pd.DataFrame()
    
    def check_search_term_processed(self, df: pd.DataFrame, search_term: str) -> bool:
        """Проверяет, обработан ли уже данный поисковый запрос"""
        if df.empty or 'Search Term' not in df.columns:
            return False
        
        existing_records = df[df['Search Term'] == search_term]
        if len(existing_records) > 0:
            print(f"🔍 Найдено {len(existing_records)} записей для '{search_term}'")
            return True
        return False
    
    def create_products_dataframe(self, products: List[Dict], search_term: str, existing_df: pd.DataFrame = None) -> pd.DataFrame:
        """Создает DataFrame с товарами или дополняет существующий"""
        
        print(f"\n�� Обработка товаров ({len(products)} записей) для '{search_term}'")
        
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
        
        print(f"✅ Таблица обновлена: {len(new_df)} новых записей, всего {len(df)} записей")
        return df
    
    def is_product_processed(self, row) -> bool:
        """Проверяет, обработан ли уже товар"""
        bsr_filled = bool(str(row.get('BSR', '')).strip() and str(row.get('BSR', '')).strip() != 'nan')
        category_filled = bool(str(row.get('Категория', '')).strip())
        
        return bsr_filled or category_filled
    
    def process_detailed_products(self, df: pd.DataFrame, limit: int = None) -> pd.DataFrame:
        """Обрабатывает детальные данные товаров с OpenAI Vision"""
        
        total_products = len(df) if limit is None else min(limit, len(df))
        
        print(f"\n🔍 Детальная обработка товаров (лимит: {total_products})")
        
        processed_count = 0
        skipped_count = 0
        
        for i in range(len(df)):
            if limit and processed_count >= limit:
                print(f"🛑 Достигнут лимит обработки: {limit}")
                break
                
            asin = df.iloc[i]['ASIN']
            
            if self.is_product_processed(df.iloc[i]):
                print(f"⏭️  Товар {i+1}: {asin} - уже обработан")
                skipped_count += 1
                continue
            
            print(f"\n📦 Товар {i+1}: {asin}")
            
            details = self.get_product_details(asin)
            
            if not details['success']:
                print(f"      ❌ Ошибка получения данных: {details.get('error', 'Unknown')}")
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
                print(f"      📊 BSR: #{bsr_number} ({bsr_category})")
            
            # Ищем Supplement Facts
            supplement_image = self.find_supplement_facts_image(product_data)
            
            if supplement_image:
                df.iloc[i, df.columns.get_loc('Ссылка на Supplement Facts (изображение)')] = supplement_image
                print(f"      🎯 Supplement Facts найден!")
                
                # OpenAI Vision анализ
                try:
                    product_title = df.iloc[i]['Название продукта (Title)']
                    brand = df.iloc[i]['Бренд']
                    
                    ingredients, dosages, age_group, form = self.ai_analyzer.analyze_supplement_facts(
                        supplement_image, product_title, brand
                    )
                    
                    self.stats['openai_calls'] += 1
                    
                    # Заполняем извлеченные данные
                    df.iloc[i, df.columns.get_loc('Все ингредиенты (из Supplement Facts)')] = ingredients
                    df.iloc[i, df.columns.get_loc('Дозировки (мг/ед.)')] = dosages
                    
                    if age_group:
                        df.iloc[i, df.columns.get_loc('Возрастная группа')] = age_group
                        print(f"         📅 Возраст: {age_group}")
                    
                    if form:
                        df.iloc[i, df.columns.get_loc('Форма выпуска')] = form
                        print(f"         💊 Форма: {form}")
                    
                    print(f"      ✅ OpenAI Vision анализ завершен!")
                    
                except Exception as e:
                    print(f"      ❌ Ошибка OpenAI анализа: {e}")
                
            else:
                print(f"      ❌ Supplement Facts не найден")
            
            processed_count += 1
            time.sleep(1)
        
        print(f"\n📊 Обработка завершена: {processed_count} обработано, {skipped_count} пропущено")
        return df
    
    def save_results(self, df: pd.DataFrame, output_file: str):
        """Сохраняет результаты в CSV"""
        print(f"\n💾 Сохранение результатов в: {output_file}")
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"✅ Сохранено {len(df)} записей")
    
    def print_stats(self):
        """Выводит финальную статистику"""
        duration = datetime.now() - self.stats['start_time']
        
        print("\n" + "=" * 60)
        print("📊 ФИНАЛЬНАЯ СТАТИСТИКА:")
        print(f"🕐 Время работы: {duration}")
        print(f"📞 Всего API вызовов: {self.stats['total_api_calls']}")
        print(f"�� Поисковых запросов: {self.stats['search_calls']}")
        print(f"📦 Запросов товаров: {self.stats['product_calls']}")
        print(f"💳 Потрачено кредитов Rainforest: {self.stats['credits_used']}")
        print(f"🤖 OpenAI Vision вызовов: {self.stats['openai_calls']}")
        print(f"🛍️ Найдено товаров: {self.stats['products_found']}")
        print(f"🎯 Найдено Supplement Facts: {self.stats['supplement_facts_found']}")
        print("=" * 60)

def main():
    parser = argparse.ArgumentParser(description='Полный pipeline с OpenAI Vision API')
    parser.add_argument('--rainforest-key', required=True, help='Rainforest API ключ')
    parser.add_argument('--openai-key', required=True, help='OpenAI API ключ')
    parser.add_argument('--keywords-file', default='Kids Supplements Keywords.csv', help='Файл с ключевыми запросами')
    parser.add_argument('--output-file', default='kids_supplements_openai_filled.csv', help='Выходной файл')
    parser.add_argument('--max-pages', type=int, default=2, help='Максимум страниц поиска')
    parser.add_argument('--detail-limit', type=int, default=3, help='Количество товаров для детальной обработки')
    
    args = parser.parse_args()
    
    print("🚀 ЗАПУСК ПОЛНОГО PIPELINE С OPENAI VISION")
    print("=" * 60)
    
    processor = FullPipelineProcessor(args.rainforest_key, args.openai_key)
    
    try:
        # Загружаем ключевые запросы
        keywords = processor.load_keywords(args.keywords_file)
        if not keywords:
            print("❌ Ключевые запросы не загружены")
            return
        
        # Загружаем существующие данные
        existing_df = processor.load_existing_data('kids_supplements.csv')
        
        # Обрабатываем ключевые запросы
        df = existing_df
        
        for keyword_idx, search_term in enumerate(keywords):
            print(f"\n🎯 Обработка ключевого запроса {keyword_idx+1}/{len(keywords)}: '{search_term}'")
            
            if not existing_df.empty and processor.check_search_term_processed(existing_df, search_term):
                print(f"⏭️  Поисковый запрос '{search_term}' уже обработан, пропускаем поиск")
            else:
                products = processor.search_products_multiple_pages(search_term, args.max_pages)
                
                if not products:
                    print(f"❌ Товары не найдены для '{search_term}'")
                    continue
                
                df = processor.create_products_dataframe(products, search_term, df)
                processor.save_results(df, 'kids_supplements.csv')
                print(f"💾 Промежуточное сохранение: {len(df)} записей")
            
            # Тестируем только первый ключ
            if keyword_idx >= 0:
                break
        
        if df.empty:
            print("❌ Нет данных для обработки")
            return
        
        # Обрабатываем детальные данные
        df = processor.process_detailed_products(df, args.detail_limit)
        
        # Сохраняем результаты
        processor.save_results(df, args.output_file)
        processor.save_results(df, 'kids_supplements.csv')
        
        print(f"\n🎉 PIPELINE ЗАВЕРШЕН УСПЕШНО!")
        print(f"📁 Результаты сохранены в: {args.output_file}")
        
    except KeyboardInterrupt:
        print("\n⏹️ Обработка прервана пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
    finally:
        processor.print_stats()

if __name__ == "__main__":
    main()
