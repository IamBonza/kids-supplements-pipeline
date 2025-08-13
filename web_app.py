#!/usr/bin/env python3
"""
Web интерфейс для Kids Supplements Pipeline на Render.com
"""

import os
import threading
import time
from datetime import datetime
from flask import Flask, render_template_string, request, send_file, jsonify
import subprocess
import json
from pathlib import Path

app = Flask(__name__)

# Глобальные переменные для отслеживания состояния
pipeline_status = {
    'running': False,
    'progress': 0,
    'message': 'Готов к запуску',
    'last_run': None,
    'results_file': None
}

# HTML шаблон
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Kids Supplements Pipeline</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
        .container { max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        h1 { color: #2c3e50; text-align: center; }
        .status { padding: 15px; margin: 20px 0; border-radius: 5px; }
        .status.ready { background: #d4edda; border: 1px solid #c3e6cb; color: #155724; }
        .status.running { background: #d1ecf1; border: 1px solid #bee5eb; color: #0c5460; }
        .status.error { background: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; }
        .button { background: #007bff; color: white; padding: 12px 24px; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; margin: 10px 5px; }
        .button:hover { background: #0056b3; }
        .button:disabled { background: #6c757d; cursor: not-allowed; }
        .progress { width: 100%; background: #e9ecef; border-radius: 5px; margin: 10px 0; }
        .progress-bar { height: 20px; background: #007bff; border-radius: 5px; transition: width 0.3s; }
        .config { background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }
        .input { width: 100%; padding: 8px; margin: 5px 0; border: 1px solid #ced4da; border-radius: 4px; }
        .results { background: #e8f5e8; padding: 15px; border-radius: 5px; margin: 20px 0; }
    </style>
    <script>
        function updateStatus() {
            fetch('/api/status')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('status-message').textContent = data.message;
                    document.getElementById('progress-bar').style.width = data.progress + '%';
                    document.getElementById('progress-text').textContent = data.progress + '%';
                    
                    const statusDiv = document.getElementById('status');
                    statusDiv.className = 'status ' + (data.running ? 'running' : 'ready');
                    
                    document.getElementById('start-btn').disabled = data.running;
                    
                    if (data.results_file) {
                        document.getElementById('download-section').style.display = 'block';
                        document.getElementById('results-info').textContent = 
                            'Последняя обработка: ' + (data.last_run || 'Неизвестно');
                    }
                });
        }
        
        function startPipeline() {
            const keywordLimit = document.getElementById('keyword-limit').value;
            const detailLimit = document.getElementById('detail-limit').value;
            
            fetch('/api/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    keyword_limit: parseInt(keywordLimit),
                    detail_limit: parseInt(detailLimit)
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    alert('Pipeline запущен!');
                } else {
                    alert('Ошибка: ' + data.error);
                }
            });
        }
        
        setInterval(updateStatus, 2000);
        window.onload = updateStatus;
    </script>
</head>
<body>
    <div class="container">
        <h1>🧸 Kids Supplements Pipeline</h1>
        
        <div id="status" class="status ready">
            <strong>Статус:</strong> <span id="status-message">Загрузка...</span>
            <div class="progress">
                <div id="progress-bar" class="progress-bar" style="width: 0%"></div>
            </div>
            <small id="progress-text">0%</small>
        </div>
        
        <div class="config">
            <h3>⚙️ Настройки обработки</h3>
            <label>Количество ключевых слов:</label>
            <input type="number" id="keyword-limit" class="input" value="5" min="1" max="51">
            
            <label>Лимит товаров для детального анализа:</label>
            <input type="number" id="detail-limit" class="input" value="10" min="1" max="100">
            
            <button id="start-btn" class="button" onclick="startPipeline()">
                🚀 Запустить обработку
            </button>
        </div>
        
        <div id="download-section" class="results" style="display: none;">
            <h3>📊 Результаты</h3>
            <p id="results-info">Результаты готовы к скачиванию</p>
            <a href="/download" class="button">📥 Скачать CSV</a>
            <a href="/api/stats" class="button">📈 Статистика</a>
        </div>
        
        <div class="config">
            <h3>ℹ️ Информация</h3>
            <p><strong>Pipeline:</strong> OpenAI Vision + Rainforest API</p>
            <p><strong>Возможности:</strong> Извлечение ингредиентов, дозировок, возраста, формы выпуска</p>
            <p><strong>Resume:</strong> Автоматическое продолжение при перезапуске</p>
        </div>
    </div>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/status')
def api_status():
    return jsonify(pipeline_status)

@app.route('/api/start', methods=['POST'])
def api_start():
    global pipeline_status
    
    if pipeline_status['running']:
        return jsonify({'success': False, 'error': 'Pipeline уже запущен'})
    
    data = request.get_json()
    keyword_limit = data.get('keyword_limit', 5)
    detail_limit = data.get('detail_limit', 10)
    
    # Запускаем pipeline в отдельном потоке
    thread = threading.Thread(target=run_pipeline, args=(keyword_limit, detail_limit))
    thread.daemon = True
    thread.start()
    
    return jsonify({'success': True})

@app.route('/download')
def download():
    # Проверяем persistent disk сначала, потом рабочую папку
    persistent_file = Path('/opt/render/project/src/data/kids_supplements.csv')
    working_file = Path('kids_supplements.csv')
    
    if persistent_file.exists():
        results_file = persistent_file
    elif working_file.exists():
        results_file = working_file
    else:
        return "Файл результатов не найден", 404
    
    return send_file(str(results_file), as_attachment=True, 
                    download_name=f'kids_supplements_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

@app.route('/api/stats')
def api_stats():
    # Проверяем persistent disk сначала, потом рабочую папку
    persistent_stats = Path('/opt/render/project/src/data/pipeline_stats.json')
    working_stats = Path('pipeline_stats.json')
    
    if persistent_stats.exists():
        stats_file = persistent_stats
    elif working_stats.exists():
        stats_file = working_stats
    else:
        return jsonify({'error': 'Статистика не найдена'})
    
    with open(stats_file, 'r') as f:
        stats = json.load(f)
    return jsonify(stats)

def run_pipeline(keyword_limit, detail_limit):
    global pipeline_status
    
    pipeline_status['running'] = True
    pipeline_status['progress'] = 0
    pipeline_status['message'] = 'Инициализация...'
    
    try:
        # Получаем API ключи из переменных окружения
        rainforest_key = os.getenv('RAINFOREST_API_KEY')
        openai_key = os.getenv('OPENAI_API_KEY')
        
        if not rainforest_key or not openai_key:
            pipeline_status['message'] = 'Ошибка: API ключи не настроены'
            pipeline_status['running'] = False
            return
        
        pipeline_status['progress'] = 10
        pipeline_status['message'] = 'Запуск pipeline...'
        
        # Создаем папку data если её нет
        data_dir = Path('/opt/render/project/src/data')
        data_dir.mkdir(exist_ok=True)
        
        # Формируем команду с сохранением на persistent disk
        output_file = str(data_dir / 'kids_supplements.csv')
        cmd = [
            'python3', 'pipeline_openai_complete.py',
            '--rainforest-key', rainforest_key,
            '--openai-key', openai_key,
            '--keyword-limit', str(keyword_limit),
            '--detail-limit', str(detail_limit),
            '--output-file', output_file
        ]
        
        pipeline_status['progress'] = 20
        pipeline_status['message'] = 'Обработка данных...'
        
        # Запускаем pipeline
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        # Симуляция прогресса (в реальности можно парсить вывод pipeline)
        for i in range(20, 90, 10):
            if process.poll() is None:  # Процесс еще работает
                pipeline_status['progress'] = i
                time.sleep(30)  # Обновляем каждые 30 секунд
        
        # Ждем завершения
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            pipeline_status['progress'] = 100
            pipeline_status['message'] = 'Обработка завершена успешно!'
            pipeline_status['last_run'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            pipeline_status['results_file'] = 'kids_supplements.csv'
            
            # Сохраняем статистику на persistent disk
            stats = {
                'last_run': pipeline_status['last_run'],
                'keyword_limit': keyword_limit,
                'detail_limit': detail_limit,
                'success': True
            }
            stats_file = data_dir / 'pipeline_stats.json'
            with open(stats_file, 'w') as f:
                json.dump(stats, f)
        else:
            pipeline_status['message'] = f'Ошибка выполнения: {stderr[:200]}'
            
    except Exception as e:
        pipeline_status['message'] = f'Критическая ошибка: {str(e)[:200]}'
    
    finally:
        pipeline_status['running'] = False

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
