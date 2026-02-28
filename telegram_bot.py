#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram бот для запуска анализа данных компонентов через Hadoop/Spark
Использует aiogram для взаимодействия с пользователем
"""

import os
import asyncio
import subprocess
import sys
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Получение токена бота из переменной окружения
BOT_TOKEN = os.getenv('BOT_TOKEN', '')

if not BOT_TOKEN:
    logger.error("BOT_TOKEN не установлен! Установите переменную окружения BOT_TOKEN в файле .env")
    logger.error("Пример: BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz")
    sys.exit(1)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Глобальная переменная для хранения результатов анализа
analysis_results = {}


def run_analysis():
    """Запуск анализа данных через Spark"""
    try:
        logger.info("Запуск анализа данных...")
        
        # Используем упрощенную версию скрипта для бота
        # Проверяем наличие bot_analyze_data.py, иначе используем обычную версию
        check_result = subprocess.run(
            ['docker', 'exec', 'spark-master', 'test', '-f', '/opt/spark/work-dir/bot_analyze_data.py'],
            capture_output=True,
            timeout=5
        )
        
        if check_result.returncode == 0:
            script_path = '/opt/spark/work-dir/bot_analyze_data.py'
        else:
            script_path = '/opt/spark/work-dir/analyze_data.py'
        
        # Запускаем анализ через Spark
        result = subprocess.run(
            [
                'docker', 'exec',
                '-e', 'PYTHONIOENCODING=utf-8',
                'spark-master',
                '/spark/bin/spark-submit',
                '--master', 'spark://spark-master:7077',
                script_path
            ],
            capture_output=True,
            text=True,
            timeout=300,  # 5 минут таймаут
            encoding='utf-8',
            errors='replace'
        )
        
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'stdout': '',
            'stderr': 'Таймаут выполнения анализа (превышено 5 минут)',
            'returncode': -1
        }
    except Exception as e:
        return {
            'success': False,
            'stdout': '',
            'stderr': str(e),
            'returncode': -1
        }


def format_results_for_telegram(output_text):
    """Форматирование результатов анализа для Telegram"""
    if not output_text:
        return "Результаты анализа не получены"
    
    # Разбиваем вывод на строки
    lines = output_text.split('\n')
    formatted_lines = []
    
    for line in lines:
        # Пропускаем служебные строки Spark
        if any(skip in line for skip in ['INFO', 'WARN', 'ERROR', '26/02/', 'log4j:', 'BlockManager', 
                                          'TaskSetManager', 'DAGScheduler', 'SparkContext', 'MemoryStore',
                                          'FileSourceScanExec', 'CodeGenerator', 'ShutdownHookManager',
                                          'DeprecationWarning', 'pyspark', 'py4j', '/spark/python/lib']):
            continue
        
        # Пропускаем пустые строки в начале
        if not formatted_lines and not line.strip():
            continue
        
        # Добавляем строку (уже отформатированную с эмодзи и markdown)
        if line.strip() or (formatted_lines and formatted_lines[-1].strip()):
            formatted_lines.append(line)
    
    # Объединяем результаты
    result = '\n'.join(formatted_lines)
    
    # Если результат пустой, пробуем извлечь данные по-другому
    if not result or len(result.strip()) < 50:
        # Ищем ключевые метрики в выводе
        key_phrases = ['Общее количество записей', 'Средняя цена', 'Минимальная цена', 
                      'Максимальная цена', 'СТАТИСТИКА', 'КОРРЕЛЯЦИЯ', '📊', '💰']
        found_lines = []
        for line in lines:
            if any(phrase in line for phrase in key_phrases):
                if not any(skip in line for skip in ['INFO', 'WARN', 'ERROR', '26/02/', 'DeprecationWarning']):
                    found_lines.append(line)
        if found_lines:
            result = '\n'.join(found_lines)
    
    # Ограничиваем длину сообщения (Telegram лимит ~4096 символов)
    if len(result) > 4000:
        result = result[:4000] + "\n\n... (результат обрезан)"
    
    return result if result else "Анализ выполнен, но результаты не были извлечены. Проверьте логи."


async def update_progress(status_msg, stage, progress_dots=0, elapsed_time=0):
    """Обновление сообщения с прогресс-баром"""
    stages = {
        'init': ('🚀 Инициализация анализа', 'Подготовка к запуску...'),
        'loading': ('📂 Загрузка данных', 'Чтение данных из HDFS...'),
        'processing': ('⚙️ Обработка данных', 'Выполнение MapReduce операций...'),
        'calculating': ('📊 Расчет метрик', 'Анализ статистики...'),
        'finalizing': ('✨ Завершение', 'Форматирование результатов...')
    }
    
    # Анимация точек
    dots_animation = ['', '.', '..', '...']
    dots = dots_animation[progress_dots % 4]
    
    stage_name, stage_desc = stages.get(stage, ('⏳ Обработка', 'Выполняется анализ...'))
    
    # Прогресс-бар (визуальный)
    progress_bar_length = 10
    progress_filled = min(progress_dots % (progress_bar_length * 2), progress_bar_length)
    progress_bar = '█' * progress_filled + '░' * (progress_bar_length - progress_filled)
    
    # Время выполнения
    time_str = f"⏱ {elapsed_time}с" if elapsed_time > 0 else ""
    
    progress_text = (
        f"{stage_name}{dots}\n"
        f"`{stage_desc}`\n\n"
        f"`[{progress_bar}]` {time_str}\n\n"
        f"⏳ Пожалуйста, подождите..."
    )
    
    try:
        await status_msg.edit_text(progress_text, parse_mode='Markdown')
    except Exception as e:
        logger.warning(f"Не удалось обновить сообщение прогресса: {e}")


async def run_analysis_with_progress(status_msg):
    """Запуск анализа с обновлением прогресса"""
    import time
    start_time = time.time()
    
    try:
        # Этап 1: Инициализация
        await update_progress(status_msg, 'init', 0, 0)
        await asyncio.sleep(0.8)
        
        # Этап 2: Загрузка данных
        elapsed = int(time.time() - start_time)
        await update_progress(status_msg, 'loading', 1, elapsed)
        await asyncio.sleep(1)
        
        # Запускаем анализ в отдельном потоке
        loop = asyncio.get_event_loop()
        analysis_task = loop.run_in_executor(None, run_analysis)
        
        # Пока анализ выполняется, обновляем прогресс
        progress_counter = 0
        stage_sequence = ['loading', 'processing', 'calculating', 'finalizing']
        stage_index = 1  # Начинаем с 'processing'
        
        while not analysis_task.done():
            # Обновляем прогресс каждые 1.5 секунды
            await asyncio.sleep(1.5)
            progress_counter += 1
            elapsed = int(time.time() - start_time)
            
            # Меняем этап каждые 4-5 обновлений
            if progress_counter % 5 == 0 and stage_index < len(stage_sequence) - 1:
                stage_index += 1
            
            current_stage = stage_sequence[min(stage_index, len(stage_sequence) - 1)]
            await update_progress(
                status_msg, 
                current_stage,
                progress_counter,
                elapsed
            )
        
        # Финальное обновление перед получением результата
        elapsed = int(time.time() - start_time)
        await update_progress(status_msg, 'finalizing', progress_counter + 1, elapsed)
        await asyncio.sleep(0.5)
        
        # Получаем результат
        analysis_result = await analysis_task
        return analysis_result
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении анализа с прогрессом: {e}")
        return {
            'success': False,
            'stdout': '',
            'stderr': str(e),
            'returncode': -1
        }


@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    # Отправляем сообщение о начале анализа
    status_msg = await message.answer(
        "🚀 *Запуск анализа данных компонентов*\n\n"
        "⏳ Инициализация..."
    )
    
    try:
        # Запускаем анализ с прогресс-баром
        analysis_result = await run_analysis_with_progress(status_msg)
        
        if analysis_result['success']:
            # Форматируем результаты
            formatted_results = format_results_for_telegram(analysis_result['stdout'])
            
            # Отправляем результаты (уже содержат эмодзи и форматирование)
            response = formatted_results
            
            # Разбиваем на части, если сообщение слишком длинное
            max_length = 4000
            if len(response) > max_length:
                parts = [response[i:i+max_length] for i in range(0, len(response), max_length)]
                for i, part in enumerate(parts):
                    if i == 0:
                        await status_msg.edit_text(part, parse_mode='Markdown')
                    else:
                        await message.answer(part, parse_mode='Markdown')
                    await asyncio.sleep(0.5)  # Небольшая задержка между сообщениями
            else:
                await status_msg.edit_text(response, parse_mode='Markdown')
        else:
            error_msg = "❌ *Ошибка при выполнении анализа*\n\n"
            if analysis_result['stderr']:
                error_details = analysis_result['stderr'][:1500]
                error_msg += f"```\n{error_details}\n```"
            else:
                error_msg += "Неизвестная ошибка"
            
            await status_msg.edit_text(error_msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Ошибка в обработчике /start: {e}")
        await status_msg.edit_text(
            f"❌ Произошла ошибка при выполнении анализа:\n\n{str(e)[:1000]}"
        )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    """Обработчик команды /help"""
    help_text = """🤖 *Бот для анализа данных компонентов*

*Команды:*
/start - Запустить анализ данных компонентов
/help - Показать эту справку
/status - Проверить статус системы

*Описание:*
Бот выполняет анализ данных о ценах на компоненты с использованием Hadoop и Spark.
Результаты включают статистику по ценам, типам компонентов, производителям и другим метрикам.

*Время выполнения:* Обычно 1-3 минуты"""
    await message.answer(help_text, parse_mode='Markdown')


@dp.message(Command("status"))
async def cmd_status(message: Message):
    """Проверка статуса системы"""
    try:
        status_lines = ["📊 *Статус системы:*\n"]
        
        # Проверяем статус Spark контейнера
        result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=spark-master', '--format', '{{.Status}}'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0 and result.stdout.strip():
            status_lines.append("✅ Spark: {}".format(result.stdout.strip()))
        else:
            status_lines.append("❌ Spark: не запущен")
        
        # Проверяем статус HDFS
        result_hdfs = subprocess.run(
            ['docker', 'ps', '--filter', 'name=namenode', '--format', '{{.Status}}'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result_hdfs.returncode == 0 and result_hdfs.stdout.strip():
            status_lines.append("✅ HDFS: {}".format(result_hdfs.stdout.strip()))
        else:
            status_lines.append("❌ HDFS: не запущен")
        
        await message.answer("\n".join(status_lines), parse_mode='Markdown')
    except Exception as e:
        await message.answer(f"❌ Ошибка при проверке статуса: {str(e)}")


@dp.message()
async def echo_handler(message: Message):
    """Обработчик всех остальных сообщений"""
    await message.answer(
        "Неизвестная команда. Используйте /start для запуска анализа или /help для справки."
    )


async def main():
    """Главная функция для запуска бота"""
    logger.info("Запуск Telegram бота...")
    try:
        # Проверяем доступность Spark контейнера
        check_result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=spark-master', '--format', '{{.Names}}'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if 'spark-master' not in check_result.stdout:
            logger.warning("Spark контейнер не найден. Убедитесь, что docker-compose up запущен.")
        
        # Запускаем бота
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")

