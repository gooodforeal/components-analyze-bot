#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Версия analyze_data.py для использования в Telegram боте
Возвращает результаты в виде строки вместо вывода в консоль
"""

import subprocess
import os
import time
import sys
import io
import locale
import warnings

# Подавляем предупреждения о Python версиях
warnings.filterwarnings('ignore', category=DeprecationWarning)

# Настройка кодировки
try:
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'C.UTF-8')
    except:
        pass

os.environ['PYTHONIOENCODING'] = 'utf-8'

try:
    if hasattr(sys.stdout, 'buffer'):
        if sys.stdout.encoding != 'utf-8':
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'buffer'):
        if sys.stderr.encoding != 'utf-8':
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
except:
    pass

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min as spark_min, max as spark_max, 
    count, sum as spark_sum, stddev, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def create_spark_session():
    """Создание Spark сессии"""
    spark = SparkSession.builder \
        .appName("ComponentsPriceAnalysis") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    return spark

def load_data(spark, hdfs_path):
    """Загрузка данных из HDFS"""
    schema = StructType([
        StructField("Component_Type", StringType(), True),
        StructField("Manufacturer", StringType(), True),
        StructField("Model", StringType(), True),
        StructField("Spec_1", StringType(), True),
        StructField("Spec_2", StringType(), True),
        StructField("Spec_3", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Day", IntegerType(), True),
        StructField("Week", IntegerType(), True),
        StructField("Merchant", StringType(), True),
        StructField("Region_Code", StringType(), True),
        StructField("Currency", StringType(), True),
        StructField("Price_USD", DoubleType(), True),
        StructField("Price_Original", DoubleType(), True)
    ])
    
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(hdfs_path)
    
    return df

def get_component_metrics(df_component, component_type):
    """Получение метрик для конкретного типа компонента"""
    output_lines = []
    
    # Эмодзи для типов компонентов
    emoji_map = {
        'CPU': '🖥️',
        'GPU': '🎮',
        'RAM': '💾'
    }
    emoji = emoji_map.get(component_type, '🔧')
    
    output_lines.append("{} *{} - ДЕТАЛЬНАЯ СТАТИСТИКА*".format(emoji, component_type))
    output_lines.append("")
    
    # Общая статистика по типу
    total_records = df_component.count()
    output_lines.append("📝 Количество записей: *{:,}*".format(total_records))
    output_lines.append("")
    
    # Статистика по ценам
    output_lines.append("💰 *Цены (USD)*")
    price_stats = df_component.agg(
        spark_round(avg("Price_USD"), 2).alias("Средняя цена"),
        spark_round(spark_min("Price_USD"), 2).alias("Минимальная цена"),
        spark_round(spark_max("Price_USD"), 2).alias("Максимальная цена"),
        spark_round(stddev("Price_USD"), 2).alias("Стандартное отклонение")
    ).collect()[0]
    
    output_lines.append("📊 Средняя: *${}*".format(price_stats['Средняя цена']))
    output_lines.append("📉 Мин: *${}*".format(price_stats['Минимальная цена']))
    output_lines.append("📈 Макс: *${}*".format(price_stats['Максимальная цена']))
    output_lines.append("📐 Отклонение: *${}*".format(price_stats['Стандартное отклонение']))
    output_lines.append("")
    
    # Топ-5 производителей для этого типа
    output_lines.append("🏭 *Топ-5 производителей*")
    manufacturer_stats = df_component.groupBy("Manufacturer").agg(
        count("*").alias("Количество записей"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена")
    ).orderBy(col("Количество записей").desc())
    
    rows = manufacturer_stats.limit(5).collect()
    if rows:
        for i, row in enumerate(rows, 1):
            output_lines.append("{}. *{}*".format(i, row['Manufacturer']))
            output_lines.append("   └ {:,} записей | Средняя: *${}*".format(
                row['Количество записей'],
                row['Средняя цена']
            ))
    else:
        output_lines.append("   Нет данных")
    output_lines.append("")
    
    # Статистика по годам
    output_lines.append("📅 *По годам*")
    year_stats = df_component.groupBy("Year").agg(
        count("*").alias("Количество записей"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена")
    ).orderBy("Year")
    
    rows = year_stats.collect()
    if rows:
        for row in rows:
            output_lines.append("• *{}*: {:,} записей | Средняя: *${}*".format(
                row['Year'],
                row['Количество записей'],
                row['Средняя цена']
            ))
    else:
        output_lines.append("   Нет данных")
    output_lines.append("")
    
    # Топ-5 мерчантов для этого типа
    output_lines.append("🛒 *Топ-5 мерчантов*")
    merchant_stats = df_component.groupBy("Merchant").agg(
        count("*").alias("Количество записей"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена")
    ).orderBy(col("Количество записей").desc())
    
    rows = merchant_stats.limit(5).collect()
    if rows:
        for i, row in enumerate(rows, 1):
            output_lines.append("{}. *{}*".format(i, row['Merchant']))
            output_lines.append("   └ {:,} записей | Средняя: *${}*".format(
                row['Количество записей'],
                row['Средняя цена']
            ))
    else:
        output_lines.append("   Нет данных")
    output_lines.append("")
    
    # Корреляция для этого типа
    try:
        correlation = df_component.stat.corr("Year", "Price_USD")
        if correlation is not None:
            corr_emoji = "📈" if correlation > 0 else "📉" if correlation < 0 else "➡️"
            output_lines.append("🔗 *Корреляция год-цена*")
            output_lines.append("{} *{:.4f}*".format(corr_emoji, correlation))
            if abs(correlation) < 0.1:
                output_lines.append("   (очень слабая связь)")
            elif abs(correlation) < 0.3:
                output_lines.append("   (слабая связь)")
            elif abs(correlation) < 0.5:
                output_lines.append("   (умеренная связь)")
            else:
                output_lines.append("   (сильная связь)")
            output_lines.append("")
    except:
        pass
    
    return "\n".join(output_lines)


def get_metrics_string(df):
    """Получение метрик в виде строки с красивым форматированием"""
    output_lines = []
    
    output_lines.append("📊 *КЛЮЧЕВЫЕ МЕТРИКИ ДАТАСЕТА КОМПОНЕНТОВ*")
    output_lines.append("")
    
    # Общая статистика
    output_lines.append("📈 *ОБЩАЯ СТАТИСТИКА*")
    total_records = df.count()
    output_lines.append("📝 Всего записей: *{:,}*".format(total_records))
    output_lines.append("")
    
    # Краткая статистика по типам компонентов
    output_lines.append("🔧 *Распределение по типам*")
    component_stats = df.groupBy("Component_Type").agg(
        count("*").alias("Количество"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена"),
        spark_round(spark_min("Price_USD"), 2).alias("Мин. цена"),
        spark_round(spark_max("Price_USD"), 2).alias("Макс. цена")
    ).orderBy("Component_Type")
    
    rows = component_stats.collect()
    for row in rows:
        output_lines.append("• *{}*: {:,} записей | Средняя: *${}*".format(
            row['Component_Type'],
            row['Количество'],
            row['Средняя цена']
        ))
    output_lines.append("")
    output_lines.append("─" * 40)
    output_lines.append("")
    
    # Получаем список всех типов компонентов
    component_types = [row['Component_Type'] for row in rows]
    
    # Для каждого типа компонента выводим детальную статистику
    for comp_type in component_types:
        df_component = df.filter(col("Component_Type") == comp_type)
        component_metrics = get_component_metrics(df_component, comp_type)
        output_lines.append(component_metrics)
        output_lines.append("─" * 40)
        output_lines.append("")
    
    output_lines.append("✅ *АНАЛИЗ ЗАВЕРШЕН*")
    
    return "\n".join(output_lines)

def main():
    """Главная функция"""
    # Подавляем предупреждения Spark
    import logging
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)
    
    hdfs_path = "hdfs://namenode:9000/data/all_components_prices.csv"
    local_file = "all_components_prices.csv"
    
    spark = create_spark_session()
    
    # Настраиваем Spark для подавления предупреждений
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Пробуем загрузить из HDFS
        try:
            df = load_data(spark, hdfs_path)
        except:
            # Если не получилось, загружаем из локального файла
            local_paths = [
                local_file,
                "/data/{}".format(local_file),
                "/opt/spark/work-dir/{}".format(local_file)
            ]
            for path in local_paths:
                if os.path.exists(path):
                    df = load_data(spark, "file://{}".format(path))
                    break
            else:
                raise Exception("Файл данных не найден")
        
        df.cache()
        results = get_metrics_string(df)
        print(results)
        
    except Exception as e:
        print("Ошибка при выполнении анализа: {}".format(str(e)))
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

