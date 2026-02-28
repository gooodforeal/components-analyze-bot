#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PySpark скрипт для анализа данных компонентов с использованием MapReduce
Выводит ключевые метрики из датасета all_components_prices.csv
Автоматически загружает данные в HDFS, если их там нет
"""

import subprocess
import os
import time
import sys
import io

# Настройка кодировки для вывода
import locale
try:
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'C.UTF-8')
    except:
        pass

# Установка переменной окружения для кодировки
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Настройка stdout/stderr для UTF-8
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
    count, sum as spark_sum, stddev, round as spark_roundhasoop-proj
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

def check_hdfs_file_exists(hdfs_path):
    """Проверка существования файла в HDFS через Spark"""
    try:
        # Используем Spark для проверки существования файла
        spark = SparkSession.builder \
            .appName("CheckHDFSFile") \
            .master("local[1]") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.defaultFS", "hdfs://namenode:9000")
        
        uri = sc._jvm.java.net.URI(hdfs_path)
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_path)
        exists = fs.exists(path)
        
        spark.stop()
        return exists
    except Exception as e:
        print("Предупреждение при проверке файла в HDFS: {}".format(e))
        return False

def upload_to_hdfs(local_path, hdfs_path):
    """Загрузка файла в HDFS через PySpark"""
    # В контейнере Spark команда hdfs может быть недоступна,
    # поэтому используем PySpark для загрузки
    return upload_to_hdfs_via_spark(local_path, hdfs_path)

def upload_to_hdfs_via_spark(local_path, hdfs_path):
    """Загрузка файла в HDFS через PySpark"""
    try:
        if not os.path.exists(local_path):
            print("Ошибка: локальный файл не найден: {}".format(local_path))
            return False
        
        print("Использование PySpark для загрузки данных в HDFS...")
        
        # Создаем временную Spark сессию для загрузки
        spark = SparkSession.builder \
            .appName("UploadToHDFS") \
            .master("local[1]") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        # Создаем директорию в HDFS через Spark
        hdfs_dir = os.path.dirname(hdfs_path)
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.defaultFS", "hdfs://namenode:9000")
        
        uri = sc._jvm.java.net.URI("hdfs://namenode:9000")
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        dir_path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_dir)
        
        if not fs.exists(dir_path):
            fs.mkdirs(dir_path)
            print("Директория {} создана в HDFS".format(hdfs_dir))
        
        # Читаем локальный файл
        print("Чтение локального файла: {}".format(local_path))
        df = spark.read.option("header", "true").csv("file://{}".format(local_path))
        
        # Записываем в HDFS как один файл (coalesce(1) объединяет все партиции)
        print("Запись в HDFS: {}".format(hdfs_path))
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(hdfs_path.replace('.csv', '_temp'))
        
        # Переименовываем файл part-00000 в нужное имя через Hadoop API
        temp_dir = hdfs_path.replace('.csv', '_temp')
        temp_path = sc._jvm.org.apache.hadoop.fs.Path(temp_dir)
        
        # Находим файл part-00000
        file_statuses = fs.listStatus(temp_path)
        part_file = None
        for status in file_statuses:
            file_name = status.getPath().getName()
            if file_name.startswith('part-') and not file_name.endswith('.crc'):
                part_file = status.getPath()
                break
        
        if part_file:
            # Создаем путь для финального файла
            final_path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_path)
            
            # Перемещаем файл
            if fs.exists(final_path):
                fs.delete(final_path, False)
            fs.rename(part_file, final_path)
            
            # Удаляем временную директорию
            fs.delete(temp_path, True)
            
            print("✓ Данные успешно загружены в HDFS: {}".format(hdfs_path))
            spark.stop()
            return True
        else:
            print("Предупреждение: не найден файл part- в {}".format(temp_dir))
            print("Данные загружены в директорию: {}".format(temp_dir))
            spark.stop()
            return True
        
    except Exception as e:
        print("Ошибка при загрузке через Spark: {}".format(e))
        import traceback
        traceback.print_exc()
        try:
            spark.stop()
        except:
            pass
        return False

def ensure_data_in_hdfs(local_path, hdfs_path):
    """Проверка и загрузка данных в HDFS, если их там нет"""
    print("Проверка наличия данных в HDFS...")
    
    # Небольшая задержка для инициализации HDFS
    time.sleep(2)
    
    # Проверяем существование файла в HDFS
    file_exists = check_hdfs_file_exists(hdfs_path)
    
    if file_exists:
        print("✓ Данные уже находятся в HDFS: {}".format(hdfs_path))
        return True
    
    # Если файла нет, загружаем его
    print("✗ Данные не найдены в HDFS, начинаю загрузку...")
    
    # Проверяем наличие локального файла
    possible_paths = [
        local_path,
        "/data/{}".format(os.path.basename(local_path)),
        "/opt/spark/work-dir/{}".format(os.path.basename(local_path)),
        os.path.basename(local_path),
        "./{}".format(os.path.basename(local_path))
    ]
    
    local_file = None
    for path in possible_paths:
        if os.path.exists(path):
            local_file = path
            print("✓ Найден локальный файл: {}".format(path))
            break
    
    if local_file:
        success = upload_to_hdfs(local_file, hdfs_path)
        if success:
            # Проверяем еще раз после загрузки
            time.sleep(1)
            if check_hdfs_file_exists(hdfs_path):
                print("✓ Подтверждено: данные успешно загружены в HDFS")
                return True
        return success
    else:
        print("✗ Ошибка: локальный файл не найден.")
        print("Проверенные пути: {}".format(possible_paths))
        print("Текущая директория: {}".format(os.getcwd()))
        return False

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

def print_metrics(df):
    """Вывод ключевых метрик из датасета"""
    
    print("=" * 80)
    print("КЛЮЧЕВЫЕ МЕТРИКИ ДАТАСЕТА КОМПОНЕНТОВ")
    print("=" * 80)
    
    # Общая статистика
    print("\n1. ОБЩАЯ СТАТИСТИКА:")
    print("-" * 80)
    total_records = df.count()
    print("Общее количество записей: {:,}".format(total_records))
    
    # Статистика по ценам
    print("\n2. СТАТИСТИКА ПО ЦЕНАМ (USD):")
    print("-" * 80)
    price_stats = df.agg(
        spark_round(avg("Price_USD"), 2).alias("Средняя цена"),
        spark_round(spark_min("Price_USD"), 2).alias("Минимальная цена"),
        spark_round(spark_max("Price_USD"), 2).alias("Максимальная цена"),
        spark_round(stddev("Price_USD"), 2).alias("Стандартное отклонение")
    ).collect()[0]
    
    print("Средняя цена: ${}".format(price_stats['Средняя цена']))
    print("Минимальная цена: ${}".format(price_stats['Минимальная цена']))
    print("Максимальная цена: ${}".format(price_stats['Максимальная цена']))
    print("Стандартное отклонение: ${}".format(price_stats['Стандартное отклонение']))
    
    # Статистика по типам компонентов
    print("\n3. СТАТИСТИКА ПО ТИПАМ КОМПОНЕНТОВ:")
    print("-" * 80)
    component_stats = df.groupBy("Component_Type").agg(
        count("*").alias("Количество"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена"),
        spark_round(spark_min("Price_USD"), 2).alias("Мин. цена"),
        spark_round(spark_max("Price_USD"), 2).alias("Макс. цена")
    ).orderBy("Component_Type")
    
    # Выводим данные построчно для избежания проблем с кодировкой
    rows = component_stats.collect()
    print("Component_Type | Количество | Средняя цена | Мин. цена | Макс. цена")
    print("-" * 80)
    for row in rows:
        print("{} | {} | ${} | ${} | ${}".format(
            row['Component_Type'],
            row['Количество'],
            row['Средняя цена'],
            row['Мин. цена'],
            row['Макс. цена']
        ))
    
    # Статистика по производителям
    print("\n4. ТОП-10 ПРОИЗВОДИТЕЛЕЙ ПО КОЛИЧЕСТВУ ЗАПИСЕЙ:")
    print("-" * 80)
    manufacturer_stats = df.groupBy("Manufacturer").agg(
        count("*").alias("Количество записей"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена")
    ).orderBy(col("Количество записей").desc())
    
    # Выводим данные построчно
    rows = manufacturer_stats.limit(10).collect()
    print("Manufacturer | Количество записей | Средняя цена")
    print("-" * 80)
    for row in rows:
        print("{} | {} | ${}".format(
            row['Manufacturer'],
            row['Количество записей'],
            row['Средняя цена']
        ))
    
    # Статистика по годам
    print("\n5. СТАТИСТИКА ПО ГОДАМ:")
    print("-" * 80)
    year_stats = df.groupBy("Year").agg(
        count("*").alias("Количество записей"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена"),
        spark_round(spark_min("Price_USD"), 2).alias("Мин. цена"),
        spark_round(spark_max("Price_USD"), 2).alias("Макс. цена")
    ).orderBy("Year")
    
    # Выводим данные построчно
    rows = year_stats.collect()
    print("Year | Количество записей | Средняя цена | Мин. цена | Макс. цена")
    print("-" * 80)
    for row in rows:
        print("{} | {} | ${} | ${} | ${}".format(
            row['Year'],
            row['Количество записей'],
            row['Средняя цена'],
            row['Мин. цена'],
            row['Макс. цена']
        ))
    
    # Статистика по мерчантам
    print("\n6. ТОП-10 МЕРЧАНТОВ ПО КОЛИЧЕСТВУ ЗАПИСЕЙ:")
    print("-" * 80)
    merchant_stats = df.groupBy("Merchant").agg(
        count("*").alias("Количество записей"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена")
    ).orderBy(col("Количество записей").desc())
    
    # Выводим данные построчно
    rows = merchant_stats.limit(10).collect()
    print("Merchant | Количество записей | Средняя цена")
    print("-" * 80)
    for row in rows:
        print("{} | {} | ${}".format(
            row['Merchant'],
            row['Количество записей'],
            row['Средняя цена']
        ))
    
    # Статистика по моделям (топ-10)
    print("\n7. ТОП-10 МОДЕЛЕЙ ПО КОЛИЧЕСТВУ ЗАПИСЕЙ:")
    print("-" * 80)
    model_stats = df.groupBy("Model").agg(
        count("*").alias("Количество записей"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена")
    ).orderBy(col("Количество записей").desc())
    
    # Выводим данные построчно
    rows = model_stats.limit(10).collect()
    print("Model | Количество записей | Средняя цена")
    print("-" * 80)
    for row in rows:
        print("{} | {} | ${}".format(
            row['Model'],
            row['Количество записей'],
            row['Средняя цена']
        ))
    
    # Статистика по регионам
    print("\n8. СТАТИСТИКА ПО РЕГИОНАМ:")
    print("-" * 80)
    region_stats = df.groupBy("Region_Code").agg(
        count("*").alias("Количество записей"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена")
    ).orderBy(col("Количество записей").desc())
    
    # Выводим данные построчно
    rows = region_stats.collect()
    print("Region_Code | Количество записей | Средняя цена")
    print("-" * 80)
    for row in rows:
        print("{} | {} | ${}".format(
            row['Region_Code'],
            row['Количество записей'],
            row['Средняя цена']
        ))
    
    # Динамика цен по месяцам (для последнего года)
    print("\n9. ДИНАМИКА ЦЕН ПО МЕСЯЦАМ (2018 год):")
    print("-" * 80)
    monthly_stats = df.filter(col("Year") == 2018).groupBy("Month").agg(
        count("*").alias("Количество записей"),
        spark_round(avg("Price_USD"), 2).alias("Средняя цена")
    ).orderBy("Month")
    
    # Выводим данные построчно
    rows = monthly_stats.collect()
    print("Month | Количество записей | Средняя цена")
    print("-" * 80)
    for row in rows:
        print("{} | {} | ${}".format(
            row['Month'],
            row['Количество записей'],
            row['Средняя цена']
        ))
    
    # Корреляция между полями
    print("\n10. КОРРЕЛЯЦИЯ МЕЖДУ ГОДОМ И ЦЕНОЙ:")
    print("-" * 80)
    correlation = df.stat.corr("Year", "Price_USD")
    print("Корреляция между годом и ценой: {:.4f}".format(correlation))
    
    print("\n" + "=" * 80)
    print("АНАЛИЗ ЗАВЕРШЕН")
    print("=" * 80)

def main():
    """Главная функция"""
    # Пути к данным
    hdfs_path = "hdfs://namenode:9000/data/all_components_prices.csv"
    local_file = "all_components_prices.csv"
    
    # Сначала убеждаемся, что данные в HDFS
    print("=" * 80)
    print("ПОДГОТОВКА ДАННЫХ")
    print("=" * 80)
    
    if not ensure_data_in_hdfs(local_file, hdfs_path):
        print("Предупреждение: не удалось загрузить данные в HDFS автоматически")
        print("Попытка продолжить с существующими данными в HDFS...")
        time.sleep(2)
    
    # Создание Spark сессии
    print("\n" + "=" * 80)
    print("СОЗДАНИЕ SPARK СЕССИИ")
    print("=" * 80)
    spark = create_spark_session()
    
    try:
        # Загрузка данных из HDFS
        print("\nЗагрузка данных из HDFS...")
        df = load_data(spark, hdfs_path)
        
        # Кэширование данных для оптимизации
        df.cache()
        
        # Вывод метрик
        print_metrics(df)
        
    except Exception as e:
        print("\nОшибка при выполнении анализа: {}".format(str(e)))
        print("\nПопытка загрузить данные из локального файла...")
        try:
            # Пробуем загрузить из локального файла как fallback
            local_paths = [
                local_file,
                "/data/{}".format(local_file),
                "/opt/spark/work-dir/{}".format(local_file)
            ]
            for path in local_paths:
                if os.path.exists(path):
                    print("Загрузка из локального файла: {}".format(path))
                    df = load_data(spark, "file://{}".format(path))
                    df.cache()
                    print_metrics(df)
                    break
            else:
                raise Exception("Локальный файл не найден")
        except Exception as e2:
            print("Ошибка при загрузке из локального файла: {}".format(str(e2)))
            import traceback
            traceback.print_exc()
    
    finally:
        # Закрытие Spark сессии
        spark.stop()

if __name__ == "__main__":
    main()

