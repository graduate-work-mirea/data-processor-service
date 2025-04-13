import pandas as pd
import json
import os
import sys
import logging
from datetime import datetime, timedelta
import argparse

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('DataProcessor')

def load_data(input_file):
    """Загрузка данных из JSON-файла."""
    logger.info(f"Loading data from {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return pd.DataFrame(data)

def preprocess_data(df):
    """Предобработка данных: обработка типов и пропущенных значений."""
    logger.info("Starting data preprocessing")

    # Преобразование даты
    df['date'] = pd.to_datetime(df['date'])

    # Числовые поля: интерполяция и заполнение пропусков
    numeric_fields = ['sales_quantity', 'price', 'original_price', 'discount_percentage',
                      'stock_level', 'customer_rating', 'review_count', 'delivery_days']
    for field in numeric_fields:
        df[field] = pd.to_numeric(df[field], errors='coerce')
        df[field] = df.groupby('product_name')[field].transform(
            lambda x: x.interpolate().bfill().ffill()
        )

    df['original_price'] = df.apply(
        lambda row: row['price'] if row['original_price'] == 0 else row['original_price'], axis=1
    )

    # Удаление строк с пропусками в числовых полях после обработки
    df = df.dropna(subset=numeric_fields)

    # Категориальные поля: заполнение пропусков значением 'unknown'
    categorical_fields = ['brand', 'region', 'category', 'seller']
    for field in categorical_fields:
        df[field] = df[field].fillna('unknown')

    # Агрегация данных
    return df.groupby(['product_name', 'date', 'region', 'brand', 'category']).agg({
        'sales_quantity': 'sum',
        'price': 'mean',
        'original_price': 'mean',
        'discount_percentage': 'mean',
        'stock_level': 'mean',
        'customer_rating': 'mean',
        'review_count': 'mean',
        'delivery_days': 'mean',
        'seller': 'first',
        'is_weekend': 'first',
        'is_holiday': 'first'
    }).reset_index()

def create_features(df):
    """Создание признаков для модели."""
    logger.info("Creating features")

    # Добавление временных признаков
    df['day_of_week'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['quarter'] = df['date'].dt.quarter

    # Сортировка данных
    df = df.sort_values(['product_name', 'date'])

    # Лаги и скользящие средние
    lag_periods = [1, 3, 7]
    rolling_periods = [3, 7]

    for product in df['product_name'].unique():
        product_df = df[df['product_name'] == product]
        if len(product_df) < 7:  # Уменьшен порог до 7 записей
            continue

        # Лаги
        for lag in lag_periods:
            df.loc[df['product_name'] == product, f'sales_quantity_lag_{lag}'] = product_df['sales_quantity'].shift(lag)
            df.loc[df['product_name'] == product, f'price_lag_{lag}'] = product_df['price'].shift(lag)

        # Скользящие средние
        for window in rolling_periods:
            df.loc[df['product_name'] == product, f'sales_quantity_rolling_mean_{window}'] = (
                product_df['sales_quantity'].rolling(window=window).mean().values
            )
            df.loc[df['product_name'] == product, f'price_rolling_mean_{window}'] = (
                product_df['price'].rolling(window=window).mean().values
            )

    # Целевые переменные: прогнозирование за 7 дней
    for product in df['product_name'].unique():
        product_df = df[df['product_name'] == product].copy()
        # Изменение цены через 7 дней
        df.loc[df['product_name'] == product, 'price_target'] = product_df['price'].shift(-7)
        # Суммарный спрос за следующие 7 дней
        df.loc[df['product_name'] == product, 'sales_target'] = product_df['sales_quantity'].rolling(
            window=7, min_periods=1
        ).sum().shift(-7)

    # Фильтрация продуктов с менее чем 7 записями
    product_counts = df.groupby('product_name').size()
    df = df[df['product_name'].isin(product_counts[product_counts >= 7].index)]
    return df.dropna(subset=['price_target', 'sales_target'])

def process_data(input_file, output_dir, cutoff_date):
    """Основная функция обработки данных."""
    try:
        # Загрузка и обработка данных
        df = load_data(input_file)
        df = preprocess_data(df)
        df = create_features(df)

        # Разделение на тренировочную и тестовую выборки
        train_df = df[df['date'] < cutoff_date]
        test_df = df[df['date'] >= cutoff_date]

        # Сохранение данных
        os.makedirs(output_dir, exist_ok=True)
        train_df.to_csv(os.path.join(output_dir, 'train_data.csv'), index=False)
        test_df.to_csv(os.path.join(output_dir, 'test_data.csv'), index=False)
        logger.info(f"Data saved to {output_dir}")
        return True
    except Exception as e:
        logger.error(f"Error in processing: {e}")
        return False

if __name__ == "__main__":
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Process marketplace data')
    parser.add_argument('--input', required=True, help='Path to input JSON file')
    parser.add_argument('--output', required=True, help='Output directory')
    parser.add_argument('--cutoff', default='2025-03-20', help='Cutoff date in YYYY-MM-DD format')

    args = parser.parse_args()

    # Запуск обработки
    success = process_data(
        args.input,
        args.output,
        datetime.strptime(args.cutoff, '%Y-%m-%d')
    )
    sys.exit(0 if success else 1)