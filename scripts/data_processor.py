import pandas as pd
import json
import os
import sys
import logging
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('DataProcessor')

def load_data(input_file):
    logger.info(f"Loading data from {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return pd.DataFrame(data)

def preprocess_data(df):
    logger.info("Starting data preprocessing")

    df['date'] = pd.to_datetime(df['date'])

    numeric_fields = ['sales_quantity', 'price', 'original_price', 'discount_percentage',
                      'stock_level', 'customer_rating', 'review_count', 'delivery_days']
    for field in numeric_fields:
        df[field] = pd.to_numeric(df[field], errors='coerce')
        df[field] = df.groupby('product_name')[field].transform(lambda x: x.fillna(x.mean()))

    df = df.dropna(subset=numeric_fields)

    categorical_fields = ['brand', 'region', 'category', 'seller']
    for field in categorical_fields:
        df[field] = df[field].fillna('unknown')

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
    logger.info("Creating features")

    df['day_of_week'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['quarter'] = df['date'].dt.quarter

    df = df.sort_values(['product_name', 'date'])

    lag_periods = [1, 3, 7]
    rolling_periods = [3, 7]

    for product in df['product_name'].unique():
        product_df = df[df['product_name'] == product]
        if len(product_df) < 14:
            continue

        for lag in lag_periods:
            df.loc[df['product_name'] == product, f'sales_quantity_lag_{lag}'] = product_df['sales_quantity'].shift(lag)
            df.loc[df['product_name'] == product, f'price_lag_{lag}'] = product_df['price'].shift(lag)

        for window in rolling_periods:
            df.loc[df['product_name'] == product, f'sales_quantity_rolling_mean_{window}'] = (
                product_df['sales_quantity'].rolling(window=window).mean().values
            )
            df.loc[df['product_name'] == product, f'price_rolling_mean_{window}'] = (
                product_df['price'].rolling(window=window).mean().values
            )

    for product in df['product_name'].unique():
        product_df = df[df['product_name'] == product].copy()
        df.loc[df['product_name'] == product, 'price_target'] = product_df['price'].shift(-7)

        for idx, row in product_df.iterrows():
            next_7_days = product_df[(product_df['date'] > row['date']) &
                                     (product_df['date'] <= row['date'] + timedelta(days=7))]
            df.loc[idx, 'sales_target'] = next_7_days['sales_quantity'].sum()

    product_counts = df.groupby('product_name').size()
    df = df[df['product_name'].isin(product_counts[product_counts >= 14].index)]
    return df.dropna(subset=['price_target', 'sales_target'])

def normalize_features(df, output_dir):
    logger.info("Normalizing features")
    numeric_features = ['price', 'original_price', 'discount_percentage', 'stock_level',
                        'customer_rating', 'review_count', 'delivery_days']

    scaler = StandardScaler()
    df[numeric_features] = scaler.fit_transform(df[numeric_features])

    with open(os.path.join(output_dir, 'scaler_params.json'), 'w') as f:
        json.dump({
            'mean': scaler.mean_.tolist(),
            'scale': scaler.scale_.tolist(),
            'features': numeric_features
        }, f)

    return df

def process_data(input_file, output_dir, cutoff_date):
    try:
        df = load_data(input_file)
        df = preprocess_data(df)
        df = create_features(df)
        df = normalize_features(df, output_dir)

        train_df = df[df['date'] < cutoff_date]
        test_df = df[df['date'] >= cutoff_date]

        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(os.path.join(output_dir, 'processed_data.csv'), index=False)
        logger.info(f"Data saved to {output_dir}")
        return True
    except Exception as e:
        logger.error(f"Error in processing: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process marketplace data')
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--cutoff', default='2025-03-20')

    args = parser.parse_args()

    success = process_data(
        args.input,
        args.output,
        datetime.strptime(args.cutoff, '%Y-%m-%d')
    )
    sys.exit(0 if success else 1)