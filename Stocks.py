import os
import pandas as pd
from yahoo_fin import stock_info as si
from concurrent.futures import ThreadPoolExecutor
import re
from dotenv import load_dotenv

load_dotenv()


def read_input_file(input_file):
    with open(input_file, 'r', encoding='utf-8-sig') as file:
        timestamps = file.read().splitlines()

    cleaned_timestamps = [clean_timestamp(ts) for ts in timestamps]

    return cleaned_timestamps


def clean_timestamp(timestamp):
    cleaned_timestamp = re.sub(r'[^\x20-\x7E]', '', timestamp).strip()
    cleaned_timestamp = re.sub(r'\.\d+', '', cleaned_timestamp)
    return cleaned_timestamp


def fetch_stock_data(timestamp, ticker):
    try:
        date = timestamp.split()[0]
        price_at_time = si.get_data(ticker, start_date=date, end_date=date)

        if not price_at_time.empty:
            open_price = price_at_time.iloc[0]['open']
            close_price = price_at_time.iloc[-1]['close']
            percentage_change = ((close_price - open_price) / open_price) * 100
            return [timestamp, ticker, percentage_change]
        else:
            print(f"No data available for {ticker} on {date}")
            return [timestamp, ticker, None]
    except Exception as e:
        print(f"Error fetching data for {timestamp}, {ticker}: {e}")
        return [timestamp, ticker, None]


def write_to_csv(results, output_file):
    df = pd.DataFrame(results, columns=["timestamp", "stock", "percentage_change"])
    df.to_csv(output_file, index=False)


def main():
    amazon_file = os.getenv('AMAZON_DATES')
    google_file = os.getenv('GOOGLE_DATES')
    bitcoin_file = os.getenv('BITCOIN_DATES')
    output_file = os.getenv('DESTINATION_FILE')

    if not all([amazon_file, google_file, bitcoin_file, output_file]):
        print("Environment variables AMAZON_DATES, GOOGLE_DATES, BITCOIN_DATES, and DESTINATION_FILE must be set.")
        return

    amazon_timestamps = read_input_file(amazon_file)
    bitcoin_timestamps = read_input_file(bitcoin_file)
    google_timestamps = read_input_file(google_file)

    tasks = [(timestamp, "AMZN") for timestamp in amazon_timestamps] + \
            [(timestamp, "BTC-USD") for timestamp in bitcoin_timestamps] + \
            [(timestamp, "GOOGL") for timestamp in google_timestamps]

    results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_stock_data, timestamp, ticker) for timestamp, ticker in tasks]
        for future in futures:
            result = future.result()
            results.append(result)

    write_to_csv(results, output_file)


if __name__ == '__main__':
    main()
