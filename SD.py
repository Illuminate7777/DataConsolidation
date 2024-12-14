import yfinance as yf
import pandas as pd
from collections import defaultdict
import os
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

# Setup directories and logging
output_dir = "./Stock/SectorData"
os.makedirs(output_dir, exist_ok=True)
logging.basicConfig(filename="data_collection_errors.log", level=logging.ERROR)

# Load tickers from CSV file
tickers_df = pd.read_csv("./Stock/TickersList.csv")
tickers = tickers_df.iloc[:, 0].tolist()  # Assumes tickers are in the first column

# Dictionary to hold data per sector and industry
sector_data = defaultdict(lambda: defaultdict(list))

# Dynamic discount rate placeholder
def calculate_discount_rate(sector, ticker):
    # Placeholder for actual calculation logic
    return 0.08

def fetch_data(ticker):
    stock = yf.Ticker(ticker)
    try:
        # Basic info
        info = stock.info
        sector = info.get("sector", "Unknown")
        industry = info.get("industry", "Unknown")
        market_cap = info.get("marketCap")

        # Validate sector and industry fields
        if sector == "Unknown" or industry == "Unknown":
            logging.error(f"Sector or industry missing for ticker {ticker}")
            return []

        # Monthly data
        hist = stock.history(period="1y", interval="1mo")  # Last 1 year monthly data

        # Skip if no historical data
        if hist.empty:
            logging.warning(f"No available data for {ticker}")
            return []

        # 52-week data and current analyst forecast
        high_52w = info.get("fiftyTwoWeekHigh")
        low_52w = info.get("fiftyTwoWeekLow")
        avg_52w = (high_52w + low_52w) / 2 if high_52w and low_52w else None
        forecast = info.get("targetMeanPrice")

        # Valuation ratios for comparables
        pe_ratio = info.get("trailingPE")
        ps_ratio = info.get("priceToSalesTrailing12Months")
        ev_ebitda = info.get("enterpriseToEbitda")

        # DCF components
        free_cash_flow = info.get("freeCashflow")
        growth_rate = info.get("revenueGrowth")
        discount_rate = calculate_discount_rate(sector, ticker)

        # Append data for each month
        stock_data = []
        for date, row in hist.iterrows():
            data = {
                "Date": date,
                "Ticker": ticker,
                "Sector": sector,
                "Industry": industry,
                "Market Cap": market_cap if market_cap else "N/A",
                "52W High": high_52w if high_52w else "N/A",
                "52W Low": low_52w if low_52w else "N/A",
                "52W Average": avg_52w if avg_52w else "N/A",
                "Analyst Forecast": forecast if forecast else "N/A",
                "P/E Ratio": pe_ratio if pe_ratio else "N/A",
                "P/S Ratio": ps_ratio if ps_ratio else "N/A",
                "EV/EBITDA": ev_ebitda if ev_ebitda else "N/A",
                "Free Cash Flow": free_cash_flow if free_cash_flow else "N/A",
                "Growth Rate": growth_rate if growth_rate else "N/A",
                "Discount Rate": discount_rate,
                "Close": row["Close"],
                "Volume": row["Volume"]
            }

            # Log missing fields
            missing_fields = [field for field, value in data.items() if value == "N/A"]
            if missing_fields:
                logging.warning(f"Missing data for ticker {ticker} on {date} in fields: {missing_fields}")

            stock_data.append(data)
        
        print(f"Completed processing ticker: {ticker}")
        return stock_data

    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return []

# Parallel processing setup
all_data = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_data, ticker): ticker for ticker in tickers}
    
    for future in as_completed(futures):
        ticker_data = future.result()
        all_data.extend(ticker_data)  # Aggregate data from each ticker

# Save data to CSV
main_df = pd.DataFrame(all_data)
main_df.to_csv("all_stocks.csv", index=False)

print("Data collection and CSV creation completed.")
