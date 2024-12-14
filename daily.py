import os
import zipfile
import csv
from datetime import datetime
from collections import defaultdict
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
import shutil

TEMP_FOLDER = "./temp_unzip"

# Helper function to initialize dictionary entries
def initialize_data():
    return {"size": 0, "total_weighted_price": 0}

def process_txt_file(txt_file_path, data):
    """Processes a .txt file and updates the provided data dictionary."""
    with open(txt_file_path, 'r', encoding='utf-8') as txt_file:
        reader = csv.reader(txt_file, delimiter='|')
        headers = next(reader, None)  # Skip header if present
        
        for row in reader:
            if len(row) < 8:
                continue
            
            market_center, symbol, date, time, short_type, size, price, link_indicator = row[:8]
            
            try:
                date_obj = datetime.strptime(date, "%Y%m%d")
                day = date_obj.strftime("%Y-%m-%d")
                size = float(size)
                price = float(price)
            except (ValueError, IndexError):
                continue
            
            key = (market_center, symbol, day)
            if key not in data:
                data[key] = initialize_data()
            data[key]["size"] += size
            data[key]["total_weighted_price"] += size * price

def process_zip_file(zip_path):
    """Extracts a zip file, processes its .txt file, and returns consolidated data."""
    data = {}  # Use a regular dictionary instead of defaultdict with lambda
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            zip_file.extractall(TEMP_FOLDER)
            for filename in zip_file.namelist():
                if filename.endswith('.txt'):
                    txt_file_path = os.path.join(TEMP_FOLDER, filename)
                    process_txt_file(txt_file_path, data)
                    os.remove(txt_file_path)  # Clean up the txt file after processing
        print(f"Completed processing: {zip_path}")
    except Exception as e:
        print(f"Error processing {zip_path}: {e}")
        print(traceback.format_exc())
    return data

def consolidate_results(results):
    """Consolidates results from multiple dictionaries into a single aggregated dictionary."""
    consolidated_data = defaultdict(initialize_data)
    for data in results:
        for key, values in data.items():
            consolidated_data[key]["size"] += values["size"]
            consolidated_data[key]["total_weighted_price"] += values["total_weighted_price"]
    return consolidated_data

def save_data(output_csv, data):
    """Saves consolidated data to CSV."""
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["MarketCenter", "Symbol", "Date", "TotalSize", "WeightedAvgPrice"])
        
        for (market_center, symbol, day), values in data.items():
            total_size = values["size"]
            if total_size > 0:
                weighted_avg_price = values["total_weighted_price"] / total_size
                writer.writerow([market_center, symbol, day, total_size, weighted_avg_price])

    print(f"Consolidated data saved to {output_csv}")

def consolidate_short_sale_data(folder_path, output_csv, batch_size=4):
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    zip_files = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path) if filename.endswith('.zip')]

    results = []
    for i in range(0, len(zip_files), batch_size):
        batch = zip_files[i:i + batch_size]
        
        with ProcessPoolExecutor(max_workers=batch_size) as executor:
            future_to_zip = {executor.submit(process_zip_file, zip_path): zip_path for zip_path in batch}
            for future in as_completed(future_to_zip):
                zip_file = future_to_zip[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"Error in processing {zip_file}: {e}")
    
    consolidated_data = consolidate_results(results)
    save_data(output_csv, consolidated_data)

    shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
    print("Temporary files cleaned up.")

# Main entry point
if __name__ == "__main__":
    folder_path = './SSVD'  # Folder containing .zip files with .txt files inside
    output_csv = 'consolidated_daily.csv'  # Name of the output .csv file
    consolidate_short_sale_data(folder_path, output_csv, batch_size=16)
