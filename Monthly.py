import os
import zipfile
import csv
from datetime import datetime
from collections import defaultdict
import traceback

def process_txt_file(txt_file, data):
    # Read the txt file, assuming '|' separator
    reader = csv.reader((line.decode('utf-8').strip() for line in txt_file), delimiter='|')
    
    # Skip header if present
    headers = next(reader, None)
    
    for row in reader:
        # Check if row has exactly 8 columns
        if len(row) < 8:
            print(f"Skipping incomplete row: {row}")
            continue
        
        # Extract relevant columns by position
        market_center, symbol, date, time, short_type, size, price, link_indicator = row[:8]
        
        # Convert date to month and year format
        try:
            date_obj = datetime.strptime(date, "%Y%m%d")
            month_year = date_obj.strftime("%Y-%m")
        except ValueError:
            print(f"Skipping row with invalid date: {row}")
            continue
        
        # Convert size and price to float for calculations
        try:
            size = float(size)
            price = float(price)
        except ValueError:
            print(f"Skipping row with invalid size or price data: {row}")
            continue
        
        # Key for consolidation: market center, symbol, and month-year
        key = (market_center, symbol, month_year)
        
        # Aggregate data
        data[key]["size"] += size
        data[key]["total_weighted_price"] += size * price

def save_partial_data(output_csv, data):
    """Saves the current state of data to CSV."""
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["MarketCenter", "Symbol", "MonthYear", "TotalSize", "WeightedAvgPrice"])
        
        # Write each consolidated row
        for (market_center, symbol, month_year), values in data.items():
            total_size = values["size"]
            if total_size > 0:
                weighted_avg_price = values["total_weighted_price"] / total_size
                writer.writerow([market_center, symbol, month_year, total_size, weighted_avg_price])

    print(f"Partial data saved to {output_csv} after an error.")

def consolidate_short_sale_data(folder_path, output_csv):
    data = defaultdict(lambda: {"size": 0, "total_weighted_price": 0})
    
    try:
        # Loop through each zip file in the folder
        for filename in os.listdir(folder_path):
            if filename.endswith('.zip'):
                zip_path = os.path.join(folder_path, filename)
                print(f"Processing zip file: {filename}")
                
                # Open the zip file
                with zipfile.ZipFile(zip_path, 'r') as zip_file:
                    for zip_info in zip_file.infolist():
                        if zip_info.filename.endswith('.txt'):
                            print(f"Processing txt file: {zip_info.filename}")
                            with zip_file.open(zip_info) as txt_file:
                                process_txt_file(txt_file, data)

    except Exception as e:
        print("An error occurred during processing.")
        print(traceback.format_exc())  # Print the full error traceback
        save_partial_data(output_csv, data)  # Save partial data before exiting
        return  # Exit function after saving partial data in case of error

    # Write consolidated data to the output CSV after successful processing
    save_partial_data(output_csv, data)
    print(f"Consolidated data saved to {output_csv}")

# Example usage:
folder_path = './SSVD'  # Folder containing .zip files with .txt files inside
output_csv = 'consolidated_short_sale_data_full.csv'  # Name of the output .csv file
consolidate_short_sale_data(folder_path, output_csv)
