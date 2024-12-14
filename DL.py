import requests
import os
from datetime import datetime, timedelta

# Set the download directory
download_dir = "./SSVD"  # Change this to your path
os.makedirs(download_dir, exist_ok=True)  # Create directory if it doesn't exist

# Define the date range
start_date = datetime(2015, 1, 1)
end_date = datetime(2023, 12, 1)

# Base URL pattern
base_urls = [
    "https://cdn.finra.org/equity/regsho/monthly/FNRAsh{date}.zip",
    "https://cdn.finra.org/equity/regsho/monthly/FNSQsh{date}.zip",
    "https://cdn.finra.org/equity/regsho/monthly/FNSQsh{date}_1.zip",
    "https://cdn.finra.org/equity/regsho/monthly/FNSQsh{date}_2.zip",
    "https://cdn.finra.org/equity/regsho/monthly/FNSQsh{date}_3.zip",
    "https://cdn.finra.org/equity/regsho/monthly/FNSQsh{date}_4.zip",
    "https://cdn.finra.org/equity/regsho/monthly/FNQCsh{date}.zip",
    "https://cdn.finra.org/equity/regsho/monthly/FNYXsh{date}.zip"
]

# Loop over each month in the specified range
current_date = start_date
while current_date <= end_date:
    # Format the date as needed for the URL (e.g., "201501" for January 2015)
    date_str = current_date.strftime("%Y%m")

    for url_pattern in base_urls:
        # Generate the full URL
        file_url = url_pattern.format(date=date_str)

        # Define the local file path
        filename = os.path.join(download_dir, file_url.split("/")[-1])

        # Check if the file already exists to avoid re-downloading
        if not os.path.exists(filename):
            try:
                # Download the file
                response = requests.get(file_url)
                response.raise_for_status()  # Raise an error for failed requests

                # Save the file
                with open(filename, "wb") as file:
                    file.write(response.content)
                print(f"Downloaded {filename}")
            except requests.exceptions.HTTPError:
                print(f"File not found: {file_url}")  # Some files might be missing
        else:
            print(f"File already exists: {filename}")

    # Move to the next month
    current_date += timedelta(days=31)
    current_date = current_date.replace(day=1)  # Ensure it's the first day of the next month
