# Projet Zero NOAA API Data Download

### Description
This Python script is designed to automate the process of downloading data from the NOAA API, handling errors and retries for failed requests. The data retrieved from the API is appended to a CSV file.

### API Documentation

https://www.ncdc.noaa.gov/cdo-web/webservices/v2

### Requirements
- Python 3
- `requests` library
- `csv` module
- An API key for NOAA's National Climatic Data Center (NCDC)

### Setup
1. **API Key**: Place your NOAA API key in a text file located at `../API_Keys/NOAA_Token.txt`. This file is read at the beginning of the script to securely manage your API key.

### How to Use
**Function Call**: The main entry point is the `execute_api_calls_with_retries` function, which requires the following parameters:
   - `base_url`: The base URL for the API. For NOAA, this is typically "https://www.ncdc.noaa.gov/cdo-web/api/v2/".
   - `endpoint`: The specific API endpoint you wish to call (e.g., "/stations/").
   - `headers`: A dictionary containing your API token under the key `"token"`.
   - `initial_parameters`: A dictionary containing the initial query parameters for the API call.
   - `file_path`: Path to the CSV file where results will be stored.
   - `error_log_path`: Path to the CSV file where failed attempts will be logged.
   - `max_retries`: (Optional) The maximum number of retries for a failed API call (default is 3).

### Notes
- The script prints the offset for each API call
- The `execute_api_calls_with_retries` function checks if the target CSV file already exists to prevent accidental data overwriting. If the file exists, the function will not execute, and you must manually manage the file to proceed.
