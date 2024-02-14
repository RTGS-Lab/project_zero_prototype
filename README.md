# Project Zero

## Overview
Project Zero is an integrated platform designed to aggregate and facilitate easy access to climate and weather data by combining multiple API services. The project aims to streamline data downloading and processing tasks through a single interface.

## Repository Structure

- **Depricated_Files**: Contains old files that were used for initial testing and development. These files are retained for historical reference and may not be relevant to the current version of the project.

- **NOAA_API_ETL**: This folder includes a Python script designed to automate the downloading of data from the NOAA API. It handles error logging and retries for failed requests, appending successful data retrievals to a CSV file for further analysis or use.

- **NWS_Flask App**: Contains a Flask application that serves as a frontend interface to the NWS API, allowing users to retrieve and display weather information through a web-based platform.

## Getting Started

### Requirements
- Python 3.x
- Flask for the NWS Flask App
- `requests` library for making API calls
- An API key for NOAA's National Climatic Data Center (NCDC) for the NOAA API ETL script

### Setup and Usage

#### NOAA_API_ETL
1. **API Key**: Store your NOAA API key in a text file located at `./API_Keys/NOAA_Token.txt`. The ETL script reads this file to securely access the API.
2. **Running the Script**: Execute the `execute_api_calls_with_retries` function with the necessary parameters, such as the base URL, API endpoint, headers (including your API token), and file paths for data storage and error logging.

#### NWS_Flask App
1. **Installation**: Ensure Python and Flask are installed on your system.
2. **Running the Application**: Start the Flask application by running the `app.py` file. Access the web interface at `http://127.0.0.1:5000/` to interact with the NWS API through the provided frontend.

## Features and Functionality

- **NOAA_API_ETL Script**: Automates data retrieval from the NOAA API, including error handling and data appending to CSV files.
- **NWS_Flask App**: Provides a user-friendly web interface to display weather information fetched from the NWS API, including station listings, point information, and forecast data.

## Current Status

This is an ongoing project, information is continuously added.
Current task:
* Download data from NOAA
* Create a database
* Create flask app that communicates with the database
