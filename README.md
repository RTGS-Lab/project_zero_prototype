# Project Zero

## Overview
Project Zero is an integrated platform designed to aggregate and facilitate easy access to climate and weather data by combining multiple API services. The project aims to streamline data downloading and processing tasks through a single interface.

## Repository Structure

- **ETL_Management**: This folder contains the primary code for this project. It contains a Flask application to run the API services and host a web interface, along with ETL Python scripts that automate data checking and downloading thru the Flask app.

- **NOAA_API_PREP**: This folder includes Python scripts designed to automate the downloading of data from the NOAA API and pushing preliminary data to a database. It handles error logging and retries for failed requests, appending successful data retrievals to a CSV file for further analysis or use.

- **NOAA_GRID_PREP**: This folder includes Python scripts designed to automate the downloading of data from the NOAA Gridded FTP Server and pushing preliminary data to a database. It handles error logging and retries for failed requests.

- **AMERIFLUX_PREP**: This folder includes Python scripts designed to automate the downloading of data from the Ameriflux API and pushing station info data to a database.

- **Depricated_Files**: Contains old files that were used for initial testing and development. These files are retained for historical reference and may not be relevant to the current version of the project.


## Getting Started

### Requirements
- Python 3.x
- Jupyter Notebooks to be able to execute .ipynb files.
- All the Python packages listed in `requrements.txt` This can be done by running the command: `pip install -r requirements.txt` in the main directory of this project, or `!pip install -r requirements.txt` when within a Jupyter Notebook.
- Ability to run the `rpy2` Python package and install the `amerifluxr` library. `rpy2` is an interface for running R code in Python, which we use for the Ameriflux API library.
- An API key for NOAA's Climate Data Online API for use within our own the NOAA API ETL. That can be aquired by providing an email to the NOAA [here](https://www.ncdc.noaa.gov/cdo-web/token).
- A database that is setup and credentials. Postgres (with PostGIS optional) is recommended for this project. You can create a local instance [here](https://www.postgresql.org/download/).

### Setup
In short, you only need to input database credentials into `app.py` that is in the `/ETL_Management` folder, and then run the `app.py` file in a terminal to have functionality of the Project Zero API application. A few extra steps are required for pre-loading data and making web interfaces working.

### Database setup:
* Know how to accesss your database. You will need:
    * Host (i.e. localhost or https://example.com)
    * Database name (i.e. postgres)
    * User name (i.e. postgres or myusername)
    * Password
* **EDIT** your `app.py` file to include your database credentials!
* Load weather stations for NOAA web interface
    * Run the cells of the `NOAA_API_LOAD_DB.ipynb` file in the `/NOAA_API_PREP` folder. More information is contained within the file, but a few changes are necessary as you run the file:
        * Add in database credentials at the top of the file.
        * Add in NOAA API key at the top of the file.
        * Edit the FIPS code when downloading weather station data (second to last cell) to your area(s) of choice, or make empty to download all station locations.
* Load flux stations for Ameriflux web interface
    * Run the cells of the `AMERIFLUX_LOAD_DB.ipynb` file in the `/AMERIFLUX_PREP` folder. More information is contained within the file, but a few changes are necessary as you run the file:
        * Add in database credentials at the top of the file
        * Adjust the `os.environ['R_HOME']` line to ensure R code works in your environment

### Running R Code
* Getting R code to run in Python notebooks can be a challenge. An important line of code that may be needed to edit is `os.environ['R_HOME']` in the Ameriflux files (`AMERIFLUX_LOAD_DB.ipynb` near the first chunk of code and `ameriflux_etl_manager.py` near the second set of import statements). `os.environ['R_HOME']` should be set to match where the `bin, doc, etc, src, ...` folders are for your computer's installation version of R, or follow the `rpy2` docs to ensure the code runs.

### Data download folders
* It may be necessary to create folders for the data you donwload, as they are not stored in the github repo. Here is where data is stored for each API:
    * NOAA Point data: stored in database, follow instructions above to prep database
    * NOAA Gridded data: stored in `/GRID_DATA` 
    * Ameriflux data: By default stored in `/AMF_DATA` based on `out_dir` argument of the AMF API

### Running the application
* To run the Project Zero tool, navigate to the `/ETL_Management` in terminal, or some method of running pyhton code.
* Run the `app.py` file using the command `python app.py`
* This should start the Flask applicaiton, and you can make requests via the API or Web Interface to your `localhost` or IP address that the application is running on.

## Applicaiton Interaction
Project Zero currently has three ETL managers implemented to interact with these data sources:
* The NOAA GHCNd climate data network
* The NOAA NClimGrid-Daily gridded climate data
* The Ameriflux BASE flux tower data network

A user can interact via web requests to the custom made Flask Applicaiton API, or use the web user interface.

### Two-Step Process
The applicaiton is designed to be interacted in two steps (two API calls) when requesting specific data. It's likely a user wants to download a LOT of data, so it is best practice to check the data request before proceeding to download all observations.
1. **Check metadata of data request**. Before calling a full data download, it is good to check if the dataset exists, is complete, and you are downloading what you expect. This is done by a `FALSE` for request parameter `direct_download`.
    * For the NOAA GHCNd dataset, this means checking completeness of chached data in the database, then loading any missing data by pulling from the API in a seperate background thread.
    * For the NOAA NClimGrid-Daily dataset, this means checking if all expected files are present in the server before downloading them locally.
    * For the Ameriflux BASE dataset, this means retrieving data type completeness metadata for each requested station.
* If all the data looks good to a user, they can proceed to download the data.
2. **Download the data**. After the request is verified, a user can change the single request parameter `direct_download` to download in `CSV` or `JSON` format. This will then load all the data, perform necessary aggregations, and serve the result.

### Flask API
A user can make request calls to the running flask applicaiton to generate and download data. There are many parameters that can be called in the flask application, more details of these parameters can be found in the `noaa_api_call.py` file in `/ETL_Management/resources`. In short, a user can define the stations/locations, time, datatypes, and data formats for data download. The `API_test.ipynb` file in the `/ETL_Management` folder contains full example API calls that can be modified and ran. API calls are made to the Flask Application's `/NOAA_API_CALL` endpoint.

### Web interface
A user can make similar data calls using the web user interface. When a user goes to the Flask Applicaiton page in a web browser (`localhost:5000/` or the Application's IP address), it will default to the web interface for the NOAA GHCNd dataset. A user can go to the `/grid` endpoint (`localhost:5000/grid`) to interact with the NOAA NClimGrid-Daily dataset. A user can go to the `/AMF` endpoint to interact with the Ameriflux BASE dataset.

## Current Status
The initial implementation of this project is complete, though future updates will refine project scope, clarity, and ease of use.

# Aknowledgements

Funding provided by LCCMR