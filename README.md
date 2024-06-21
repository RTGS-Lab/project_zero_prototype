# Project Zero

## Overview
Project Zero is an integrated platform designed to aggregate and facilitate easy access to climate and weather data by combining multiple API services. The project aims to streamline data downloading and processing tasks through a single interface.

## Repository Structure

- **ETL_Management**: This folder contains the primary code for this project. It contains a Flask application to run the API services and host a web interface, along with ETL Python scripts that automate data checking and downloading thru the Flask app.

- **SETUP_DB**: This folder contains two .ipynb files designed to prep the user database for applicaiton interaction in the NOAA GHCND dataset and Ameriflux stations.

- **System_Architecture**: This folder contains images with system architecture diagrams, notably ETL diagrams for data aquisition for each data source.

- **Deprecated_Files**: Contains old files that were used for initial testing and development. These files are retained for historical reference and may not be relevant to the current version of the project.

## Getting Started

### Requirements
- Python 3.11.5 or higher. 3.11.5 is what the application has been developed on, results may vary with older versions.
- Jupyter Notebooks to be able to execute .ipynb files.
- All the Python packages listed in `requirements.txt` This can be done by running the command: `pip install -r requirements.txt` in the main directory of this project, or `!pip install -r requirements.txt` when within a Jupyter Notebook.
    - rasterio may have trouble installing on Windows, instructions [here](https://rasterio.readthedocs.io/en/latest/installation.html) may help.
- Ability to run the `rpy2` Python package and install the `amerifluxr` library. `rpy2` is an interface for running R code in Python, which we use for the Ameriflux API library. **R IS REQUIRED TO UTILIZA AMERIFLUX DATA** You may skip installing R, but will be unable to utilize the ameriflux data source.
- An API key for NOAA's Climate Data Online API for use within our own the NOAA API ETL. That can be aquired by providing an email to the NOAA [here](https://www.ncdc.noaa.gov/cdo-web/token).
- A database that is setup and credentials. Postgres (with PostGIS optional) is recommended for this project. You can create a local instance [here](https://www.postgresql.org/download/).

### Setup
With this project prototype, there are a few files that require manual modificaiton. Assuming all the above requirements are satisfied, here is a step by step setup.

### 1. Database setup:
* Know how to accesss your database. You will need:
    * Host (i.e. localhost or https://example.com)
    * Database name (i.e. postgres)
    * User name (i.e. postgres or myusername)
    * Password
* **EDIT** your `app.py` file to include your database credentials!
* Load weather stations for NOAA web interface
    * Run the cells of the `NOAA_API_LOAD_DB.ipynb` file in the `/SETUP_DB` folder. More information is contained within the file, but a few changes are necessary as you run the file:
        * Add in database credentials at the top of the file.
        * Add in NOAA API key at the top of the file.
        * Edit the FIPS code when downloading weather station data (second to last cell) to your area(s) of choice, or make empty to download all station locations.
* Load flux stations for Ameriflux web interface
    * Run the cells of the `AMERIFLUX_LOAD_DB.ipynb` file in the `/SETUP_DB` folder. More information is contained within the file, but a few changes are necessary as you run the file:
        * Add in database credentials at the top of the file
        * Adjust the `os.environ['R_HOME']` line to ensure R code works in your environment

### 2. Running R Code
* Getting R code to run in Python notebooks can be a challenge. An important line of code that may be needed to edit is `os.environ['R_HOME']` in the Ameriflux files (`AMERIFLUX_LOAD_DB.ipynb` near the first chunk of code and `ameriflux_etl_manager.py` near the second set of import statements). `os.environ['R_HOME']` should be set to match where the `bin, doc, etc, src, ...` folders are for your computer's installation and version of R, or follow the `rpy2` docs to ensure the code runs.

### 3. Data download folders
* It may be necessary to create folders for the data you download, as they are not stored in the github repo. Here is where data is stored for each API:
    * NOAA Point data: stored in database, follow instructions above to prep database
    * NOAA Gridded data: stored in `/GRID_DATA` 
    * Ameriflux data: By default stored in `/AMF_DATA` based on `out_dir` argument of the AMF API

### 4. Running the application
* To run the Project Zero tool, navigate to the `/ETL_Management` in terminal, or some method of running pyhton code.
* Run the `app.py` file using the command `python app.py`
* This should start the Flask applicaiton, and you can make requests via the API or Web Interface to your `localhost` or IP address that the application is running on.

## Usage
Project Zero currently has three ETL managers implemented to interact with these data sources:
* The NOAA GHCNd climate data network
* The NOAA NClimGrid-Daily gridded climate data
* The Ameriflux BASE flux tower data network

A user can interact via web requests to the custom made Flask Applicaiton API, or use the web user interface.

### The two-step process for downloading data
The applicaiton is designed to be interacted in two steps (two API calls) when requesting specific data. It's likely a user wants to download a LOT of data, so this application is desinged to have a user check their data before proceeding to download all observations. **The applicaiton may not work if the metadata has not been checked**
1. **Check metadata of data request**. Before calling a full data download, it is good to check if the dataset exists, is complete, and you are downloading what you expect. This is done by a `FALSE` for request parameter `direct_download`.
    * For the NOAA GHCNd dataset, this means checking completeness of cached data in the database, then loading any missing data by pulling from the API in a separate background thread.
    * For the NOAA NClimGrid-Daily dataset, this means checking if all expected files are present in the server before downloading them locally.
    * For the Ameriflux BASE dataset, this means retrieving data type completeness metadata for each requested station.
* If all the data looks good to a user, they can proceed to download the data.
2. **Download the data**. After the request is verified, a user can change the single request parameter `direct_download` to download in `CSV` or `JSON` format. This will then load all the data, perform necessary aggregations, and serve the result.

### Web interface
The simplest way to gather data is through the web interface. This is accessed When a user goes to the Flask Applicaiton page in a web browser.

#### 1. Going to the webpage
Go to `localhost:5000/` or the Application's IP address. The default page is the web interface for the NOAA GHCNd dataset. A user can go to the `/grid` endpoint (`localhost:5000/grid`) to interact with the NOAA NClimGrid-Daily dataset. A user can go to the `/amf` endpoint (`localhost:5000/amf`)to interact with the Ameriflux BASE dataset.

#### 2. The Map
The first thing you see on the webpage is a map. Depending on which dataset you load, it will be populated with different items:

* NOAA GHCNd will load all station points that you have loaded to your database. Use the bounding box or list to select stations.
* NOAA NClimGrid will be an empty map. Use the bounding box to select an area to download data from.
* Ameriflux BASE will load all Ameriflux stations that you have loaded to your database. Use the bounding box or list to select stations.

To interact with the map, you can draw a bounding box using the square button on the left hand side of the map. This allows you to draw a bounding box and select the stations or area you wish to download data from. Alternatively, if you know the station ID, you can select 'List' to list out the names of stations you wish to download data from.

#### 3. Inputs
The next thing on the webpage is a user input form. Depending on which dataset you load, it will be populated with different items specific to the data interface you are interacting with. There are three sections:

* API Parameters: This section contains necessary inputs to make the API call.
    * Start date (required): The starting date that we want to look at our data
    * End date (required): The ending date that we want to look at our data
    * Data Type ID's (optional): If we know specific data columns we want to download, we can select just columns of interest (i.e. PRCP, or TAVG)
    * **(Ameriflux only)** User ID, User Email, Data Policy, Agree to Policy, Intended Use, Intended Use Description, Output Directory: These are all parameters unique to the Ameriflux API which are not used in other API calls.

* Additional Parameters: This section contains necessary inputs to aggregate and clean the data to how the user would like it.
    * Aggregation Time Type (required): What temporal resolution the user would like to aggregate to
    * Data Type & Aggregation Style (optional): Input a given data variable, and how the user would like that variable to be aggregated by. By default, all variables will be aggregated by MEAN
        * The NOAA NClimGrid dataset only reports 4 variables, so they are already listed on the website.
    * Dataframe format (optional): How the user would like their dataframes to be formatted when outputting data. By default it is Wide format (timestamp, var1, var2, var3 ...). Tall format can also be chosen for the NOAA datasets (timestamp, var1; timestamp, var2; timestamp, var3; ...).
    * **Direct Download**: This is the IMPORTANT selection between downloading metadata versus data output (as described in the 2 step process).
        * A user should first submit the form using the "Metadata" option to ensure their request looks good and data is ready/accessible. This will output a JSON file, reporting from each step of the code's ETL pipeline process.
        * Once the data looks good (code is complete and/or user is confident in their request), they can choose an output of JSON or CSV for their data. IT will perform the necessary downloads and post the data to the user.

* Credentials
    * These are items specific to the ETL pipelines of each dataset. The check boxes are different stages of data processing when gathering data from the data sources. By default, leave these checked.
    * The NOAA GHCNd data source also has database credentials and NOAA API key inputs, these are required.

#### Web interface example
 There is a `/screenshots` folder, which will provide detail for this example. Say I would like to download climate data near Rosemont Minnesota, just south of the Twin Cities Metro. I want to compare each week's Maximum and Minimum Temperature for each dataset from February 9, 2023 to December 20, 2023.
* Load the web user interface (`localhost:5000`), I am at the NOAA GHCNd dataset.
* I zoom to find the stations I wish to download data from, and highlight them with a bounding box.
* I input all necessary parameters and leave the 'Metadata' option of the 'Direct Download' field selected. (`GHCND.png`)
* I look at the metadata to see that my database is empty with 0 of the 628 observations cached. The ETL pipeline has already started to fill in missing observations, so I wait a minute and re-submit. (`GHCND_meta.png`)
* When I re-submit, the database has all values and the data is complete. I want to download this as a CSV, so I switch the 'Direct Download' to 'CSV' This dataset is fairly small, so it downloads almost instantly. The CSV shows the aggregated week's end date (i.e. Feb. 13-19 for the date 2/19/2023) and the observed MIN and MAX temperature values. (`GHCND_data.png`)
* To download froom the NOAA NClimGrid API, I go to the `/grid` endpoint (`localhost:5000/grid`). Here I draw a similar bounding box and fill in the items (`GRID.png`).
* Looking at the metadata, we can see the remote server should have all the files needed to complete the query, so we download as 'CSV' again (`GRID_meta.png`)
* Note, this dataset takes longer to load as it downloads full NetCDF files and also takes an extensive amount of computer memory to aggregate.
* Finally, we download from the Ameriflux BASE API, I got to the `/amf` endpoint (`localhost:5000/grid`). Here I draw a similar bounding box and fill in the items (`AMF.png`).
* The metadata saves a file labelled `metadata.csv`, where it shows each variable type and reported completeness by the Ameriflux API. We can then change to CSV and download the data.

### Flask API
A user can make API request calls to the running flask applicaiton to generate and download data without having to deal with the web interface. There are many parameters that can be called in the flask application, similar to the web interface. More details of these parameters can be found in the `noaa_api_call.py` file in `/ETL_Management/resources`. In short, a user can define the stations/locations, time, datatypes, and data formats for data download. The `API_test.ipynb` file in the `/ETL_Management` folder contains full example API calls that can be modified and ran. API calls are made to the Flask Application's `/NOAA_API_CALL` endpoint.

## Current Status
The initial implementation of this project is complete, though future updates will refine project scope, clarity, and ease of use.

## Acknowledgements

Funding for this project was provided by the Minnesota Environment and Natural Resources Trust Fund as recommended by the Legislative-Citizen Commission on Minnesota Resources (LCCMR) project ML 2021, Chp6, Art6, Sec 2, 04e-E812SIM 2021-266.
