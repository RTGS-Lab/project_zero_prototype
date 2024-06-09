"""
NOAA Gridded ETL Management Program
V1.0 (14 May 2024)
Logan Gall, gall0487@umn.edu

This file serves as a functional Extract, Transform, Load (ETL) tool for a custom-built NOAA NClimGridded-Daily Dataset.
This code contains multiple functions that all work together to process a user's NOAA API request.
The primary function in this code is process_request, which takes in external API arguments, and processes these argumetns to check, download, and aggregate data from the NOAA database.
"""

#Imports
#psycopg2 for database connection
import psycopg2
#requests for NOAA API calls
import requests
#datetime for data aggregation by date
from datetime import datetime, timedelta
#pandas for data aggregation
import pandas as pd
import geopandas as gpd
#rasterio and shapefly for raster manipulation
import rasterio
from shapely.geometry import box
from rasterio.transform import from_origin
#threading to update database with new data in the background
import threading
#Flask for api response return
from flask import Flask, Response
#json for api response return
import json
#time for sleep
import time
#os for file paths
import os
#xarray for netcdf file management
import xarray as xr
#glob for file paths
import glob

"""
Class GRIDETLManager
Global variables:
    self.args
        - Arguments passed in by user to the NOAA API. There are many, so they are defined in the file noaa_api_call.py
        - There are many Call_X functions, these allow a user to call or bypass any optional data checks in the ETL manager. Typically they will remain active, unless a user has problems with a specific data check.
        - Typical users will input 'Endpoint' (NOAA_GRID_DATA), 'Call_Direct_Download' (FALSE for checking data, CSV or JSON for downloading data after check) 'API_Arguments' (define data they want), 'Additional_Arguments' (define how they want aggregation).
    self.response_codes
        - For every section of data checking that occurs, the response codes store the failure/success of the function call. This is used for debugging and indicating to the user what is happening during the data processing pipeline.

Typically, the only function called in this class externally is process_request(self), which will take all the args and perform a data processing pipeline and either return downloaded data or the response codes indicating how the status of the data checking.

This is a MODIFIED version of the NOAAETLManager class, so items such as database calls remain in the code, yet do not produce results. This is to ensure the program does not reach unexpected errors from generating a fully new and unique class.
"""
class GRIDETLManager:
    #initialization of the global variables
    def __init__(self, args):
        #All of the API arguments stored in the self.args variable
        self.args = args
        #Response codes for reporting data checking calls
        self.response_codes = {}

    
    #Data processing pipeline.
    ##First checks for direct download, if FALSE goes through optional parameters for data checking
    def process_request(self):
        #Database values, stores the response returned by the database -- in this implementation, it is the files from the FTP server
        db_vals = None
        #API values, stores the response returned by API calls
        api_vals = None
        #Completeness check, tells if data is complete between FTP Server and api
        complete = None
        #Argument translation. Calls the translate_endpoint function
        ##Returns a dictionary with extra parameters that are key for this specific data download endpoint
        ##Ex. URL of API, and translation from API to database (startdate : X to SQL date >= X)
        arg_trans = self.translate_endpoint(self.args['Endpoint'])
        self.args['API_Arguments']['datatypeid'] = self.args['API_Arguments']['datatypeid'].lower()

        #If Call_Direct_Download is CSV or JSON --> start our data download process
        if (self.args['Call_Direct_Download'] == 'CSV') or (self.args['Call_Direct_Download'] == 'JSON'):
            #full_call calls generate_api_call, which takes in the user's api parameters and formats it in a 'requests' API call
            full_call = self.generate_api_call(arg_trans, self.args['API_Arguments'], self.args['NOAA_API_KEY'])
            
            #api_vals call api_call, which takes in the formatted call generated above
            ##db_vals returns as the count of rows of data in the API call
            db_vals = self.api_download(full_call['url'], full_call['endpoint'], full_call['headers'], full_call['parameters'])
            files = glob.glob(db_vals + '//*.nc')
            #check if db_vals was able to get data from the FTP Server
            if db_vals is not None:
                #if Call_Aggregation is True, then start aggregating the data based on additional arguments
                if self.args['Call_Aggregation']:
                    #call aggregate_data function to aggregate and clean data for user
                    db_vals = self.aggregate_data(db_vals, arg_trans, self.args['Additional_Arguments'], self.args['API_Arguments'])

            #if we want JSON, return in JSON format
            if (self.args['Call_Direct_Download'] == 'JSON'):
                json_vals = db_vals.to_json(orient="records", lines = False, indent = 4)
                #return the resulting rows from the database call
                return json_vals

            #if we want CSV, return in CSV format
            elif (self.args['Call_Direct_Download'] == 'CSV'):
                # Convert DataFrame to CSV
                csv_data = db_vals.to_csv(index=False)  # Set index=False if you don't want the DataFrame index in the file
            
                # Create a response with the CSV data
                return Response(
                    csv_data,
                    mimetype='text/csv',
                    headers={'Content-Disposition': 'attachment; filename="dataframe.csv"'}
                )

    ##Above return ends the process_request function. The function will not continue if Call_Direct_Download is called.
    ##Below are data checking calls. They all default to True in a typical user request.

        #Call Database
        ##This function is redundant code from the NOAA API. This can be run, but is not used in this ETL manager.
        if self.args['Call_DB']:
            #conn calls db_connect, which creates an open connection to our database with psycopg2
            conn = self.db_connect(self.args['DB_Credentials'])
            #sql calls generate_sql, which generatees an sql query for the user inputted parameters
            sql = self.generate_sql(translation = arg_trans,
                                api_arguments = self.args['API_Arguments'])
            #db_vals calls execute_sql which runs the above generated sql statements AND returns the count of all the rows (download defaults to False)
            db_vals = self.execute_sql(sql, conn)
            #close the connection to the database
            try:
                conn.close()
            except Exception as e:
                print('no database to disconnect')
                
        #Call API
        ##in order to check for data completeness, we want to count how many rows the NOAA FTP Server has for this specific request
        if self.args['Call_API']:
            #full_call calls generate_api_call, which takes in the user's api parameters and formats it in a FTP server API call
            full_call = self.generate_api_call(arg_trans, self.args['API_Arguments'], self.args['NOAA_API_KEY'])
            #using length of url list as a proxy for how many files we expect to download
            db_vals = full_call['endpoint']
            #api_vals call api_call, which takes in the formatted call generated above
            ##returns the count of rows of data in the API call
            api_vals = self.api_call(full_call['url'], full_call['endpoint'], full_call['headers'], full_call['parameters'])

        #Call Completeness check
        ##if both databse and api are called, it will check if they have the same number of rows
        if self.args['Call_Completeness']:
            #complete calls check_completeness, can be True (data is comlete), False (data is incomplete), or None (db_vals or api_vals is None)
            complete = self.check_completeness(db_vals, api_vals)
        
        #Call filling incomplete data
        ##This function does not do anything for the NOAA Gridded data.
        if self.args['Call_Fill_Incomplete']:
            self.response_codes['Fill_Incomplete'] = "database not implemented for Gridded Data."

        #After all the data checks are complete, return the response codes (metadata) for all the data checks
        ##This will be a dictionary with keys for each data check, the value is the status of that data check
        ##ex. response_codes['api_call'] : 'API returned 691'
        return json.dumps(self.response_codes, indent = 4)

    
    #translate_endpoint function
    ##this function provides mappings for these items based on the user inputted 'Endpoint':
    ###table -- the table that we are downloading data from in the database
    ###sql -- translate API parameters to SQL query. ex. 'statdate = X' in the API wil translate to 'date >= X' in the SQL query
    ###api -- provide url, endpoint, and data to be downloaded from the NOAA API
    ###aggregation -- user options to aggregate the data by date, swaps it to single character for pandas resample/aggreagtion
    def translate_endpoint(self, endpoint):
        #Mappings of relevent details for each API/DB Call
        endpoint_mappings = {
            'NOAA_GRID_DATA': {
                'table': 'grid_data',
                'sql': {
                    'datatypeid' : 'datatype ',
                    'startdate' : 'time >',
                    'enddate' : 'time <'
                },
                'api' : {
                    'url' : 'https://www.ncei.noaa.gov/data/nclimgrid-daily/access/grids/'
                },
                'aggregation' : {
                    'daily': 'D',
                    'weekly': 'W',
                    'monthly': 'M',
                    'yearly': 'Y'
                }
            },
            # Add more mappings as needed
        }
        #dat is the dictionary that corresponds to the user-provided endpoint, defaults to None if user's 'Endpoint' is not in the dictionary list
        dat = endpoint_mappings.get(endpoint, None)
        #check if dat is filled
        if dat is not None:
            #set response code to True (successfully mapped data)
            self.response_codes['translatiion'] = True
        #if dat is None, it failed.
        else:
            #set resposnse code to False (data did not get mapped right)
            ##The following functions will probably break if this fails
            self.response_codes['translation'] = False
        #returns dictionary or None
        return dat

    
    #database connection function
    ##creates and returns a database connection using psycopg2
    ##input: db_credentials -- dictionary containing 'dbname', 'user', 'password', 'host', and 'port'
    ##output: database connection object (if successful) or None (if fails)
    ###This is redundant code in the NOAA Gridded ETL manager.
    def db_connect(self, db_credentials):
        #end function early for gridded ETL manager
        self.response_codes['DB_Connect'] = f"database not implemented for Gridded Data."
        return None

    
    #generate sql query function
    ##creates a sql query that can download all the data for the given user inputs
    ##input: translation -- the translation dictionary from translate_endpoint function
    ##input: api_argumets -- the user-inputted api arguments that follow NOAA API's input scheme.
    ##output: a sql query dictionary with 'SELECT', 'FROM', and 'WHERE' keys.
    ###This is redundant code in the NOAA Gridded ETL manager.
    def generate_sql(self, translation, api_arguments):
        self.response_codes['generate_sql'] = f"database not implemented for Gridded Data."
        return None

    
    #execute a given SQL statement
    ##This function has dual purpose: count rows of the given query (download = False) OR return all data as pandas dataframe (download = True)
    ##input: sql_dict -- dictionary containing sql query with keys 'SELECT', 'FROM', and 'WHERE'
    ##input: connection -- psycopg2 database connection
    ##input: download -- indicator to count rows or download data (defaults to false)
    ##output: all the data in pandas dataframe (if download = True), count of rows in query (if download = False), OR None (if an error occurs)
    ###This is redundant code in the NOAA Gridded ETL manager.
    def execute_sql(self, sql_dict, connection, download = False):
        self.response_codes['Execute_SQL'] = f"database not implemented for Gridded Data."
        return None

    
    #aggregate data funciton
    ##This function aggregates data based on date and performs small data cleaning for the user
    ##input: df -- a filepath where all the data files are located
    ##input: translation -- mapping of data for the given endpoint, in this case we are looking at converting user inputted 'time' to single characters (ex below)
    ##input: additional_arguments -- extra arguments specific to our custom NOAA Gridded API. in this case, looking at time step to aggregate values (ex below) and how to aggregate each data type (ex2 below)
    ###ex. additional_arguments['aggregation']['time'] = 'weekly' --> translation['aggregation']['weekly] : 'W' (turn 'weekly' aggregation to 'W' character)
    ###ex2. additional_arguments['aggregation']['prcp'] = 'SUM' and additional_arguments['aggregation']['tavg'] = 'MEAN' --> SUM the PRCP column and MEAN the TAVG column
    def aggregate_data(self, df, translation, additional_arguments, api_parameters):
        #create a list of all downloaded netcdf files
        files = glob.glob(df + '//*.nc')
        #open multiple NetCDF files, concatenate them along the 'time' dimension
        combined_ds = xr.open_mfdataset(files, concat_dim='time', combine='nested')

        #select the data variabless. For the NOAA data, these are all the reported values
        data_vars = combined_ds[['tmin', 'tmax', 'tavg', 'prcp']]

        #convert the xarray Dataset to a pandas DataFrame
        df = data_vars.to_dataframe().reset_index()

        print(df.head())
        
        #ensure 'date' column is datetime type for proper resampling
        df['time'] = pd.to_datetime(df['time'])
        #determine the aggregation time step. Look at user additional_arguments, and translate that to single character. (ex above)
        if additional_arguments is not None:
            freq = translation['aggregation'].get(additional_arguments['aggregation']['time'], 'D')    
        else:
            freq = 'D'

        #subset to time we care about
        start = datetime.strptime(api_parameters['startdate'], "%Y-%m-%d")
        end = datetime.strptime(api_parameters['enddate'], "%Y-%m-%d")
        # Subset the DataFrame to the time range we care about
        df = df[(df['time'] >= start) & (df['time'] <= end)]
        print(len(df))

        #default aggregation methods for our data
        agg_methods = {'tmin': 'mean', 'tmax': 'mean', 'tavg': 'mean', 'prcp': 'sum'}
        #subset columns we want to keep
        if api_parameters['datatypeid'] is not None or api_parameters['datatypeid'] != '':
            #find all the data column names that we want to keep
            data_columns = api_parameters['datatypeid'].split(',')
            #constant columns that will always be returned
            fixed_columns = ['time', 'lat', 'lon']
            #keep the constant columns plus the data we want
            keep = fixed_columns + data_columns
            #subset the dataframe
            df = df[keep]
            #filter agg_methods to include only the relevant aggregation methods for the datatype_columns
            agg_methods = {key: agg_methods[key] for key in data_columns if key in agg_methods}
            
        #convert DataFrame to GeoDataFrame
        df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
        #setting CRS to WGS 84
        df.set_crs(epsg=4326, inplace=True)
        #pick bounding box for aggregation
        if additional_arguments is not None:
            if additional_arguments['box'] is not None:
                minlat = additional_arguments['box']['minlat']
                minlon = additional_arguments['box']['minlon']
                maxlat = additional_arguments['box']['maxlat']
                maxlon = additional_arguments['box']['maxlon']

                bounding_box = box(minlon, minlat, maxlon, maxlat)
                
                df = df[df.geometry.within(bounding_box)]

        print(df.head())
        #initialize an empty list to store the aggregated DataFrames
        aggregated_dfs = []

        if (additional_arguments is not None) and ('aggregation' in additional_arguments):
            print('true')
        else:
            print('false')
        #check to see if there's a custom aggregation method passed in additional_arguments
        if (additional_arguments is not None) and ('aggregation' in additional_arguments):
            for key, method in additional_arguments['aggregation'].items():
                print(key)
                if key in agg_methods:
                    agg_methods[key] = method  #ensure only valid columns are included
        #convert 'time' column to datetime if not already
        df['time'] = pd.to_datetime(df['time'])

        if additional_arguments is not None and 'dropNA' in additional_arguments:
            if additional_arguments['dropNA'] == True:
                df.dropna(inplace=True)
        
        #group by 'lat', 'lon' and resample by 'time'
        grouped = df.set_index('time').groupby(['lat', 'lon']).resample(freq, dropna=False)
        
        #aggregating using the specified methods
        aggregated = grouped.agg(agg_methods)
        
        #reset index after aggregation
        aggregated = aggregated.reset_index()
        
        #convert date back to a readable text for the user
        aggregated['time'] = aggregated['time'].dt.strftime('%Y-%m-%d')

        result_df = aggregated
        
        #check for requested dataframe format for the resulting data
        if additional_arguments is not None:
            if additional_arguments.get('format') == 'long':
                #reshape the DataFrame to have 'datatype' and 'value' columns
                result_df = aggregated.melt(id_vars=['time', 'lat', 'lon'], var_name='datatype', value_name='value')
            elif additional_arguments.get('format') == 'wide':
                #if wide format is requested or no format is specified, use the wide format as default
                result_df = aggregated
        print(result_df)
        #return the data
        return result_df

    
    #generate api call function
    ##this function creates the URL list to download files from the FTP Server
    ##input: translation -- endpoint translaiton that provides the base URL and endpoint for this API call
    ##input: api_parameters -- user-inputs for API call, updates the default parameters
    ##input: noaa_api_key -- the api key to let the user connect to the NOAA API #not used for the gridded dataset
    ##output: full_call dictionary with keys 'url', 'endpoint', 'headers', and 'parameters' -- designed to be placed directly into a requests API call
    def generate_api_call(self, translation, api_parameters, noaa_api_key):
        #find what years & months need to be downloaded
        start = datetime.strptime(api_parameters['startdate'], "%Y-%m-%d").date()
        end = datetime.strptime(api_parameters['enddate'], "%Y-%m-%d").date()
        #get the current date
        today = datetime.today().date()
        #determine what month was last month
        last_month = today.replace(day=1) - timedelta(days=1)
        
        #initialize the current month to start
        current = start
        #list to hold the YYYYMM format months
        months = []
        #list to hold years for data
        years = []
        #list to hold fild download urls
        urls = []

        #base URL for constructing the final URLs
        web_dir = 'https://www.ncei.noaa.gov/data/nclimgrid-daily/access/grids/'
        #loop until the current month exceeds the end month
        ##adds URLs for the FTP netcdf file for later downloading
        while current <= end:
            #append the current month in YYYYMM format and the year
            month_str = current.strftime("%Y%m")
            year_str = current.strftime("%Y")
            months.append(month_str)
            years.append(year_str)

            #create the filepath using the year and month
            filepath = f'{year_str}/ncdd-{month_str}-grd-scaled.nc'
            url = web_dir + filepath
            #add the url of the file to the url list
            urls.append(url)

            #check if the current month is this month or last month
            #if it is, add the preliminary raster filepath as well, as the complete filepath may not be avaailable yet
            if current.year == today.year and current.month == today.month or \
               current.year == last_month.year and current.month == last_month.month:
                filepath_prelim = f'{year_str}/ncdd-{month_str}-grd-prelim.nc'
                url_prelim = web_dir + filepath_prelim
                urls.append(url_prelim)
            
            #create a timedelta to move to the first day of the next month
            next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
            
            #update current month to the next month
            current = next_month
            print(current)
            
        #re-using header to store warnings
        if end.year == today.year and end.month == today.month or \
               end.year == last_month.year and end.month == last_month.month:
            headers = "Warning: requested dates are recent, data may be incomplete or subject to change regularly"
        else:
            headers = None
        #use paramters for api parameters
        parameters = api_parameters
        #store all the URLs in the URL list
        url = urls
        #re-using endpoint to store the number of files that we expect to download
        endpoint = len(months)
        #store all the above in a dictionary full_call
        full_call = {
            'url' : url,
            'endpoint' : endpoint,
            'headers' : headers,
            'parameters' : parameters
        }
        #return to the user the full_call so they can check the API is being called correctly
        self.response_codes['generate_api_call'] = full_call
        #return the full_call dictionary
        return full_call

    
    #api call function
    ##this is designed to count the amount of files the FTP server has
    ##input: url -- base url for the API
    ##input: enpoint -- the data endpoint for the API
    ##input: headers -- leading data for the API call (usually api key)
    ##input: parameters -- the parameters for the API call (settings, start date, end date, etc.)
    ##output: rows -- the count of files (amount of data) that the given API call has to the FTP server
    def api_call(self, url, endpoint, headers, parameters):
        #try to call the API and return the count of rows    
        #since we only care about the number of rows, we only look at header to be returned so we can check if it downloads
        rows = 0
        for link in url:
            filename = link.split('/')[-1]
            # Retry logic
            max_attempts = 3
            attempts = 0

            #try to get the file a few times
            while attempts < max_attempts:
                try:
                    # Send a GET request to the URL
                    response = requests.head(link)
                    
                    # Raise an exception if the request was unsuccessful
                    response.raise_for_status()
                    #if sucessful, then add count to the file
                    if response.status_code == 200:
                        rows += 1
                        break
                    else:
                        attempts += 1
                #failed attempt to download data error
                except requests.exceptions.RequestException as e:
                    attempts += 1
                    print(f"Attempt {attempts} failed: {e}")
                    time.sleep(0.5)  # Wait for 1 second before retrying
        #return the count of files the FTP server has
        self.response_codes['api_call'] = 'API returned ' + str(rows)
        return rows

    
    #api download data function
    ##this function downloads all the data instead of just counting rows. Separated into it's own unique function due to length.
    ##it repeats API calls, downloading data 'limit' number of rows at a time until it downloads all available data (or function errors 5 times in a row)
    ##ex. 'limit' = 500, metadata rows = 1200.. the function will make 3 API calls
    ##input: url -- base url for the API
    ##input: enpoint -- the data endpoint for the API
    ##input: headers -- leading data for the API call (usually api key)
    ##input: parameters -- the parameters for the API call (settings, start date, end date, etc.)
    ##output: all_data -- all of the data for the given API call
    def api_download(self, urls, endpoint, headers, parameters):
        target_folder = '../GRID_DATA/'
        for url in urls:
            # Extract filename from URL
            filename = url.split('/')[-1]
        
            # Make sure the target folder exists
            if not os.path.exists(target_folder):
                os.makedirs(target_folder)
        
            # Full path for saving the file
            full_path = os.path.join(target_folder, filename)
        
            # Retry logic
            max_attempts = 3
            attempts = 0

            #attempt to download all files
            while attempts < max_attempts:
                try:
                    #send a GET request to the URL
                    response = requests.get(url, stream=True)
                    
                    #raise an exception if the request was unsuccessful
                    response.raise_for_status()
        
                    #write the file
                    with open(full_path, 'wb') as file:
                        #write the content of the response in chunks to the file
                        for chunk in response.iter_content(chunk_size=8192):
                            file.write(chunk)
        
                    print(f"File downloaded: {full_path}")
                    break
                except requests.exceptions.RequestException as e:
                    attempts += 1
                    print(f"Attempt {attempts} failed: {e}")
                    time.sleep(0.5)  #wait for 1 second before retrying

        return target_folder
        

    #check completeness function
    ##this function checks if the data is complete based on the reported number of rows by the database and API.
    ##input: db_vals -- count of rows in generated API call, or None if generate API was never called
    ##input: api_vals -- count of rows actually existing in FTP server, or None if calls API was never called
    ##output: None (one of the _vals is None), True (db_vals and api_vals are equal.. data is complete), False (db_vals and api_vals are not equal.. data is incomplete)
    def check_completeness(self, db_vals, api_vals):
        print(db_vals)
        print(api_vals)
        #if one of the _vals is None, then one of the API connections was not called
        ##report and return None
        if db_vals is None or api_vals is None:
            print('one api/connection not called')
            self.response_codes['check_completeness'] = 'one api/connection not called'
            return None
        #if the difference between the two values is 0, then they have equal number of rows in response, data is complete
        ##report and return True
        elif (db_vals - api_vals) == 0:
            print('data is complete')
            self.response_codes['check_completeness'] = 'All expected month files are present. This does not guarantee daily completeness if requested dates are recent'
            return True
        #the difference between the two values is not 0, they have unqual number of rows in response, data is incomplete
        ##report and return False
        elif (db_vals - api_vals) > 0:
            print('expectation has more files')
            self.response_codes['check_completeness'] = 'Expected files: ' + str(db_vals) + " BUT there are " + str(api_vals) + " files present in the file server"
            return False
        else:
            print('too many files')
            self.response_codes['check_completeness'] = 'Expected files: ' + str(db_vals) + " BUT there are " + str(api_vals) + " files present in the file server"
            return False
    
    #fill incomplete function
    ##this function downloads all data from the API for the given parameter and sends it to the database
    ##this will only run if the data is incomplete and user allows function to run.
    ##this is designed to run in background on a seperate thread because it may take a long time to make all API calls and push data to database.
    ##input: translation -- translation for the given data endpoint, pulls api endpoints and url to generate api call
    ##input: api_parameters -- api parameters for the given request
    ##input: noaa_api_key -- NOAA API Key for API connection
    ##input: conn -- database connection
    ##output: nothing, it updates database inside function  
    ###This is redundant code in the NOAA Gridded ETL manager
    def fill_incomplete(self, translation, api_parameters, noaa_api_key, conn, diff):
        print("Fill incomplete not used for Gridded data")
        return None