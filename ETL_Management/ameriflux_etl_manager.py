"""
Ameriflux ETL Management Program
V1.0 (14 May 2024)
Logan Gall, gall0487@umn.edu

This file serves as a functional Extract, Transform, Load (ETL) tool for a custom-built Ameriflux Dataset.
This code contains multiple functions that all work together to process a user's Ameriflux API request.
The primary function in this code is process_request, which takes in external API arguments, and processes these argumetns to check, download, and aggregate data from the Ameriflux database.
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
import numpy as np
#Flask for api response return
from flask import Flask, Response
#json for api response return
import json
#time for sleep
import time
#os for file paths
import os



#R-Python interface
import rpy2
import os
os.environ['R_HOME'] = 'C:\Program Files\R\R-4.3.2'
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects import conversion, default_converter
from rpy2.robjects.packages import importr, data

"""
Class AMFETLManager
Global variables:
    self.args
        - Arguments passed in by user to the Ameriflux API. There are many, so they are defined in the file noaa_api_call.py
        - There are many Call_X functions, these allow a user to call or bypass any optional data checks in the ETL manager. Typically they will remain active, unless a user has problems with a specific data check.
        - Typical users will input 'Endpoint' (NOAA_GRID_DATA), 'Call_Direct_Download' (FALSE for checking data, CSV or JSON for downloading data after check) 'API_Arguments' (define data they want), 'Additional_Arguments' (define how they want aggregation).
    self.response_codes
        - For every section of data checking that occurs, the response codes store the failure/success of the function call. This is used for debugging and indicating to the user what is happening during the data processing pipeline.

Typically, the only function called in this class externally is process_request(self), which will take all the args and perform a data processing pipeline and either return downloaded data or the response codes indicating how the status of the data checking.

This is a MODIFIED version of the NOAAETLManager class, so items such as database calls remain in the code, yet do not produce results. This is to ensure the program does not reach unexpected errors from generating a fully new and unique class.
"""
class AMFETLManager:
    #initialization of the global variables
    def __init__(self, args):
        #All of the API arguments stored in the self.args variable
        self.args = args
        #Response codes for reporting data checking calls
        self.response_codes = {}

    
    #Data processing pipeline.
    ##First checks for direct download, if FALSE goes through optional parameters for data checking
    def process_request(self):
        #R-Python interface
        with conversion.localconverter(default_converter):
            utils = importr('utils')
            base = importr('base')
            amr = importr('amerifluxr')
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

        #If Call_Direct_Download is CSV or JSON --> start our data download process
        if (self.args['Call_Direct_Download'] == 'CSV') or (self.args['Call_Direct_Download'] == 'JSON'):
            #full_call calls generate_api_call, which takes in the user's api parameters and formats it in a 'requests' API call
            full_call = self.generate_api_call(arg_trans, self.args['API_Arguments'], self.args['NOAA_API_KEY'])
            
            #api_vals call api_call, which takes in the formatted call generated above
            ##db_vals returns as the count of rows of data in the API call
            db_vals = self.api_download(full_call['url'], full_call['endpoint'], full_call['headers'], full_call['parameters'])
            
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
            long_str = ""
            for i in db_vals:
                long_str += (" ; " + i)
            self.response_codes['data_download'] = "Raw data saved to: " + long_str
            return json.dumps(self.response_codes, indent = 4)

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
            db_vals = None
            #api_vals call api_call, which takes in the formatted call generated above
            ##returns the count of rows of data in the API call
            api_vals = self.api_call(full_call['url'], full_call['endpoint'], full_call['headers'], full_call['parameters'])
            api_vals = api_vals.to_csv(full_call['parameters']['out_dir'] + '\\metadata.csv', index=False)  # Set index=False if you don't want the DataFrame index in the file

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
        print(api_vals)
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
            'AMF_DATA': {
                'table': 'ameriflux_data',
                'sql': {
                    'datatypeid' : 'datatype ',
                    'startdate' : 'time >',
                    'enddate' : 'time <'
                },
                'aggregation' : {
                    '30_minute': '30T',
                    'hourly': 'H',
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
        with conversion.localconverter(default_converter):
            utils = importr('utils')
            base = importr('base')
            amr = importr('amerifluxr')

        datasets = []
        
        for i in df:
            with conversion.localconverter(default_converter):
                # Extract station_id from file name
                file_name = os.path.basename(i)
                station_id = file_name.split('_')[1]
                
                new_dat = amr.amf_read_base(file = i, unzip = True, parse_timestamp = True)
                station_dat = amr.amf_site_info()
                with (robjects.default_converter + pandas2ri.converter).context():
                    new_dat = robjects.conversion.get_conversion().rpy2py(new_dat)
                    station_dat = robjects.conversion.get_conversion().rpy2py(station_dat)

                #find station info
                station_dat = station_dat[station_dat['SITE_ID'] == station_id]

                lat = station_dat['LOCATION_LAT'].values[0]
                lon = station_dat['LOCATION_LONG'].values[0]
                
                # Add station_id as a new column in the dataframe
                new_dat['station_id'] = station_id
                new_dat['Latitude'] = lat
                new_dat['Longitude'] = lon
                
                datasets.append(new_dat)
                
        combined_df = pd.DataFrame()

        start = pd.Timestamp(datetime.strptime(api_parameters['startdate'], "%Y-%m-%d %H:%M:%S")).tz_localize('GMT')     
        end = pd.Timestamp(datetime.strptime(api_parameters['enddate'], "%Y-%m-%d %H:%M:%S")).tz_localize('GMT')
        
        for dataset in datasets:
            #subset by time
            dataset = dataset[(dataset['TIMESTAMP'] >= start) & (dataset['TIMESTAMP'] <= end)]
            #subset to data variables if presented
            if api_parameters['datatypeid'] is not None or api_parameters['datatypeid'] != '':
                keep_cols = ['TIMESTAMP', 'Latitude', 'Longitude']
                extra_cols = api_parameters['datatypeid'].split(',')
                keep_cols.extend(extra_cols)
                dataset = dataset[keep_cols]

            # Replace all -9999 values with NaN
            dataset.replace(-9999, np.nan, inplace=True)

            if additional_arguments is not None and 'dropNA' in additional_arguments:
                if additional_arguments['dropNA'] == True:
                    dataset.dropna(inplace=True)

            #aggregate given variables (otherwise mean)
            # Define the default aggregation function
            default_agg_func = 'mean'

            # Create the aggregation dictionary
            agg_dict = {col: additional_arguments['aggregation'].get(col, default_agg_func) for col in dataset.columns if col not in ['TIMESTAMP', 'Latitude', 'Longitude']}

            if additional_arguments is not None:
                freq = translation['aggregation'].get(additional_arguments['aggregation']['time'], '30T')    
            else:
                freq = '30T'
            # Resample and aggregate the data
            print(dataset)
            dataset = dataset.set_index('TIMESTAMP').groupby(['Latitude', 'Longitude'])
            dataset = dataset.resample(freq).agg(agg_dict).reset_index()
            print(dataset)
            #append to database
            combined_df = pd.concat([combined_df, dataset], ignore_index=True)
        
        #return the data
        return combined_df

    
    #generate api call function
    ##this function creates the URL list to download files from the FTP Server
    ##input: translation -- endpoint translaiton that provides the base URL and endpoint for this API call
    ##input: api_parameters -- user-inputs for API call, updates the default parameters
    ##input: noaa_api_key -- the api key to let the user connect to the NOAA API #not used for the gridded dataset
    ##output: full_call dictionary with keys 'url', 'endpoint', 'headers', and 'parameters' -- designed to be placed directly into a requests API call
    def generate_api_call(self, translation, api_parameters, noaa_api_key):
        # Find what years & months need to be downloaded
        start = datetime.strptime(api_parameters['startdate'], "%Y-%m-%d %H:%M:%S")        
        end = datetime.strptime(api_parameters['enddate'], "%Y-%m-%d %H:%M:%S")
        user_id = api_parameters['user_id']
        user_email = api_parameters['user_email']
        sites = api_parameters['site_id']
        data_product = 'BASE-BADM'
        data_policy = api_parameters['data_policy']
        agree_polcy = api_parameters['agree_policy']
        intended_use = api_parameters['intended_use']
        intended_use_text = api_parameters['intended_use_text']
        verbose = api_parameters['verbose']
        out_dir = api_parameters['out_dir']
        url = None
        endpoint = None
        headers = None
        parameters = api_parameters
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
        #download metadata bifs
        with conversion.localconverter(default_converter):
            utils = importr('utils')
            base = importr('base')
            amr = importr('amerifluxr')
            
        sites = parameters['site_id']
        with conversion.localconverter(default_converter):
            site_metadata = amr.amf_list_data(sites)
        with (robjects.default_converter + pandas2ri.converter).context():
            site_metadata = robjects.conversion.get_conversion().rpy2py(site_metadata)
        self.response_codes['api_call'] = "metadata saved to: " + parameters['out_dir'] + '\\metadata.csv'
        return site_metadata
 
    
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
        #download real data zips
        with conversion.localconverter(default_converter):
            utils = importr('utils')
            base = importr('base')
            amr = importr('amerifluxr')

        user_id = parameters['user_id']
        user_email = parameters['user_email']
        sites = parameters['site_id']
        data_product = 'BASE-BADM'
        data_policy = parameters['data_policy']
        agree_policy = parameters['agree_policy']
        intended_use = parameters['intended_use']
        intended_use_text = parameters['intended_use_text']
        verbose = parameters['verbose']
        out_dir = parameters['out_dir']

        with conversion.localconverter(default_converter):
            target_folder = amr.amf_download_base(user_id = user_id,
                  user_email = user_email,
                  site_id = sites,
                  data_product = data_product,
                  data_policy = data_policy,
                  agree_policy = agree_policy,
                  intended_use = intended_use,
                  intended_use_text = intended_use_text,
                  verbose = verbose,
                  out_dir = out_dir)
        
        return target_folder
        

    #check completeness function
    ##this function checks if the data is complete based on the reported number of rows by the database and API.
    ##input: db_vals -- count of rows in generated API call, or None if generate API was never called
    ##input: api_vals -- count of rows actually existing in FTP server, or None if calls API was never called
    ##output: None (one of the _vals is None), True (db_vals and api_vals are equal.. data is complete), False (db_vals and api_vals are not equal.. data is incomplete)
    def check_completeness(self, db_vals, api_vals):
        print("Completeness check not used for Ameriflux data")
        self.response_codes['check_completeness'] = "Completeness check not used for Ameriflux data"
        return None
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