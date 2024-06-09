"""
NOAA ETL Management Program
V1.2 (12 May 2024)
Logan Gall, gall0487@umn.edu

This file serves as a functional Extract, Transform, Load (ETL) tool for a custom-built NOAA API Database.
This code contains multiple functions that all work together to process a user's NOAA API request from our own database.
The primary function in this code is process_request, which takes in external API arguments, and processes these argumetns to check, download, and aggregate data from the database.
"""

#Imports
#psycopg2 for database connection
import psycopg2
#requests for NOAA API calls
import requests
#datetime for data aggregation by date
from datetime import datetime
#pandas for data aggregation
import pandas as pd
#threading to update database with new data in the background
import threading
#Flask for api response return
from flask import Flask, Response
#json for api response return
import json

"""
Class NOAAETLManager
Global variables:
    self.args
        - Arguments passed in by user to the NOAA API. There are many, so they are defined in the file noaa_api_call.py
        - There are many Call_X functions, these allow a user to call or bypass any optional data checks in the ETL manager. Typically they will remain active, unless a user has problems with a specific data check.
        - Typical users will input 'Endpoint' (NOAA_DATA), 'Call_Direct_Download' (FALSE for checking data, CSV or JSON for downloading data after check) 'API_Arguments' (define data they want), 'Additional_Arguments' (define how they want aggregation) 'DB_Credentials' (database credentials), 'NOAA_API_KEY' (NOAA's API key).
    self.response_codes
        - For every section of data checking that occurs, the response codes store the failure/success of the function call. This is used for debugging and indicating to the user what is happening during the data processing pipeline.

Typically, the only function called in this class externally is process_request(self), which will take all the args and perform a data processing pipeline and either return downloaded data or the response codes indicating how the status of the data checking.
"""
class NOAAETLManager:
    #initialization of the global variables
    def __init__(self, args):
        #All of the API arguments stored in the self.args variable
        self.args = args
        #Response codes for reporting data checking calls
        self.response_codes = {}

    
    #Data processing pipeline.
    ##First checks for direct download, if FALSE goes through optional parameters for data checking
    def process_request(self):
        #Database values, stores the response returned by the database
        db_vals = None
        #API values, stores the response returned by API calls
        api_vals = None
        #Completeness check, tells if data is complete between database and api
        complete = None
        #Argument translation. Calls the translate_endpoint function
        ##Returns a dictionary with extra parameters that are key for this specific data download endpoint
        ##Ex. URL of API, and translation from API to database (startdate : X to SQL date >= X)
        arg_trans = self.translate_endpoint(self.args['Endpoint'])

        #If Call_Direct_Download is CSV or JSON --> start our data download process
        if (self.args['Call_Direct_Download'] == 'CSV') or (self.args['Call_Direct_Download'] == 'JSON'):
            #conn calls db_connect, which creates an open conneciton to our database with psycopg2
            conn = self.db_connect(self.args['DB_Credentials'])
            #sql calls generate_sql, which generates an sql query for the user inputted parameters
            sql = self.generate_sql(translation = arg_trans,
                                api_arguments = self.args['API_Arguments'])
            #db_vals calls execute_sql, which runs the above generated sql statement AND returns all the rows (download=True)
            db_vals = self.execute_sql(sql, conn, download=True)
            #close the connection to the database
            conn.close()
            #check if db_vals was able to get data from the database
            if db_vals is not None:
                #if Call_Aggregation is True, then start aggregating the data based on additional arguments
                if self.args['Call_Aggregation']:
                    #call aggregate_data function to aggregate and clean data for user
                    db_vals = self.aggregate_data(db_vals, arg_trans, self.args['Additional_Arguments'])

            if (self.args['Call_Direct_Download'] == 'JSON'):
                json_vals = db_vals.to_json(orient="records", lines = False, indent = 4)
                #return the resulting rows from the database call
                return json_vals

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
        ##in order to check for data completeness, we want to count how many rows are currently in the database for this specific request
        if self.args['Call_DB']:
            #conn calls db_connect, which creates an open connection to our database with psycopg2
            conn = self.db_connect(self.args['DB_Credentials'])
            #sql calls generate_sql, which generatees an sql query for the user inputted parameters
            sql = self.generate_sql(translation = arg_trans,
                                api_arguments = self.args['API_Arguments'])
            #db_vals calls execute_sql which runs the above generated sql statements AND returns the count of all the rows (download defaults to False)
            db_vals = self.execute_sql(sql, conn)
            #close the connection to the database
            conn.close()

        #Call API
        ##in order to check for data completeness, we want to count how many rows the NOAA API has for this specific request
        if self.args['Call_API']:
            #full_call calls generate_api_call, which takes in the user's api parameters and formats it in a 'requests' API call
            full_call = self.generate_api_call(arg_trans, self.args['API_Arguments'], self.args['NOAA_API_KEY'])
            #api_vals call api_call, which takes in the formatted call generated above
            ##returns the count of rows of data in the API call
            api_vals = self.api_call(full_call['url'], full_call['endpoint'], full_call['headers'], full_call['parameters'])

        #Call Completeness check
        ##if both databse and api are called, it will check if they have the same number of rows
        if self.args['Call_Completeness']:
            #complete calls check_completeness, can be True (data is comlete), False (data is incomplete), or None (db_vals or api_vals is None)
            complete = self.check_completeness(db_vals, api_vals)

        #Call filling incomplete data
        ##if our data is not fully complete, we will make a background thread to call API, download data, and fill in the database
        ##Todo: extra logic to make this more efficient
        if self.args['Call_Fill_Incomplete']:
            #check if complete is False (data is incomplete between database and API)
            if complete == False:
                #store diffference between API and Database
                diff = (db_vals - api_vals)
                #create new connection to database
                conn = self.db_connect(self.args['DB_Credentials'])
                #start a new 'thread' to run the fill_incomplete function
                ##This will run in the background in order to download all data from the API, and push it to the database.
                ##This can take awhile for large data downloads
                thread = threading.Thread(target=self.fill_incomplete, args=(arg_trans, self.args['API_Arguments'], self.args['NOAA_API_KEY'], conn, diff))
                #start the background thread process
                thread.start()
                #return to the user that our data is incomplete, we need to wait for data to be filled in to the database
                ##A user would typically re-call the data completeness check until this does not appear
                self.response_codes['Fill_Incomplete'] = "Running Fill Incomplete, check again soon."

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
            'NOAA_DATA': {
                'table': 'noaa_api',
                'sql': {
                    'datatypeid' : 'datatype ',
                    'stationid' : 'station ',
                    'startdate' : 'date >',
                    'enddate' : 'date <'
                },
                'api' : {
                    'url' : 'https://www.ncdc.noaa.gov/cdo-web/api/v2/',
                    'endpoint' : '/data/',
                    'datasetid' : 'GHCND'
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
    def db_connect(self, db_credentials):
        #attempt to connect to database using psycopg2
        try:
            connection = psycopg2.connect(
                dbname=db_credentials['dbname'],
                user=db_credentials['user'],
                password=db_credentials['password'],
                host=db_credentials['host'],
                port=db_credentials['port']
            )
            #if that worked, we can set response code to True (connection was successful)
            self.response_codes['DB_Connect'] = True
            #return connection object
            return connection
        #if it fails, report the error
        except psycopg2.Error as e:
            #print error to console
            print(f"Error connecting to the database: {e}")
            #save the error to response code to report to user
            self.response_codes['DB_Connect'] = f"Error connecting to the database: {e}"
            #return None, which will cause later functions to also fail
            return None

    
    #generate sql query function
    ##creates a sql query that can download all the data for the given user inputs
    ##input: translation -- the translation dictionary from translate_endpoint function
    ##input: api_argumets -- the user-inputted api arguments that follow NOAA API's input scheme.
    ##output: a sql query dictionary with 'SELECT', 'FROM', and 'WHERE' keys.
    def generate_sql(self, translation, api_arguments):
        #select all columns of the data
        ##there used to be more logic involved, such as picking columns to be returned, but this was migrated to the data aggregation function
        select_clause = "SELECT *"
        #get data from our endpoint table (NOAA_DATA --> noaa_api table)
        from_clause = 'FROM "' + translation['table'] + '"'  # Ensure table_name is correctly quoted for SQL
        #default WHERE clause (downloads everything, allows us to extend the where clause with AND statements)
        where_clause = "WHERE 1=1"
    
        #look at each argument in the api_arguments dictionary
        ##if we have a sql translation, add it to the WHERE clause
        ##ex. arg:value 'startdate':'2023-12-30' adds " AND date >= '2023-12-30' " to the WHERE clause.
        ##ex2. arg:value 'dataype':'PRCP,TAVG' adds " AND datatype IN ('PRCP', 'TAVG') "  to the WHERE clause.
        for arg, value in api_arguments.items():
            #check if argument type is in our translation dictionary
            if arg in translation['sql']:
                #check if the argument value contains multiple values separated by commas
                if ',' in value:
                    #split the value by comma and create a sql IN clause
                    values_list = value.split(',')
                    #create list of values (see ex2 above)
                    in_clause = "(" + ", ".join(["'" + val + "'" for val in values_list]) + ")"
                    #make this into a 'condition': " argument IN (list, of, values) "
                    condition = translation['sql'][arg] + " IN " + in_clause
                else:
                    if value != '':
                        #single value 'condition': " argument = value " (see ex above)
                        condition = translation['sql'][arg] + "= '" + value + "'"
                #add our 'condition' to the WHERE clause, seperate each condition by " AND "
                where_clause += " AND " + condition

        #combine all our sql clauses into a dictionary
        sql_statement = {
            "SELECT": select_clause,
            "FROM": from_clause,
            "WHERE": where_clause
        }

        #save the sql statement to the response codes, so user can verify it is working correct
        self.response_codes['generate_sql'] = sql_statement
        
        #return the dictionary
        return sql_statement

    
    #execute a given SQL statement
    ##This function has dual purpose: count rows of the given query (download = False) OR return all data as pandas dataframe (download = True)
    ##input: sql_dict -- dictionary containing sql query with keys 'SELECT', 'FROM', and 'WHERE'
    ##input: connection -- psycopg2 database connection
    ##input: download -- indicator to count rows or download data (defaults to false)
    ##output: all the data in pandas dataframe (if download = True), count of rows in query (if download = False), OR None (if an error occurs)
    def execute_sql(self, sql_dict, connection, download = False):
        #immediate error if database connection doesn't exist
        if connection is None:
            #send error in response code to user
            self.response_codes['Execute_SQL'] = 'Failed to execute SQL due to DB connection error.'
            return None
        #if download is True, we want to download all the data to a pandas dataframe
        if download:
            #put query together into one string
            sql_query = sql_dict['SELECT'] + sql_dict['FROM'] + sql_dict['WHERE']
            #execute sql query and save it to pandas dataframe
            try:
                data = pd.read_sql_query(sql_query, con=connection)
                #if it worked, report that to user in response_codes
                self.response_codes['Execute_SQL'] = f'Successfully executed.'
                #return pandas dataframe
                return data
            #if error, report it and return None
            except psycopg2.Error as e:
                print(f"Error executing SQL: {e}")
                self.response_codes['Execute_SQL'] = f'Failed to execute SQL. Error: {e}'
                return None
        #if download is not True, just count the rows of the data (saves processing time)
        else:
            #put query together with a select count instead of select *
            sql_query = f"SELECT COUNT(*) {sql_dict['FROM']} {sql_dict['WHERE']}"
            #execute query and get count of rows
            try:
                #create connection cursor
                cursor = connection.cursor()
                #execute query
                cursor.execute(sql_query)
                #get the count of rows
                row_count = cursor.fetchone()[0]
                #report the row count to the user in response_codes
                self.response_codes['Execute_SQL'] = f'Successfully executed. Rows returned: {row_count}.'
                #return row count back
                return row_count
            #if error, report it and return None
            except psycopg2.Error as e:
                print(f"Error executing SQL: {e}")
                self.response_codes['Execute_SQL'] = f'Failed to execute SQL. Error: {e}'
                return None
            #when done trying to count rows, close the cursor
            finally:
                cursor.close()

    
    #aggregate data funciton
    ##This function aggregates data based on date and performs small data cleaning for the user
    ##input: df -- dataframe with data returned from database
    ##input: translation -- mapping of data for the given endpoint, in this case we are looking at converting user inputted 'time' to single characters (ex below)
    ##input: additional_arguments -- extra arguments specific to our custom NOAA API. in this case, looking at time step to aggregate values (ex below) and how to aggregate each data type (ex2 below)
    ###ex. additional_arguments['aggregation']['time'] = 'weekly' --> translation['aggregation']['weekly] : 'W' (turn 'weekly' aggregation to 'W' character)
    ###ex2. additional_arguments['aggregation']['prcp'] = 'SUM' and additional_arguments['aggregation']['tavg'] = 'MEAN' --> SUM the PRCP column and MEAN the TAVG column
    def aggregate_data(self, df, translation, additional_arguments):
        #ensure 'date' column is datetime type for proper resampling
        df['date'] = pd.to_datetime(df['date'])
        #determine the aggregation time step. Look at user additional_arguments, and translate that to single character. (ex above)
        if additional_arguments is not None:
            freq = translation['aggregation'].get(additional_arguments['aggregation']['time'], 'D')    
        else:
            freq = 'D'
        
        #initialize an empty list to store the aggregated DataFrames
        aggregated_dfs = []
        #iterate over each unique datatype so we can aggregate them individually by MEAN or SUM or other method
        for datatype in df['datatype'].unique():
            #filter the DataFrame for the current datatype
            df_subset = df[df['datatype'] == datatype]
            #determine the aggregation method for the current datatype, default is 'mean' (ex2 above)
            if additional_arguments is not None:
                agg_method = additional_arguments['aggregation'].get(datatype, 'mean')
            else:
                agg_method = 'mean'
            #group by station, datatype, and aggregate by time
            grouped = df_subset.set_index('date').groupby(['station', 'datatype']).resample(freq)
            #place the aggregated (averaged/summed/etc) data in the 'value' column
            agg_dict = {'value': agg_method,
                        'latitude' : 'first',
                        'longitude' : 'first',
                        'elevation' : 'first',
                        'name' : 'first'}
            aggregated = grouped.agg(agg_dict)
            #reset index after aggregation
            aggregated = aggregated.reset_index()
            #append the aggregated DataFrame to the Alist
            aggregated_dfs.append(aggregated)

        #once we look at all the data types, combine the lists into one big dataframe
        result_df = pd.concat(aggregated_dfs, ignore_index=True)
        #convert date back to a readable text for the user
        result_df['date'] = result_df['date'].dt.strftime('%Y-%m-%d')
        #return only the columns that the user cares about, defaults to return everything. (ex. 'return_columns' : ['date', 'datatype', 'value'] (removes 'station' and 'attributes' from the dataframe))

        # Check for requested format of the result
        if additional_arguments is not None:
            if additional_arguments.get('format') == 'wide':
                # We pivot the table so that each datatype becomes a column, and we also keep the other metadata columns
                result_df = result_df.pivot_table(
                    index=['date', 'station', 'latitude', 'longitude', 'elevation', 'name'], 
                    columns='datatype', 
                    values='value',
                    aggfunc='first'  # Using 'first' because each group should theoretically have unique values per datatype
                ).reset_index()
    
                # Flatten the hierarchical column labels and ensure unique names
                result_df.columns = [''.join(col).strip() if col[1] else col[0] for col in result_df.columns]

        # Return the data
        return result_df

    
    #generate api call function
    ##this function creates the requests parrameters in order to call the NOAA API directly
    ##input: translation -- endpoint translaiton that provides the base URL and endpoint for this API call
    ##input: api_parameters -- user-inputs for API call, updates the default parameters
    ##input: noaa_api_key -- the api key to let the user connect to the NOAA API
    ##output: full_call dictionary with keys 'url', 'endpoint', 'headers', and 'parameters' -- designed to be placed directly into a requests API call
    def generate_api_call(self, translation, api_parameters, noaa_api_key):
        #header dictionary holding the API key
        headers = {'token': noaa_api_key}
        #initialize parameters with defaults for dataset, limit, offset, and locaiton (FIPS:27 is state of Minnesota)
        parameters = {'datasetid' : translation['api']['datasetid'],
                      'limit': 500,
                      'offset': 0,
                      'locationid' : 'FIPS:27',
                     'units' : 'metric'}
        #update parameters with those provided in API_Arguments (ex. adding in startdate and enddate)
        parameters.update(api_parameters)
        #pull url and endpoint from the translation dictionary
        url = translation['api']['url']
        endpoint = translation['api']['endpoint']
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
    ##this is designed to count the amount of rows the NOAA API has in its data
    ##input: url -- base url for the API
    ##input: enpoint -- the data endpoint for the API
    ##input: headers -- leading data for the API call (usually api key)
    ##input: parameters -- the parameters for the API call (settings, start date, end date, etc.)
    ##output: rows -- the count of rows (amount of data) that the given API call has
    def api_call(self, url, endpoint, headers, parameters):
        #try to call the API and return the count of rows    
        try:
            #since we only care about the number of rows, we only ask for one row to be returned so we can look at metadata
            parameters['limit'] = 1
            #make API call with requests
            response = requests.get(url + endpoint, headers=headers, params=parameters)
            #check for error in API call
            response.raise_for_status()
            print("API Called")
            #looking at data as json dictionary
            data = response.json()
            #the count of rows for the NOAA API is in 'metadata', 'resultset' (the metadata of ALL results), and 'count'
            if bool(data):
                rows = data['metadata']['resultset']['count']
            else:
                rows = 0
            print('API returned ' + str(rows))
            #save the number of rows to response_codes so the user can look at it
            self.response_codes['api_call'] = 'API returned ' + str(rows)
            #return count of rows
            return rows
        #if an error occurs, report it and return None
        except requests.exceptions.RequestException as e:
            print(f"API call failed: {e}")  # Handle exceptions (e.g., network issues, 4xx and 5xx errors).
            self.response_codes['api_call'] = f"API call failed: {e}"
            return None

    
    #api download data function
    ##this function downloads all the data instead of just counting rows. Separated into it's own unique function due to length.
    ##it repeats API calls, downloading data 'limit' number of rows at a time until it downloads all available data (or function errors 5 times in a row)
    ##ex. 'limit' = 500, metadata rows = 1200.. the function will make 3 API calls
    ##input: url -- base url for the API
    ##input: enpoint -- the data endpoint for the API
    ##input: headers -- leading data for the API call (usually api key)
    ##input: parameters -- the parameters for the API call (settings, start date, end date, etc.)
    ##output: all_data -- all of the data for the given API call
    def api_download(self, url, endpoint, headers, parameters):
        #store all data in a list
        all_data = []
        #start with an 'offset' of 0 to start at begining of data
        parameters['offset'] = 0
        #track how many rows we have downloaded
        downloaded_rows = 0
        #track how many times we rety a given call
        retries = 0

        #loop infinitely until all our data is downloaded
        while True:
            #try to make API call and download data
            try:
                #use requests to make API call
                response = requests.get(url + endpoint, headers=headers, params=parameters)
                #check if an error occured
                response.raise_for_status()
                #look at data as json
                data = response.json()
                #look at how many rows of data we downloaded. 'results' key has all the data rows
                rows = len(data['results'])
                #store how many rows we downloaded
                downloaded_rows += rows
                #add new data to results list
                all_data.extend(data['results'])
                #check how much total data exists
                total_rows = data['metadata']['resultset']['count']
                #check if we downloaded all our data
                if downloaded_rows >= total_rows:
                    #exit loop
                    break
                #if we did not finish downloading data, change offset and repeat API call
                parameters['offset'] += rows
                #reset retries after a successful attempt
                retries = 0
            #if there is an error, report the error and retry
            except requests.exceptions.RequestException as e:
                print(f"Error: {e}")
                retries += 1
                #break after 5 consecutive errors and return None
                if retries >= 5:
                    print("Max retries reached. Exiting.")
                    return None
        #return all the data after the while loop breaks
        return all_data

    #check completeness function
    ##this function checks if the data is complete based on the reported number of rows by the database and API.
    ##input: db_vals -- count of rows in database, or None if database was never called
    ##input: api_vals -- count of rows in API, or None if API was never called
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
            self.response_codes['check_completeness'] = 'data is complete'
            return True
        #the difference between the two values is not 0, they have unqual number of rows in response, data is incomplete
        ##report and return False
        elif (db_vals - api_vals) > 0:
            print('database has more observations')
            self.response_codes['check_completeness'] = 'database has more observations'
            return False
        else:
            print('data is not complete')
            self.response_codes['check_completeness'] = 'api has more observations'
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
    def fill_incomplete(self, translation, api_parameters, noaa_api_key, conn, diff):
        # Generate an API call for our given parameters
        full_call = self.generate_api_call(translation, api_parameters, noaa_api_key)
        # Download all the data using api_download function, inputting the generated API call
        api_vals = self.api_download(full_call['url'], full_call['endpoint'], full_call['headers'], full_call['parameters'])
        # Generate UIDs for API data for easy comparison
        api_uids = {str(row['date']) + '_' + str(row['station']) + '_' + str(row['datatype']) for row in api_vals}
        try:
            # Start cursor with database connection
            cur = conn.cursor()
            if diff < 0:
                # Add data to database row by row
                for row in api_vals:
                    uid = str(row['date']) + '_' + str(row['station']) + '_' + str(row['datatype'])
                    # Fetch additional details from noaa_station_list table
                    cur.execute("""
                        SELECT latitude, longitude, name, elevation FROM noaa_station_list WHERE id = %s;
                    """, (row['station'],))
                    station_details = cur.fetchone()
                    
                    if station_details:
                        latitude, longitude, name, elevation = station_details
                        # Add the row into the database using the below SQL query.
                        cur.execute("""
                            INSERT INTO noaa_api(date, datatype, station, attributes, value, uid, latitude, longitude, name, elevation)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (uid) DO NOTHING;
                        """, (row['date'], row['datatype'], row['station'], row['attributes'], row['value'], uid, latitude, longitude, name, elevation))
            
            elif diff > 0:
                # Remove extra rows from the database that are not in the API data
                cur.execute("SELECT uid FROM noaa_api;")
                db_uids = {record[0] for record in cur.fetchall()}
                extra_uids = db_uids - api_uids
                for uid in extra_uids:
                    cur.execute("DELETE FROM noaa_api WHERE uid = %s;", (uid,))
            # Commit changes to the database
            conn.commit()
            print('Database update complete')
    
        except Exception as e:
            print(f"Error executing database operations: {e}")
        finally:
            # Close cursor and connection to release resources
            cur.close()
            conn.close()