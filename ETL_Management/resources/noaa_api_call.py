from flask_restful import Resource, reqparse
from noaa_etl_manager import NOAAETLManager

parser = reqparse.RequestParser()
parser.add_argument('Endpoint', required=True, help="Endpoint cannot be blank!")
parser.add_argument('Call_Direct_Download', required=True, choices=('FALSE', 'CSV', 'JSON'), help='Direct Download format must be either FALSE, CSV, or JSON')
parser.add_argument('Call_DB', type=bool, default=True)
parser.add_argument('Call_API', type=bool, default=True)
parser.add_argument('Call_Completeness', type=bool, default=True)
parser.add_argument('Call_Fill_Incomplete', type=bool, default=True)
parser.add_argument('Call_Aggregation', type=bool, default=True)
parser.add_argument('DB_Credentials', type=dict, required=False)
parser.add_argument('NOAA_API_KEY', required=False)
parser.add_argument('API_Arguments', type=dict, required=True, help =  'Dictionary containing API parameters. startdate and enddate cannot be blank! (YYYY-MM-DD) or (YYYY-MM-DDThh:mm:ss)')
parser.add_argument('Additional_Arguments', type=dict, required=False)

class NOAAAPICall(Resource):
    def post(self):
        args = parser.parse_args()
        etl_manager = NOAAETLManager(args)
        return etl_manager.process_request()

"""
        Here are the parameter descriptions:
        - Endpoint (required)
          -- The API endpoint that we are calling.. Only used in Translation function for now. Will be used to call different API endpoints in the future
          
        - Call_Direct_Download (required) (FALSE, CSV, JSON)
          -- Bypass of parameter checks in order to download data straight from database. This is designed so the user can download data directly from the database. 
          -- Typical use would have 2 API calls: First with Call_Direct_Download as FALSE to check if database has all user required data, and a Second to download the data itself.
          
        - API_Arguments (required) (dictionary with 'startdate' and 'enddate' at minimum)
          -- This is the API arguments that are formatted to be directly sent to the NOAA API.
          -- For this API, the only required parameters are 'startdate' and 'enddate' in the format: (YYYY-MM-DD) or (YYYY-MM-DDThh:mm:ss)

        - Additional_Arguments (optional) (dictionary with any of: 'return_columns' (list) or 'aggregation' (dictionary))
          -- This contains additioanl arguments specific to this API, not hosted in the NOAA API functionality.
          -- return_ columns is a list of columns a user wants returned in their final dataset. They must know what columns are available to return.
          -- aggreagation is user definitions of how they want data aggreagated together. This requires a 'time' field with 'daily', 'weekly', 'monthly', or 'yearly' aggreagations. Default aggregation style is MEAN, but user can define aggregation by data type by inputting data type as another field (ex. 'prcp' : 'SUM', 'tavg' : 'MEAN' ... this will sum the prcp field and average the tavg field across the aggregation times)
          
        - Call_Parameter_Check (optional) (default = True)
          -- This functionality will be implemented in the future. Planned use will be to verify inputted parameters are valid, and also define station lists based on user added location bounding boxes.
          
        - Call_DB (optional) (default = True)
          -- This is used to call database to check how many rows are currently in database based on API parameters. Generally, a user will want this true unless they want to just look at API rows.
          
        - Call_API (optionatl) (default = True)
          -- This is used to call the NOAA API to check how many rows of data the NOAA API has for the given API. Useful for comparing if user's data request is fully complete in database.
          
        - Call_Completeness (optional) (default = True)
          -- This performs a check if the API and Database have the same number of rows for requested data.
          -- Will only perform comparison if Call_DB and Call_API are True
          
        - Call_Fill_Incomplete (optional) (default = True)
          -- This will call the API to download data and attempt to fill any missing rows in the database.
          -- Will only run if data is incomplete
          
        - Call_Aggregation (optional) (default = True)
          -- This will call aggregation functions that will aggregate the data by time (and to be implemented: other user prefrences like locations or data flags)
          -- Will only run when Call_Direct_Download is not FALSE
          
        - DB_Credentials (optional, required when Call_DB is True) (dictionary with 'dbname', 'user', 'password', 'host', and 'port')
          -- Credentials for psycopg2 to be able to connect to our database hosting all data
          
        - NOAA_API_KEY (optional, required when Call_API is True) (string)
          -- The user's API key from the NOAA web2 API
"""