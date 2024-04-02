from flask import Flask
from flask_restful import Api
from resources.noaa_api_call import NOAAAPICall

app = Flask(__name__)
api = Api(app)

api.add_resource(NOAAAPICall, '/NOAA_API_CALL')

if __name__ == '__main__':
    app.run(debug=True)