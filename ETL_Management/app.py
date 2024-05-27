from flask import Flask, jsonify, request, render_template_string
import psycopg2
from flask_restful import Api
from resources.noaa_api_call import NOAAAPICall


app = Flask(__name__)
api = Api(app)

api.add_resource(NOAAAPICall, '/NOAA_API_CALL')

DATABASE_CONFIG = {
    'host': 'localhost',
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Passwordd'
}

def get_db_connection():
    conn = psycopg2.connect(**DATABASE_CONFIG)
    return conn

@app.route('/')
def index():
    return render_template_string(open('templates/index.html').read())

@app.route('/grid')
def gridindex():
    return render_template_string(open('templates/gridindex.html').read())

@app.route('/stations', methods=['GET'])
def stations():
    lat1 = request.args.get('lat1')
    lon1 = request.args.get('lon1')
    lat2 = request.args.get('lat2')
    lon2 = request.args.get('lon2')

    query = 'SELECT id, name, latitude, longitude FROM noaa_station_list'
    conditions = []
    params = []
    
    if lat1 and lon1 and lat2 and lon2:
        minlat = min(lat1, lat2)
        maxlat = max(lat1, lat2)
        minlon = min(lon1, lon2)
        maxlon = max(lon1, lon2)
        conditions.append(f"latitude BETWEEN {minlat} AND {maxlat}")
        conditions.append(f"longitude BETWEEN {maxlon} AND {minlon}")
    
    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)

    print(query)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    stations = [{'id': row[0], 'name': row[1], 'latitude': row[2], 'longitude': row[3]} for row in cur.fetchall()]
    cur.close()
    conn.close()

    return jsonify(stations)
    
@app.route('/grid_points', methods=['GET'])
def grid_points():
    lat1 = request.args.get('lat1')
    lon1 = request.args.get('lon1')
    lat2 = request.args.get('lat2')
    lon2 = request.args.get('lon2')

    query = 'SELECT id, latitude, longitude FROM noaa_grid_coords'
    conditions = []
    params = []
    
    if lat1 and lon1 and lat2 and lon2:
        minlat = min(lat1, lat2)
        maxlat = max(lat1, lat2)
        minlon = min(lon1, lon2)
        maxlon = max(lon1, lon2)
        conditions.append(f"latitude BETWEEN {minlat} AND {maxlat}")
        conditions.append(f"longitude BETWEEN {maxlon} AND {minlon}")
    
    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)

    print(query)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    stations = [{'id': row[0], 'latitude': row[1], 'longitude': row[2]} for row in cur.fetchall()]
    cur.close()
    conn.close()

    return jsonify(stations)

if __name__ == '__main__':
    app.run(debug=True)