# Project Zero Flask Application for NWS API

This Flask application serves as an interface to the National Weather Service (NWS) API, allowing users to retrieve and display weather information in a web format. The application utilizes Flask, a micro web framework written in Python, to create a lightweight server for handling API requests and displaying results through HTML templates.

## API Documentation

https://www.weather.gov/documentation/services-web-api

## Getting Started

### Prerequisites

- Python 3.x
- Flask

### Running the Application

Run the provided app.py file to start the Flask app.

This will start a local server. By default, the application will be accessible at `http://127.0.0.1:5000/`.

## Application Structure

- `app.py`: The main Python file that contains the Flask application setup and routes.
- `templates/`: A directory containing HTML templates for rendering information retrieved from the NWS API.
  - `index.html`: The main HTML template for the application's interface.

## Usage

After starting the application, navigate to `http://127.0.0.1:5000/` in your web browser. The application interfaces with the National Weather Service (NWS) API to provide the following capabilities:

### Listing Stations and Their Information

This feature allows users to view a list of weather stations and their respective information, such as location, ID, and observation capabilities.

### Listing Point Information

Retrieve detailed information about a specific geographic point, including its associated weather forecast office and grid coordinates.

### Getting Forecast Data for a Given Point

Access forecast data for a specific point by providing its latitude and longitude OR GridX and GridY and WFO. This feature presents the forecast for the upcoming period, including temperature, precipitation chances, and weather conditions.

To use these features, use the dropdown menus and fill in information.
