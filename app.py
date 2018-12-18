import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

import datetime as dt
import pandas as pd


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Convert the query results to a Dictionary using `date` as the key and `prcp` as the value."""
    """Return the JSON representation of your dictionary."""
    """Return the result of query to retrieve the last 12 months of precipitation data."""

    Latest_Date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    query_end_date=Latest_Date
    print(f"End date: ", query_end_date)

    #query_end_date = session.query(Measurement.date).all()[-1][0]

    # Calculate the date 1 year ago from the last data point in the database
    query_start_date = dt.datetime.strptime(query_end_date, "%Y-%m-%d") - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    #print("----------------Perform a query to retrieve the data from start date to end date-----------")
    query_result=session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date > query_start_date).\
        filter(Measurement.date <= query_end_date).all()

    # Save the query results as a Pandas DataFrame and set the index to the date column
    precipitation_df = pd.DataFrame(query_result, columns=['date', 'precipitation'])
    precipitation_df.set_index('date', inplace=True)
    precipitation_list = precipitation_df.to_dict(orient='index')

    return jsonify(precipitation_list)


@app.route("/api/v1.0/stations")
def stations():
    """Return a list of stations from the dataset"""
    
    #get the list of station names ONLY
    most_active_stations_in_desc_order = session.query(Station.station).\
        filter(Measurement.station == Station.station).\
        group_by(Station.station).order_by(func.count(Measurement.id).desc())
    
    list_of_stations=[]
    
    for item in most_active_stations_in_desc_order:
        list_of_stations.append(item)
        
    return jsonify(list_of_stations)
    

@app.route("/api/v1.0/tobs")
def tobs():
    """query for the dates and temperature observations from a year from the last data point."""
    """Return a JSON list of Temperature Observations (tobs) fro the previous years."""
    most_active_stations_in_desc_order_2 = session.query(Station.station).\
        filter(Measurement.station == Station.station).\
        group_by(Station.station).order_by(func.count(Measurement.id).desc())

    most_active_station_2 = most_active_stations_in_desc_order_2[0][0]

    # Latest Date in the table
    Latest_Date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    query_end_date=Latest_Date
    print(f"End date: ", query_end_date)
    # Calculate the date 1 year ago from the last data point in the database
    query_start_date = dt.datetime.strptime(query_end_date, "%Y-%m-%d") - dt.timedelta(days=365)
        
    tob_query_result=session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.station == most_active_station_2).\
        filter(Measurement.date > query_start_date).\
        filter(Measurement.date <= query_end_date).all()

    # Save the query results as a Pandas DataFrame and set the index to the date column
    tob_df = pd.DataFrame(tob_query_result, columns=['date', 'tobs'])
    tob_df.set_index('date', inplace=True)

    # Sort the dataframe by date
    sorted_tob_df = tob_df.sort_index(ascending=True)
    
    sorted_tob_list = sorted_tob_df.to_dict(orient='index')
    
    return jsonify(sorted_tob_list)


if __name__ == '__main__':
    app.run(debug=True)
