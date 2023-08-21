import pandas as pd

from enum import Enum
from typing import Union
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Path, Query

from sqlalchemy import create_engine, MetaData, Table, select


## Create a metadata object
metadata = MetaData()

## Create a connection to the database
engine = create_engine(              
    ## Path to database                     
    'sqlite:///../../data/FastAPI.db',      # <1>
)

## Create a connection to the database
connection = engine.connect()

## Reflect census table from the engine: census
fastapi = Table(
    "fastapi", 
    metadata,
    autoload_with = engine,
)

## Instantiate a FastAPI object
app = FastAPI(
    title="My first API",
    description="I hope you like it!",
    version="0.0.1",
)

class Municipality(Enum):
    """Name of the municipality"""
    pass

class Year(Enum):
    """Year of the data"""
    pass

class IncomeTax(Enum):
    """Income tax in kCHF"""
    pass

@app.get("/")
def index():
    """
    Returns all data from the fastapi table
    """
    ## Create an sql statement to select ALL columns from the census table
    stmt = (
        ## Select ALL columns from the census table
        select(
            fastapi
        )
        ## Order by the Year column
        .order_by(
            fastapi.columns.Year.desc()
        )
    )
    
    results = (
        pd.DataFrame(
            data = (
                connection
                .execute(stmt)
                .fetchall()
            )
        )
    )
    return results.to_dict(
        orient = "split",
        index = False,
    )

@app.get("/year/{year}")
def get_data_of_year(year: int):
    """
    Returns all data from the fastapi table for a given year
    """
    ## Create an sql statement to select ALL columns from the census table
    stmt = (
        ## Select ALL columns from the census table
        select(
            fastapi
        )
        ## Order by the Year column
        .order_by(
            fastapi.columns.Year.desc()
        )
        .where(
            fastapi.columns.Year == year
        )
    )
    
    ## Create a DataFrame from the results
    results = (
        pd.DataFrame(
            data = (
                connection
                .execute(stmt)
                .fetchall()
            )
        )
    )

    if results.empty:
        HTTPException(
            status_code=404, 
            detail=f"No data found",
        )

    return results.to_dict(
        orient = "split",
        index = False,
    )

@app.get("/city/{city}")
def get_city_data(city: str):
    """
    Returns all data from the fastapi table for a given city
    """
    ## Create an sql statement to select ALL columns from the census table
    stmt = (
        ## Select ALL columns from the census table
        select(
            fastapi
        )
        ## Order by the Year column
        .order_by(
            fastapi.columns.Year.desc()
        )
        .where(
            ~fastapi.columns.Municipality.startswith('Bezirk')
        )
        
    )
    
    ## Create a DataFrame from the results
    results = (
        pd.DataFrame(
            data = (
                connection
                .execute(stmt)
                .fetchall()
            )
        )
        [lambda results: results.Municipality.str.contains(city)]
    )
    if city not in results.Municipality:
        HTTPException(
            status_code=404, 
            detail=f"No data found to city {city}",
        )

    return results.to_dict(
        orient = "split",
        index = False,
    )

@app.get("/district/{district}")
def get_district_data(district: str):
    """
    Returns all data from the fastapi table for a given district
    """
    ## Create an sql statement to select ALL columns from the census table
    stmt = (
        ## Select ALL columns from the census table
        select(
            fastapi
        )
        ## Order by the Year column
        .order_by(
            fastapi.columns.Year.desc()
        )
        .where(
            fastapi.columns.Municipality.startswith('Bezirk')
        )
        
    )
    
    ## Create a DataFrame from the results
    results = (
        pd.DataFrame(
            data = (
                connection
                .execute(stmt)
                .fetchall()
            )
        )
        [lambda results: results.Municipality.str.contains(district)]
    )
    if district not in results.Municipality:
        HTTPException(
            status_code=404, 
            detail=f"No data found to district {district}",
        )

    return results.to_dict(
        orient = "split",
        index = False,
    )
