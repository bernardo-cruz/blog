import pandas as pd

from enum import Enum
from typing import Union
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Path, Query

from sqlalchemy import create_engine, MetaData, Table, select, insert, update

## Create a metadata object
metadata = MetaData()

## Create a connection to the database
engine = create_engine(              
    ## Path to database                     
    'sqlite:///../../data/FastAPI.db',      # <1>
)

## Create a connection to the database
connection = engine.connect()
engine.execution_options(isolation_level = "AUTOCOMMIT")

## Reflect census table from the engine: census
fastapi = Table(
    "fastapi", 
    metadata,
    autoload_with = engine,
)

## Helper function to fetch data
def fetch_return_dict(
        stmt, 
        connection : create_engine= connection,
    ) -> dict:
    """
    Utility function to convert sql query results to dict via pandas dataframe
    """
    return (
        pd.DataFrame(
            data = (
                connection
                .execute(stmt)
                .fetchall()
            )
        )
        .to_dict(
            orient = "records", 
        )
    )

## Instantiate a FastAPI object
app = FastAPI(
    title           =   "My first API",
    description     =   "API for access of income tax data from the canton of Zurich, Switzerland",
    version         =   "0.0.1",
)

## Create a BaseModel Child class for data representation
class IncomeTaxModel(BaseModel):
    Municipality    :   str    =  Field("Name of item")
    IncomeTax       :   int    =  Field("Average income tax for a given year")
    Year            :   int    =  Field("Year of meassurement")

class MunicipalityDataOut(BaseModel):
    IncomeTax       :   int    =  Field("Average income tax for a given year")
    Year            :   int    =  Field("Year of meassurement")
    
class IncomeTaxDataOut(BaseModel):
    Municipality    :   str    =  Field("Name of item")
    Year            :   int    =  Field("Year of meassurement")

class YearDataOut(BaseModel):
    Municipality    :   str    =  Field("Name of item")
    IncomeTax       :   int    =  Field("Average income tax for a given year")
 
## Create a index route
@app.get("/")
def index() -> list[IncomeTaxModel]:
    """
    Returns all data from the fastapi table
    """
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

    return fetch_return_dict(stmt)

## Create a route to return data for a given year
@app.get("/year/{year}")
def get_data_of_year(year: int) -> list[YearDataOut]:
    """
    Returns all data from the fastapi table for a given year
    """
    ## Create an sql statement to select ALL columns from the census table
    stmt = (
        ## Select ALL columns from the census table
        select(
            fastapi.columns.Municipality, 
            fastapi.columns.IncomeTax
        )
        ## Order by the Year column
        .order_by(
            fastapi.columns.Year.desc()
        )
        .where(
            fastapi.columns.Year == year
        )
    )
    
    return fetch_return_dict(stmt)

## Create a route to return data for a given city
@app.get("/municipality/{municipality}")
def get_municipality_data(municipality: str) -> list[MunicipalityDataOut]:
    """
    Returns all data from the fastapi table for a given municipality
    """
    ## Create an sql statement to select ALL columns from the census table
    stmt = (
        ## Select ALL columns from the census table
        select(
            fastapi.columns.Year, 
            fastapi.columns.IncomeTax
        )
        ## Order by the Year column
        .order_by(
            fastapi.columns.Year.desc()
        )
        .where(
            (~fastapi.columns.Municipality.startswith('Bezirk')) & 
            (~fastapi.columns.Municipality.startswith('Region')) &
            (fastapi.columns.Municipality == municipality)
        )
        
    )
    
    return fetch_return_dict(stmt)

## Create a route to return data for a given district
@app.get("/district/{district}")
def get_district_data(district: str) -> list[MunicipalityDataOut]:
    """
    Returns all data from the fastapi table for a given district
    """
    ## Create an sql statement to select ALL columns from the census table
    stmt = (
        ## Select ALL columns from the census table
        select(
            fastapi.columns.Year, 
            fastapi.columns.IncomeTax
        )
        ## Order by the Year column
        .order_by(
            fastapi.columns.Year.desc()
        )
        .where(
            (fastapi.columns.Municipality.startswith('Bezirk')) & 
            (fastapi.columns.Municipality.contains(district))
        )
        
    )
    
    return fetch_return_dict(stmt)

## Create a route to return data from the canton
@app.get("/canton/")
def get_canton_data() -> list[MunicipalityDataOut]:
    """
    Returns all data from the fastapi table for the canton
    """
    ## Create an sql statement to select ALL columns from the census table
    stmt = (
        ## Select ALL columns from the census table
        select(
            fastapi.columns.Year, 
            fastapi.columns.IncomeTax
        )
        ## Order by the Year column
        .order_by(
            fastapi.columns.Year.desc()
        )
        .where(
            fastapi.columns.Municipality.contains('Kanton')
        )
        
    )
    
    return fetch_return_dict(stmt)

## Create a new entry
@app.post('/entry/{municipality}/{year}/{incometax}')
def create_new_entry(
        municipality    : str = None,
        year            : int = None,
        incometax       : int = None,
    ):
    in_stmt = (
        select(
            fastapi
        )
        .where(
            fastapi.columns.Municipality == municipality,
            fastapi.columns.Year == year,
            fastapi.columns.IncomeTax == incometax,
        )
    )
    result = connection.execute(in_stmt)
    if result:
        raise HTTPException(
            status_code=400, 
            detail=f"Item with {municipality} {incometax} {incometax} already exists.",
        )
    else:
        stmt = (
            insert(
                fastapi
            )
            .values(
                Municipality = municipality,
                Year = year,
                IncomeTax = incometax,
            )
        )

        connection.execute(stmt)
        connection.commit()
        connection.close()

        return {"success": f"Item with {municipality} {incometax} {incometax} added"}
