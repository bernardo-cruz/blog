import pandas as pd

from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException

from sqlalchemy import create_engine, MetaData, Table, select, insert, update, delete

## Create a metadata object
metadata = MetaData()

## Create a connection to the database
engine = create_engine(              
    ## Path to database                     
    'sqlite:///../../data/FastAPI.db',      # <1>
)

## Reflect census table from the engine: census
fastapi = Table(
    "fastapi", 
    metadata,
    autoload_with = engine,
)

## Helper function to fetch data
def fetch_return_dict(
        stmt, 
        engine = engine,
    ) -> dict:
    """
    Utility function to convert sql query results to dict via pandas dataframe
    """
    ## Create a connection to the database
    connection = engine.connect()
    data = connection.execute(stmt).fetchall()
    connection.close()

    return (
        pd.DataFrame(
            data = data
        )
        .to_dict(
            orient = "records", 
        )
    )

## Instantiate a FastAPI object
app = FastAPI(
    title           =   "My first API",
    description     =   """
    API for access of wealth tax data from the canton of Zurich, Switzerland. 
    Basic CRUD operations are supported (Create, Read, Update, Delete)

    # To-Do:
    - Add tag parameter to routes decorators see https://fastapi.tiangolo.com/tutorial/metadata/
    - Check HTTPException status codes
    
    # Future:
    - Add more routes
    - Create Authentication
    - Create separate modules for routes, database connection, etc.

    © 2023, Bernardo Freire Barboza da Cruz
    """,
    version         =   "0.0.1",
)

## Create a BaseModel Child class for data representation
class TaxModel(BaseModel):
    Municipality    :   str    =  Field("Name of item")
    Tax             :   int    =  Field("Average tax for a given year")
    Year            :   int    =  Field("Year of meassurement")

class MunicipalityDataOut(BaseModel):
    Tax             :   int    =  Field("Average tax for a given year")
    Year            :   int    =  Field("Year of meassurement")
    
class TaxDataOut(BaseModel):
    Municipality    :   str    =  Field("Name of item")
    Year            :   int    =  Field("Year of meassurement")

class YearDataOut(BaseModel):
    Municipality    :   str    =  Field("Name of item")
    Tax             :   int    =  Field("Average tax for a given year")
 
## Create a index route
@app.get("/")
def index() -> list[TaxModel]:
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

    result = fetch_return_dict(stmt)

    if result:
        return result
    else:
        raise HTTPException(
            status_code = 404, 
            detail = f"Item not found",
        )

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
            fastapi.columns.Tax
        )
        ## Order by the Year column
        .order_by(
            fastapi.columns.Year.desc()
        )
        .where(
            fastapi.columns.Year == year
        )
    )
    
    ## Execute the statement and fetch the results
    result = fetch_return_dict(stmt)
    
    ## If the result is not empty return it, otherwise raise an exception
    if result:
        return result
    else:
        raise HTTPException(
            status_code = 404, 
            detail = f"Item not found",
        )

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
            fastapi.columns.Tax
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
    
    ## Execute the statement and fetch the results
    result = fetch_return_dict(stmt)
    
    ## If the result is not empty return it, otherwise raise an exception
    if result:
        return result
    else:
        raise HTTPException(
            status_code = 404, 
            detail = f"Item not found",
        )

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
            fastapi.columns.Tax
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
    
    ## Execute the statement and fetch the results
    result = fetch_return_dict(stmt)
    
    ## If the result is not empty return it, otherwise raise an exception
    if result:
        return result
    else:
        raise HTTPException(
            status_code = 404, 
            detail = f"Item not found",
        )

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
            fastapi.columns.Tax
        )
        ## Order by the Year column
        .order_by(
            fastapi.columns.Year.desc()
        )
        .where(
            fastapi.columns.Municipality.contains('Kanton')
        )
        
    )
    
    ## Execute the statement and fetch the results
    result = fetch_return_dict(stmt)
    
    ## If the result is not empty return it, otherwise raise an exception
    if result:
        return result
    else:
        raise HTTPException(
            status_code = 404, 
            detail = f"Item not found",
        )

## Create a new entry
@app.post('/entry/{municipality}/{year}/{tax}')
def create_new_entry(
        municipality    : str = None,
        year            : int = None,
        tax             : int = None,
    ):
    """
    Create a new entry in database
    """
    in_stmt = (
        select(
            fastapi
        )
        .where(
            fastapi.columns.Municipality == municipality,
            fastapi.columns.Year == year,
        )
    )
    
    connection = engine.connect()
    result = connection.execute(in_stmt).fetchall()
    
    if result:
        raise HTTPException(
            status_code=405, 
            detail = f"Item with name {municipality} and year {year} already exists. "
            + "Use update to change this value or delete this entry and retry",
        )
    else:
        stmt = (
            insert(
                fastapi
            )
            .values(
                Municipality = municipality,
                Year = year,
                Tax = tax,
            )
        )

        connection.execute(stmt)
        connection.commit()
        connection.close()

        return {"success": f"Item with name {municipality}; tax of {tax}; and year {year} added."}

## Update an income tax entry
@app.put("/update_tax/{municipality}/{year}/{tax}")
def update_tax_entry(
        municipality    : str = None,
        year            : int = None,
        tax             : int = None,
    ):
    """
    Update income tax value for a given municipality and year 
    """
    in_stmt = (
        select(
            fastapi
        )
        .where(
            fastapi.columns.Municipality == municipality,
            fastapi.columns.Year == year,
        )
    )
    
    connection = engine.connect()
    result = connection.execute(in_stmt).fetchall()

    if not result:
        raise HTTPException(
            status_code=404, 
            detail = f"Item with name {municipality} and year {year} not found. "
            + "Only values available in database can be updated"
        )
    else:
        stmt = (
            update(
                fastapi
            )
            .where(
                fastapi.columns.Municipality == municipality,
                fastapi.columns.Year == year,
            )
            .values(
                Tax = tax,
            )
        )

        connection.execute(stmt)
        connection.commit()
        connection.close()

        return {"success": f"Item with name {municipality}; and year {year} updated to tax of {tax};"}
    
## update year entry
@app.put("/update_year/{municipality}/{year_old}/{year_new}")
def update_year_entry(
        municipality    : str = None,
        year_old        : int = None,
        year_new        : int = None,
    ):
    """
    Update year of a municipality entry
    """
    in_stmt = (
        select(
            fastapi
        )
        .where(
            fastapi.columns.Municipality == municipality,
            fastapi.columns.Year == year_old,
        )
    )
    
    connection = engine.connect()
    result = connection.execute(in_stmt).fetchall()

    if not result:
        raise HTTPException(
            status_code=404, 
            detail = f"Item with name {municipality} and year {year_old} not found. "
            + "Only values available in database can be updated"
        )
    else:
        stmt = (
            update(
                fastapi
            )
            .where(
                fastapi.columns.Year == year_old,
            )
            .values(
                Year = year_new,
            )
        )

        connection.execute(stmt)
        connection.commit()
        connection.close()

        return {"success": f"Item with name {municipality}; and year {year_old} updated to year {year_new};"}

@app.delete("/delete/{municipality}/{year}/{tax}")
def delete_tax_entry(
        municipality    : str = None,
        year            : int = None,
        tax             : int = None,
    ):
    """
    Delete entry given municipality, year and income tax value
    """
    in_stmt = (
        select(
            fastapi
        )
        .where(
            fastapi.columns.Municipality == municipality,
            fastapi.columns.Year == year,
        )
    )
    
    connection = engine.connect()
    result = connection.execute(in_stmt).fetchall()
    
    if not result:
        raise HTTPException(
            status_code=404, 
            detail = f"Item with name {municipality} and year {year} not found. "
            + "Only values in database can be deleted",
        )
    else:
        stmt = (
            delete(
                fastapi
            )
            .where(
                fastapi.columns.Municipality == municipality,
                fastapi.columns.Year == year,
                fastapi.columns.Tax == tax,
            )
        )

        connection.execute(stmt)
        connection.commit()
        connection.close()

        return {"success": f"Item with name {municipality}; tax of {tax}; and year {year} deleted."}

