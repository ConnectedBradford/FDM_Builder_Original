import datetime
from FDMBuilder.FDM_helpers import *
from google.cloud import bigquery
import pandas as pd
import numpy as np 


# Set global variables
PROJECT = "yhcr-prd-phm-bia-core"
CLIENT = bigquery.Client(project=PROJECT)


def generate_random_dates(n=1, from_year=1950, to_year=2022):
    """Generates a series of random dates
    
    Args:
        n: int, number of dates to generate
        from_year: int, the min year dates will be taken from
        to_year: int, the max year the dates will be taken to
        
    Returns:
        pandas.Series, containing number of random dates requested
    
    """
    dates = np.concatenate(
        (np.random.choice(range(from_year, to_year), (n,1)), 
         np.random.choice(range(1,13), (n,1)), 
         np.random.choice(range(1,29), (n,1))), 
        axis=1
    )
    return pd.Series([datetime.date(year=year, month=month, day=day) 
                     for year, month, day in dates],
                     dtype="datetime64[ns]")


def add_random_days(date, upper=3652):
    """Adds a random number of days to a datetime
    
    Args:
        date: datetime.datetime, date to which random days will be added
        upper: int, the maximum number of days that can be added
        
    Returns:
        datetime.datetime, with original date plus random number of added
            days
    """
    n_rand_days = int(np.random.choice(range(upper)))
    return date + pd.offsets.DateOffset(days=n_rand_days)


def sub_random_days(date, upper=3652):
    """Subtracts a random number of days from a datetime
    
    Args:
        date: datetime.datetime, date from which random days will be subtracted
        upper: int, the maximum number of days that can be subtracted
        
    Returns:
        datetime.datetime, with original date minus random number of subtracted 
            days
    """
    n_rand_days = int(np.random.choice(range(upper)))
    return date - pd.offsets.DateOffset(days=n_rand_days)
