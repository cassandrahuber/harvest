import pandas as pd
from datetime import datetime

# note: pass start and end or make equal to itself in passing
def make_filename(df, name, var, ext="csv"):
    """
    Create a filename with the format: name_var_YYMMDD-YYMMDD.ext, where the first date is the
    minimum datetime in the dataframe and the second date is the maximum datetime in the
    dataframe.

    Parameters:
        df (dataframe): The dataframe containing the datetime column.
        base_name (str): The base name for the file (e.g. "meter_data_kw").

    Returns:
        str: The generated filename.
    """

    # ensure datetime column is in datetime format
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    min = df['datetime'].min().strftime('%y%m%d')
    max = df['datetime'].max().strftime('%y%m%d')
    
    return f'{name}_{var}_{min}-{max}.{ext}'
