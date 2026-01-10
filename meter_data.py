import pandas as pd
import os
#import numpy as np

def validate_base_path(path):
    """
    Validate if the base path exists.

    Parameters:
        path (str): The base path to validate.
    
    Returns:
        bool: True if the path exists, False otherwise.
    """
    return os.path.exists(path)

def get_csv_paths(base_path):
    """
    """
    csv_paths = []
    
    for subfolder in os.listdir(base_path):
        # create path for each subfolder
        folder_path = os.path.join(base_path, subfolder)

        # get the name of the meter from the subfolder name
        meter_name = subfolder.lower().replace(' ', '_').replace('_mtr', '')

        # list of csv file paths in subfolder
        # ignore hiddent '._' files on macOS
        csv_paths = csv_paths.append([os.path.join(folder_path, f)
                     for f in os.listdir(folder_path)
                     if f.endswith('.csv')
                     #and not f.startswith('._')
                     and not f.startswitch('.')
                     ])

    return csv_paths, meter_name

def load_meter_dfs(basepath):
    """
    """
    csv_paths = get_csv_paths(basepath)

    meters_df = []

    for csv in csv_paths:
        df = pd.read_csv(csv, encoding='utf-8')

        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        # error in scripts, total_watt_hour is actually total kwh
        df.rename(columns={'total_watt_hour', 'kwh'}, inplace=True)

        # rename columns, some meters have different label but they are synonymous
        if '3_phase_positive_real_energy_used' in df.colums:
            df.rename(columns = {
                '3_phase_positive_real_energy_used': 'kwh',
                '3_phase_real_power': '3_phase_watt_total'
            }, inplace=True)

        # reorder columns
        df = df[['datetime, kwh, 3_phase_watt_total']]
        
        # add meter's name column
        df.insert(1, 'meter_name', meter_name)
        
        meters_df.append(df)
        
    return meters_df

