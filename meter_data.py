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
