import pandas as pd
import os

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
    Get CSV paths organized by meter name.

    Parameters:
        base_path (str): The base path containing subfolders for each meter.

    Returns:
        dict: Dictionary with meter_name as key and list of CSV paths as values.
    """
    csv_data = {}
    
    for subfolder in os.listdir(base_path):
        # create path for each subfolder
        folder_path = os.path.join(base_path, subfolder)

        # skip if not a directory
        if not os.path.isdir(folder_path):
            continue

        # get the name of the meter from the subfolder name
        meter_name = subfolder.lower().replace(' ', '_').replace('_mtr', '')

        # list of csv file paths in subfolder
        # ignore hiddent '._' files on macOS
        csv_files = [os.path.join(folder_path, f)
                     for f in os.listdir(folder_path)
                     if f.endswith('.csv')
                     #and not f.startswith('._')
                     and not f.startswith('.')
                    ]
        
        # store list of file paths in dictionary where key is meter name
        if csv_files:
            csv_data[meter_name] = csv_files

    return csv_data

def load_meter_dfs(basepath):
    """
    Load meter data from CSV files in the specified base path.
    
    Parameters:
        basepath (str): The base path containing subfolders for each meter.
    Returns:
        list: List of dataframes, one for each meter.
    """
    # get list of csv file paths and their meter names
    csv_data = get_csv_paths(basepath)

    meters_df = []

    for meter_name, csv_paths, in csv_data.items():
        dfs = []

        for csv_path in csv_paths:
            df = pd.read_csv(csv_path, encoding='utf-8')
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

            # error in scripts, total_watt_hour is actually total kwh
            if 'total_watt_hour' in df.columns:
                df.rename(columns={'total_watt_hour': 'kwh'}, inplace=True)

            # rename columns, some meters have different label but they are synonymous
            if '3_phase_positive_real_energy_used' in df.columns:
                df.rename(columns = {
                    '3_phase_positive_real_energy_used': 'kwh',
                    '3_phase_real_power': '3_phase_watt_total'
                }, inplace=True)

            # reorder columns
            df = df[['datetime', 'kwh', '3_phase_watt_total']]

            dfs.append(df)

        # combine all CSVs for this meter
        combined_df = pd.concat(dfs, ignore_index=True)

        # add meter name column
        combined_df.insert(1, 'meter_name', meter_name)

        # sort by datetime
        combined_df['datetime'] = pd.to_datetime(combined_df['datetime'])
        combined_df.sort_values(by='datetime', inplace=True)

        meters_df.append(combined_df)
    
    return meters_df

def concat_meter_dfs(meter_dfs):
    """
    Concatenate a list of meter dataframes into a single dataframe.

    Parameters:
        meter_dfs (list): List of dataframes, one for each meter.

    Returns:
        dataframe: Combined dataframe containing data from all meters.
    """
    combined_df = pd.concat(meter_dfs, ignore_index=True)

    return combined_df