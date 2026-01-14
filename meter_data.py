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
        
def process_kwh(df):
    """
    Interpolate kwh readings to exact 15 minute intervals. Contains boolean 'interpolated'
    column to indicate if the row was interpolated or not, and boolean 'is_exact' column
    to indicate if row is at an exact 15 minute interval.

    Parameters:
        df (dataframe): Dataframe containing meter data with kwh readings.
    
    Returns:
        dataframe: Dataframe with interpolated kwh readings at exact 15 minute intervals.
    """
    # drop the 3_phase_watt_total column as its not needed for kwh interpolation
    df.drop('3_phase_watt_total', axis=1, inplace=True)

    # convert datatetime column to a datetime type
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
        
    # create column that contains the closest interval for each timestamp (contains ymd hms, using timedelta)
    df['interval_15min'] = df['datetime'].dt.round('15min')

    # create column that contains the offset in seconds from the closest interval for each timestamp
    # - is if its before it and + is if its after
    df['interval_offset'] = (df['datetime'] - df['interval_15min']).dt.total_seconds()

    # create new column with true if an exact interval and false if not
    df['is_exact'] = df['datetime'].eq(df['interval_15min'])

    df['interpolated'] = False
    all_interpolated_rows = []

    #process each meter separately
    for meter_name, meter_group in df.groupby('meter_name'):
        interpolated_rows = []

        # interval = the 15min bucket val, group = all rows in that bucket
        for interval, group in meter_group.groupby('interval_15min'):
            # only select rows in the group with is_exact == True
            exact = group[group['is_exact']]

            if exact.empty:
                before = group[group['interval_offset'] <= 0]
                after = group[group['interval_offset'] >= 0]

                # check if there are empties
                if not before.empty and not after.empty:
                    # grab the closest data to the interval
                    time_before = before.iloc[-1]
                    time_after = after.iloc[0]

                    # calculate the estimated kwh
                    # get the slope to 4 decimal places
                    reading_diff = time_after['kwh'] - time_before['kwh']

                    if reading_diff == 0:
                        # if no change in reading, use the before reading
                        estimated_kwh = time_before['kwh']
                    else:
                        # calculate slope
                        time_diff = (time_after['datetime'] - time_before['datetime']).total_seconds()
                        slope = round(reading_diff / time_diff, 4)
                        sec_before_interval = (interval - time_before['datetime']).total_seconds()
                        estimated_kwh = time_before['kwh'] + (slope * sec_before_interval)

                    # create interpolated row
                    new_row = time_before.copy()
                    new_row['datetime'] = interval
                    new_row['kwh'] = estimated_kwh
                    new_row['interval_offset'] = 0
                    new_row['is_exact'] = True
                    new_row['interpolated'] = True

                    # add new interpolated row to list
                    interpolated_rows.append(new_row)

        # add this meter's interpolated rows to the overall list
        all_interpolated_rows.extend(interpolated_rows)
        #all_interpolated_rows.append(interpolated_rows)

    # combine interpolated data with dataframe
    if all_interpolated_rows:
        df = pd.concat([df, pd.DataFrame(interpolated_rows)], ignore_index=True)

    df = df.drop(columns=['interval_15min', 'interval_offset'])

    # resort the data to be in order of datetime
    df = df.sort_values(by=['meter_name', 'datetime']).reset_index(drop=True)

    return df

def duplicate_check(df):
    """
    Check for duplicate rows in the dataframe and print them.

    Parameters:
        df (dataframe): The dataframe to check for duplicates.
    """
    print(df[df.duplicated(keep=False)])

def meter_list(csv):
    """
    Print the list of unique meter names in the CSV file.
    
    Parameters:
        csv (str): Path to the CSV file.
    """
    df = pd.read_csv(csv, encoding='utf-8')
    print(df['meter_name'].unique())
