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
        
def process_kwh(data_path):
    """
    Interpolate kwh readings to exact 15 minute intervals. Contains boolean 'interpolated'
    column to indicate if the row was interpolated or not, and boolean 'is_exact' column
    to indicate if row is at an exact 15 minute interval.

    Parameters:
        data_path (str): Path to the CSV file containing meter orignial data.
    
    Returns:
        dataframe: Dataframe with interpolated kwh readings at exact 15 minute intervals.
    """
    # load raw data from csv
    df = pd.read_csv(data_path, encoding='utf-8')

    # drop the 3_phase_watt_total column as its not needed for kwh interpolation
    if '3_phase_watt_total' in df.columns:
        df.drop('3_phase_watt_total', axis=1, inplace=True)

    # convert datatetime column to a datetime type
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
    
    df['is_exact'] = False
    df['interpolated'] = False
    
    all_rows = []

    for meter_name, meter_group in df.groupby('meter_name'):
        meter_group = meter_group.sort_values('datetime').reset_index(drop=True)
        
        # create target intervals
        start = meter_group['datetime'].min().floor('15min')
        end = meter_group['datetime'].max().ceil('15min')
        
        # create list of every exact 15min timestamp that SHOULD exist for that meter
        target_intervals = pd.date_range(start=start, end=end, freq='15min')
        
        # for each target interval check if a real reading exists
        for interval in target_intervals:
            exact_match = meter_group[meter_group['datetime'] == interval]
            
            if not exact_match.empty:
                # real reading exists at this exact interval
                row = exact_match.iloc[0].copy()
                row['is_exact'] = True
                row['interpolated'] = False
                all_rows.append(row)
            else:
                # only look within 15 minutes either side
                window = pd.Timedelta(mintues=15)

                before_rows = meter_group[
                    (meter_group['datetime'] < interval) & 
                    (meter_group['datetime'] >= interval - window)
                ]
                after_rows = meter_group[
                    (meter_group['datetime'] > interval) & 
                    (meter_group['datetime'] <= interval + window)
                ]

                # only interpolate if we have readings on BOTH sides
                if not before_rows.empty and not after_rows.empty:
                    time_before = before_rows.iloc[-1]  # closest before
                    time_after = after_rows.iloc[0]     # closes after

                    time_diff = (time_after['datetime'] - time_before['datetime']).total_seconds()
                    reading_diff = time_after['kwh'] - time_before['kwh']

                    if time_diff == 0 or reading_diff == 0:
                        # if no time difference or no reading difference, use the before reading
                        estimated_kwh = time_before['kwh']
                    else:
                        slope = round(reading_diff / time_diff, 4)
                        sec_before_interval = (interval - time_before['datetime']).total_seconds()
                        estimated_kwh = time_before['kwh'] + (slope * sec_before_interval)

                    # create interpolated row
                    new_row = time_before.copy()
                    new_row['datetime'] = interval
                    new_row['kwh'] = estimated_kwh
                    new_row['is_exact'] = True
                    new_row['interpolated'] = True

                    # add new interpolated row to list
                    all_rows.append(new_row)
                    
                else:
                    # no close enough readings on one or both sides, skip this interval
                    pass

        # keep original nonexact interval rows:

        # creates T/F column for whether the datetime is in exact interval, then filter to only nonexact rows with ~
        non_exact = meter_group[~meter_group['datetime'].isin(target_intervals)]

        # add non exact rows to list of all rows
        for _, row in non_exact.iterrows():     # _ is a placeholder for row index (don't need it)
            all_rows.append(row)

    # create final dataframe from list of all rows, sort by meter name and datetime, reset index
    result = pd.DataFrame(all_rows)
    result = result.sort_values(by=['meter_name', 'datetime']).reset_index(drop=True)
    
    return result

def duplicate_check(df):
    """
    Check for duplicate rows in the dataframe and print them.

    Parameters:
        df (dataframe): The dataframe to check for duplicates.
    """
    duplicate_data = df[df.duplicated(keep=False)]
    if duplicate_data.empty:
        print("No duplicate rows found.")
    else:
        print("Duplicate rows found:")
        print(duplicate_data)

def meter_list(csv):
    """
    Print the list of unique meter names in the CSV file.
    
    Parameters:
        csv (str): Path to the CSV file.
    """
    df = pd.read_csv(csv, encoding='utf-8')
    print('List of meter names: \n', df['meter_name'].unique())
