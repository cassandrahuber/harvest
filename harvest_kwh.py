import pandas as pd
        
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
                window = pd.Timedelta(minutes=15)

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
