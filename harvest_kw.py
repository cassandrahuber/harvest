import pandas as pd

def load_data(data_path, info_path):
    """
    Load meter data and meter info from CSV files. Prepare dataframe by cleaning and formatting.

    Parameters:
        data_path (str): Path to the meter data CSV.
        info_path (str): Path to the meter info CSV (contains meter model info).

    Returns:
        df (dataframe): Cleaned and formatted meter data.
        info_df (dataframe): Cleaned meter info data.
    """
    # load data from csv files into dataframes
    df = pd.read_csv(data_path, encoding='utf-8')
    info_df = pd.read_csv(info_path, encoding='utf-8')

    # remove total watt hour column, it is not relevant to these calculations
    if 'total_watt_hour' in df.columns:
        df.drop('total_watt_hour', axis=1, inplace=True)

    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values(by=['meter_name', 'datetime'])

    # remove extra columns
    info_df.drop(columns={'header1', 'header2'}, axis=1, inplace=True)
    
    # ensure all meter names are uniform
    info_df['meter_name'] = info_df['meter_name'].str.replace(' ', '_')
    
    return df, info_df

def filter_time_frame(df, start, end):
    """
    Filter meter data to only include rows within the specified datetime range.

    Parameters:
        df (dataframe): Meter data.
        start (datetime): Start datetime.
        end (datetime): End datetime.

    Returns:
        filtered_df (dataframe): Filtered meter data within the specified datetime range.
    """
    filtered_df = df[(df['datetime'] >= start) & (df['datetime'] <= end)].copy()
    return filtered_df


def process_kw_data(df, info_df):
    """
    Process meter data to calculate average kw per 15 minute interval, adjusting for meter model.
    PQM2 meters report in kw, while EPM7000 meters report in watts so will only divide EMP7000
    meters by 1000.

    Parameters:
        df (dataframe): Meter data.
        info_df (dataframe): Meter model info.

    Returns:
        result_df (dataframe): Processed data with average kw per 15 minute interval.
    """
    # create set of all meters of EPM7000 model
    model_check = set(info_df[info_df['meter_model'].str.contains('EPM7000')]['meter_name'])

    # create column that contains the interval that the row belongs to
    df['interval'] = df['datetime'].dt.floor('15min')

    # create result dataframe categorized by the meter name and interval calculate the average of the 3 phase watt total
    result_df = df.groupby(['meter_name', 'interval'])['3_phase_watt_total'].mean().reset_index()

    # create mean_kw column, dividing by 1000 for EPM7000 meters
    result_df['mean_kw'] = result_df['3_phase_watt_total'].copy()
    result_df.loc[result_df['meter_name'].isin(model_check), 'mean_kw'] /= 1000

    # rename and reorder columns, delete 3 phase column
    result_df.rename(columns={'interval': 'datetime'}, inplace=True)
    result_df.drop('3_phase_watt_total', axis=1, inplace=True)
    result_df = result_df[['datetime', 'meter_name', 'mean_kw']]

    return result_df

def load_data_for_comparison(harvest_csv, aurora_csv):
    """
    Load harvest's processed kw meter data from CSV and Aurora's from CSV for comparison.
    
    Parameters:
        harvest_csv (str): Path to harvest's processed kw data CSV.
        aurora_csv (str): Path to Aurora's processed kw data CSV.

    Returns:
        merged_df (dataframe): Merged dataframe containing both harvest's and Aurora's data.
        meters (list): List of unique meter names in the merged dataframe.
    """
    harvest_df = pd.read_csv(harvest_csv, encoding='utf-8')
    aurora_df = pd.read_csv(aurora_csv, encoding='utf-8')

    # convert datetime column to datetime type
    harvest_df['datetime'] = pd.to_datetime(harvest_df['datetime'])
    aurora_df['datetime'] = pd.to_datetime(aurora_df['datetime'])

    # merge the dataframes together on meter_name and datetime
    merged_df = pd.merge(harvest_df, aurora_df, on=['meter_name', 'datetime'], how='outer')
    merged_df.columns = merged_df.columns.str.lower().str.replace(' ', '_')

    # if blue_pillar_kw column exists, rename it to mean for consistency with database
    if 'blue_pillar_kw' in merged_df.columns:
        merged_df.rename({'blue_pillar_kw': 'mean'}, inplace=True)

    # make sure rows are sorted by meter name and datetime
    merged_df = merged_df.sort_values(by=['datetime', 'meter_name'])

    # create list of unique meters
    meters = merged_df['meter_name'].unique()

    return merged_df, meters

def create_plots_pdf(merged_df, meters, filename):
    """
    Create a PDF file with plots comparing harvest's 'mean_kw' and Aurora's 'mean' data for each meter.

    Parameters:
        merged_df (dataframe): Merged dataframe containing both harvest's and Aurora's data.
        meters (list): List of unique meter names.
        filename (str): Path to save the output PDF file.
    """
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    with PdfPages(filename) as pdf:
        for meter in meters:
            meter_data = merged_df[merged_df['meter_name'] == meter].sort_values('datetime')

            plt.figure(figsize=(10, 6))
            plt.plot(meter_data['datetime'], meter_data['mean_kw'], label="harvests_kw", alpha=0.7) # alpha is opacity of the line
            plt.plot(meter_data['datetime'], meter_data['mean'], label="auroras_kw", alpha=0.7)
            
            plt.xlabel('datetime')
            plt.ylabel('kw')
            plt.title(f'Meter: {meter}')
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()

            # save current plot to pdf
            pdf.savefig()
            plt.close() 

def get_comparison_info(merged_df, meters, corr_threshold, pct_threshold):
    """
    Create a dataframe summarizing the comparison between harvest's and Aurora's kw data for each meter.

    Parameters:
        merged_df (dataframe): Merged dataframe containing both harvest's ('mean_kw') and Aurora's data ('mean').
        meters (list): List of unique meter names.
    
    Returns:
        info_df (dataframe): Dataframe summarizing the comparison results.
    """
    import numpy as np

    # create dataframe to hold information
    info_df = pd.DataFrame({
        'meter_name': meters,
        'harvests': '',
        'auroras': '', 
        'match': ''
    })

    # make meter_name the index
    info_df.set_index('meter_name', inplace=True)
    
    for meter in meters:
        meter_data = merged_df[merged_df['meter_name'] == meter].sort_values('datetime')
        
        # check validity of harvest's kw data for meter
        if (meter_data['mean_kw'] == 0).all():
            info_df.loc[meter, 'harvests'] = 'zeros'
        elif meter_data['mean_kw'].isna().all():
            info_df.loc[meter, 'harvests'] = 'missing'
        else:
            info_df.loc[meter, 'harvests'] = 'ok'
            
        # check validity of aurora's kw data for meter 
        if (meter_data['mean'] == 0).all():
            info_df.loc[meter, 'auroras'] = 'zeros'
        elif meter_data['mean'].isna().all():
            info_df.loc[meter, 'auroras'] = 'missing'
        else:
            info_df.loc[meter, 'auroras'] = 'ok'
        
        # check if both are 'ok', then calculate
        if info_df.loc[meter, 'harvests'] == 'ok' and info_df.loc[meter, 'auroras'] == 'ok':
            # get non-na values for comparison (keeps rows ONLY if BOTH columsn have non-na values)
            valid_data = meter_data.dropna(subset=['mean_kw', 'mean']).copy()

            if len(valid_data) > 0:
                # calculate correlation
                correlation = valid_data['mean_kw'].corr(valid_data['mean'])

                # calculate percentage difference (how close the actual values are to eachother):
                # difference between the two values (absolute to get how different)
                difference = abs(valid_data['mean_kw'] - valid_data['mean'])

                # replace any zeros in 'mean_kw' column with 'NaN' to avoid division by zero errors (to get meaningful % difference)
                valid_data['mean_kw'] = valid_data['mean_kw'].replace(0, np.nan)

                # percent difference column
                valid_data['pct_diff'] = (difference / valid_data['mean_kw']) * 100

                # average percent difference for meter
                avg_pct_diff = valid_data['pct_diff'].mean()

                # threshold for "close enough" (ie: correlation > 0.95 or avg diff < 10%)
                # r = 1.0 is perfect positive correlation, want diff % low as possible
                if correlation > corr_threshold and avg_pct_diff < pct_threshold:
                    info_df.loc[meter, 'match'] = 'yes'
                elif correlation > corr_threshold:
                    info_df.loc[meter, 'match'] = f'yes (high r={correlation:.2f}) but missing data'
                else:
                    info_df.loc[meter, 'match'] = f'no (r={correlation:.2f}, avg_pct_diff={avg_pct_diff:.1f}%)'
            else:
                info_df.loc[meter, 'match'] = 'no valid data'
        else:
            info_df.loc[meter, 'match'] = 'n/a'
    
    return info_df