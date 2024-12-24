# nearest index match
# match code using nearest index match

import pandas as pd
from pathlib import Path
import time


def find_nearest_index(df, lat, lon):
    squared_distances = (df['Latitude'] - lat) ** 2 + (df['Longitude'] - lon) ** 2
    nearest_index = squared_distances.idxmin()
    return nearest_index


start = time.time()
# Initialize an empty dictionary to store DataFrames
collated_bloom_dict = {}

# Loop through sheets from 'Sheet_1' to 'Sheet_100'
for sheet_number in range(1, 101):
    sheet_name = f'{sheet_number}'

    # Load the DataFrame from the current sheet
    collated_bloom = pd.read_excel("/home1/kyeongpi/P2/data/bloom2022/bloom2022_combined.xlsx",
                                   sheet_name=sheet_name)

    # Parameterizing chl files
    chl_sorted_dir = "/home1/kyeongpi/P2/data/chl/2022"
    chl_sorted_files = sorted(Path(chl_sorted_dir).glob('*.csv'))

    # Parameterizing sol Fe dep. files
    mimi_out_dir = "/home1/kyeongpi/P2/data/mimi_out/2022_sol"
    mimi_out_files = sorted(Path(mimi_out_dir).glob('*.csv'))

    # Parameterizing duwt001 files
    duwt001_dir = "/home1/kyeongpi/P2/data/duwt001"
    duwt001_files = sorted(Path(duwt001_dir).glob('*.csv'))

    # Initialize an empty list to store merged DataFrames
    merged_dfs = []

    # Loop through each row of chl_sorted with collated_bloom
    for index, row in collated_bloom.iterrows():
        datetime_value, latitude_value, longitude_value = row['datetime'], row['Latitude'], row['Longitude']
        start = time.time()

        # Find the corresponding chl_sorted CSV file based on the date
        date_str = datetime_value.strftime('%Y_%m_%d')
        chl_sorted_file = next((file for file in chl_sorted_files if date_str in str(file)), None)
        mimi_out_file = next((file for file in mimi_out_files if date_str in str(file)), None)
        duwt001_file = next((file for file in duwt001_files if date_str in str(file)), None)

        if chl_sorted_file:
            chl_sorted_day = pd.read_csv(chl_sorted_file)
            nearest_index = find_nearest_index(chl_sorted_day, latitude_value, longitude_value)
            nearest_row = chl_sorted_day.loc[nearest_index]
            # print(f"   Found chl_sorted match, nearest_index: {nearest_index}")

        if mimi_out_file:
            mimi_out_day = pd.read_csv(mimi_out_file)
            mimi_out_day['Date'] = pd.to_datetime(mimi_out_day['Date'])
            nearest_index_Fe = find_nearest_index(mimi_out_day, latitude_value, longitude_value)
            nearest_row_Fe = mimi_out_day.loc[nearest_index_Fe]
            # print(f"   Found mimi_out match, nearest_index_Fe: {nearest_index_Fe}, nearest_row_Fe_datetime: {nearest_row_Fe['Date']}")

        if duwt001_file:
            duwt001_day = pd.read_csv(duwt001_file)
            nearest_index_wd = find_nearest_index(duwt001_day, latitude_value, longitude_value)
            nearest_row_wd = duwt001_day.loc[nearest_index_wd]

        # Create a DataFrame with the merged values
        merged_df = pd.DataFrame({
            'datetime_traj': datetime_value,
            'Latitude_traj': latitude_value,
            'Longitude_traj': longitude_value,
            'Latitude_chl': nearest_row['Latitude'],
            'Longitude_chl': nearest_row['Longitude'],
            'Chlorophyll-a': nearest_row['Chlorophyll-a'],
            'datetime_chl': date_str,
            'datetime_solFe': nearest_row_Fe['Date'],
            'Latitude_solFe': nearest_row_Fe['Latitude'],
            'Longitude_solFe': nearest_row_Fe['Longitude'],
            'FeSolAll': nearest_row_Fe['FESOLALL_Data'],
            'FeAnSolAll': nearest_row_Fe['FEANSOLALL_Data'],
            'FeBbSolAll': nearest_row_Fe['FEBBSOLALL_Data'],
            'FeDuSolAll': nearest_row_Fe['FEDUSOLALL_Data'],
            'Latitude_wd': nearest_row_wd['Latitude'],
            'Longitude_wd': nearest_row_wd['Longitude'],
            'DUWT001': nearest_row_wd['DUWT001'],
        }, index=[index])

        merged_dfs.append(merged_df)

    if merged_dfs:
        final_merged_df = pd.concat(merged_dfs, ignore_index=True)
        csv_output_path = f"/home1/kyeongpi/P2/output/2022_bloom_sol/2022_bloomsol{sheet_name}.csv"
        final_merged_df.to_csv(csv_output_path, index=False)
        print(f"Final Merged DataFrame saved to: {csv_output_path}")
    else:
        print("No valid rows to concatenate.")
    end = time.time()
    print(f' small loop time elapsed: {end - start}')
end1 = time.time()
print(f' total time: {end1 - start}')
