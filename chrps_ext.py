import xarray as xr
import pandas as pd
import os
import glob

# --- Configuration ---
stations_file = "stations.csv"
chirps_dir = "CHIRPS"
output_filename = "chirps_1981_2021.csv"

# --- Functions ---
def extract_station_data(nc_file, lon, lat):
    """Extracts rainfall data ('rfe') from a CHIRPS netCDF file, handling various errors."""
    try:
        with xr.open_dataset(nc_file) as ds:
            if 'rfe' in ds:
                data = ds['rfe'].sel(lon=lon, lat=lat, method='nearest')
                if data.size == 0:
                    print(f"Warning: No data returned for coordinates ({lon}, {lat}) in {nc_file}.")
                    return None
                if data.ndim == 0:
                    return data.item()  # Return the single value in a 0-dimensional array
                else:
                    return data.values[0]  # This is for cases where it's a 1D array with at least one element
            else:
                print(f"Warning: 'rfe' variable not found in {nc_file}. Skipping.")
                return None
    except (FileNotFoundError, OSError) as e:
        print(f"Error opening file {nc_file}: {e}")
        return None
    except KeyError as e:
        print(f"Error accessing variable in {nc_file}: {e}")
        return None
    except ValueError as e:
        print(f"Value Error in {nc_file}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while processing {nc_file}: {e}")
        return None


# --- Main Script ---
try:
    stations_df = pd.read_csv(stations_file)
    nc_files = sorted(glob.glob(os.path.join(chirps_dir, "*.nc")))
    all_data = {}

    # Initialize the dictionary with dates as keys
    for nc_file in nc_files:
        try:
            date_str = os.path.basename(nc_file).split("_")[1].split(".")[0]
            date = pd.to_datetime(date_str, format='%Y%m%d')
            all_data[date] = {}
        except (ValueError, IndexError) as e:
            print(f"Could not parse date from filename: {nc_file}, Error: {e}")
            continue

    for index, row in stations_df.iterrows():
        station_name = row['station']
        lon = row['lon']
        lat = row['lat']

        for nc_file in nc_files:
            try:
                date_str = os.path.basename(nc_file).split("_")[1].split(".")[0]
                date = pd.to_datetime(date_str, format='%Y%m%d')
            except (ValueError, IndexError) as e:
                print(f"Could not parse date from filename: {nc_file}, Error: {e}")
                continue

            data = extract_station_data(nc_file, lon, lat)
            if data is not None:
                all_data[date][station_name] = data

    # Convert to DataFrame
    output_df = pd.DataFrame(all_data).transpose()
    output_df.index.name = 'date'
    
    # Save to CSV
    output_df.to_csv(output_filename)
    print(f"Data extraction complete. Results saved to {output_filename}")

except Exception as e:
    print(f"A critical error occurred: {e}")
