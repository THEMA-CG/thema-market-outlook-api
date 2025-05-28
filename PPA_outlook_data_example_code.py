from API_script import Thema_PPA_data_API
import pandas as pd
import os
from getpass import getpass

# specify username and password
username = "yourEmail@company.com"
password = getpass()

# specify a folder for the output
output_folder = "Thema_API_output/PPA/"
os.makedirs(output_folder, exist_ok=True)

# initiates an API object of type Thema_data_API
API_object = Thema_PPA_data_API(username=username, password=password)

# example of fetching master data
master_data = API_object.get_master_data()

# create master data excel file
with pd.ExcelWriter(f"{output_folder}Master_data.xlsx", engine="xlsxwriter") as writer:
    for name, df in master_data.items():
        df.to_excel(writer, sheet_name=str(name), index=False)

# PPA Data input example
# can specify multiple values per parameter by encapsulating in {}. Script will fetch all valid combinations
# parameters that are not specified, or given as None, will be filled with all valid inputs by program. 
# Note that this might lead to high memory usage and long execution time
json = {
    "scenario": "Base",
    "edition": "December 2024",
    "zone": "Spain",
    "group": "Solar"
}
PPA_data = API_object.get_PPA_data(json)
PPA_data.to_excel(f"{output_folder}PPA_data.xlsx", index=False)