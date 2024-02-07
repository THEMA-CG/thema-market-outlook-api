from API_script import Thema_technology_data_API
import requests
import pandas as pd
import os
from getpass import getpass

# specify username and password
username = "yourEmail@company.com"
password = getpass()

# specify a folder for the output
output_folder = "Thema_API_output/Technology_outlook/"
os.makedirs(output_folder, exist_ok=True)

# initiates an API object of type Thema_technology_data_API
API_object = Thema_technology_data_API(username=username, password=password)

# example of fetching master data
master_data = API_object.get_master_data()

# create master data excel file
with pd.ExcelWriter(f"{output_folder}Master_data.xlsx", engine="xlsxwriter") as writer:
    for name, df in master_data.items():
        df.to_excel(writer, sheet_name=str(name), index=False)

# Hourly Data input example
# No parameters are mandatory. Empty edition parameter will give the latest edition. 
# If other parameters are empty, all possible variations of that parameter will be returned.
# You can specify multiple values per parameter by encapsulating in {}. Script will fetch all valid combinations
json = {
  "scenario": "Base",
  "country": "Germany",
  #"edition": "October 2023",
  "indicator": "Generation",
  "technology": {"Wind Onshore", "Wind Offshore"},
  #"Category": "Bottomfixed"
}

# example of calling the hourly data API and writing the results to excel
annual_data = API_object.get_annual_data(json)
annual_data.to_excel(f"{output_folder}Annual_data.xlsx", index=False)