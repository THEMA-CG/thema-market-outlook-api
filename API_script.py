import requests
import pandas as pd
import time
from datetime import datetime


class Thema_API:

    def __init__(self, username, password):
        """
        Constructor initializing class variables
        :param username(str): Thema web portal username
        :param password(str): Thema web portal password
        """

        self.username = username
        self.password = password

        # specify all API URLs
        self.api_root_url = "https://portal.thema.no/customer-api/"
        self.authorization_url = f"{self.api_root_url}authenticate"
        self.masterdata_url = f"{self.api_root_url}masterdata"
        self.annualData_url = f"{self.api_root_url}annualData"
        self.hourlyData_url = f"{self.api_root_url}hourlyData"

        # initiates token_timestamp and a token expiration time
        self.token_timestamp = 0
        self.token_validity_time = 600

        # initiate master_data dict
        self.master_data = {}

    def __get_authorization_token(self):
        """
        private function to call the authorization API and get a token
        """

        # checks if token has expired.
        if self.token_timestamp + self.token_validity_time < time.time() + 20:

            # sets token payload
            token_payload = {
                "username": f"{self.username}",
                "password": f"{self.password}"}

            # query authorization API
            response = requests.post(self.authorization_url, json=token_payload)

            # if status code 200, API call was successful.
            if response.status_code == 200:

                # extracts token string, sets it on the authorization header variable and updates timestamp
                token_request = response.json()["jwt"]
                self.authorization_header = {"Authorization": "Bearer " + token_request}
                self.token_timestamp = time.time()

            # aborts execution if API responds with unauthorized
            elif response.status_code == 401:
                print("The given combination of username and password does not have access")
                raise SystemExit

            # to catch other errors
            else:
                self.__handle_unexpected_errors(response, "Authorization token")

    def get_master_data(self, with_return=True):
        """
        A function to fetch master-/metadata for the API.
        Gives all the authorized combinations of input variables to other APIs.

        :param with_return(bool): specify if the result dict should be returned from the function
        :return self.master_data(dict): dictionary with all master data for API
        """

        # calls authorization func
        self.__get_authorization_token()
        response = requests.get(self.masterdata_url, headers=self.authorization_header)

        # checks if API call was successful
        if response.status_code == 200:
            response = response.json()

            # extracts the scenario information, transforms it to df and adds it to dict
            self.master_data["scenario"] = pd.DataFrame(response['scenario'])

            # calls functions to extracts and organize the other master data categories
            self.__unpack_masterdata_groups_response(response['groups'])
            self.__unpack_masterdata_regions_response(response['regions'])

            # returns data dict if specified
            if with_return:
                return self.master_data

        # calls function to handle unexpected error
        else:
            self.__handle_unexpected_errors(response, "Master data")

    def __unpack_masterdata_groups_response(self, response):
        """
        Private function for extracting and organising the 'groups' response into a df and puts it in the master data dict
        :param response(dict): the group response from the API
        """
        df_list = []

        # iterate over all keys in response dict
        for list_object in response:

            # extract group and initiate df for specific group
            group = list_object['group']
            df = pd.DataFrame(columns=["indicator", "unit"])

            # iterate over all indicators and populate group df with indicator and unit
            for indicators in list_object['indicators']:
                df.loc[len(df.index)] = indicators["indicator"], indicators['unit']

            # add group name column to df and add to df list
            df.insert(0, 'group', group)
            df_list.append(df)

        # concat all group dfs to one and add to master data dict
        self.master_data["groups"] = pd.concat(df_list, ignore_index=True)

    def __unpack_masterdata_regions_response(self, response):
        """
        Private function for extracting and organizing the regions response into editions and countries
        :param response(dict): API regions response
        """
        region_df_list = []
        countries_df_list = []

        # iterate over keys in response dict
        for list_object in response:

            # extract region name
            region = list_object['region']

            # create a list of duplicated region names equal to num of editions for region and zip together
            region_list_to_df = zip([region] * len(list_object['edition']), list_object['edition'])

            # create region df with zipped lists and append to list
            region_df = pd.DataFrame(region_list_to_df, columns=['region', 'edition'])
            region_df_list.append(region_df)

            zones_list = []
            # extract countries in region
            region_countries = list_object['countries']

            # iterate over countries
            for country in region_countries:
                # extract country name
                country_name = country['country']

                # create list of duplicated country names equal to num of zones in country and zip together
                zones_list_to_df = zip([country_name] * len(country['zone']), country['zone'])

                # create country df and add to list
                country_df = pd.DataFrame(zones_list_to_df, columns=["country", 'zone'])
                zones_list.append(country_df)

            # concat country dfs in region together, add region name and append to countries list
            countries_df = pd.concat(zones_list)
            countries_df.insert(0, 'region', region)
            countries_df_list.append(countries_df)

        # concat all regions together and add to master data dict
        self.master_data['editions'] = pd.concat(region_df_list, ignore_index=True)
        self.master_data['countries'] = pd.concat(countries_df_list, ignore_index=True)

    def __get_newest_edition(self, region):
        """
        Private func to fetch the newest edition name for a given region
        :param region(str): name of region
        :return(str): the latest edition of given region
        """

        # if not master data is already fetched, master data API is called
        if not bool(self.master_data):
            self.get_master_data(with_return=False)

        # checks if given region is in regions df from master data API
        if region in list(self.master_data['editions']['region']):
            # create subset of editions df for given region
            region_editions = self.master_data['editions'].loc[self.master_data['editions']['region'] == region].copy()

            # add new data column with edition name transformed to datetime object. Sorts df based on this column
            region_editions["Date"] = list(map(self.__transfrom_to_date, list(region_editions['edition'])))
            region_editions = region_editions.sort_values(by="Date", ascending=False)

            # extract and return the first edition in the sorted df
            return region_editions["edition"].iloc[0]

        else:
            print("Given region not in regions overview")
            print("Please make sure given region is in region overview from master data API")
            raise SystemExit

    def __transfrom_to_date(self, date):
        """
        Function to transform string into date.
        If string format doesn't match expected date format, an artificial low date is returned
        :param date(str): string to be transformed to datetime object
        :return date(datetime): a datetime object of the input string
        """
        try:
            return datetime.strptime(date, '%B %Y')
        except:
            return datetime.strptime("January 1970", '%B %Y')

    def get_hourly_data(self, json):
        """
        Func to call hourly data API
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the hourly data returned from the API
        """

        # calls authorization token func
        self.__get_authorization_token()

        # specify required fields in input, and calls func to validate required fields are present and have values
        required_fields = ["scenario", "region", "country", "zone"]
        self.__validate_json(json, required_fields)

        # if edition value is missing, func to find the newest edition for given region is called
        if 'edition' not in json.keys() or not json['edition']:
            json['edition'] = self.__get_newest_edition(json['region'])

        # calls hourly data API
        response = requests.post(self.hourlyData_url, headers=self.authorization_header, json=json)

        # if API call is successful, calls func to extract data and returns results df if df is not empty
        if response.status_code == 200:
            df = self.__extract_from_response(response, "data")
            if not df.empty:
                return df
            else:
                print("API returned no data")
                print("Make sure json file have values aligning with the information in master data")
                raise SystemExit

        # if error, func to handle unexpected errors is called
        else:
            self.__handle_unexpected_errors(response, "Hourly data")

    def get_annual_data(self, json):
        """
        Func to call annual data API
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the annual data returned from the API
        """

        # calls authorization token func
        self.__get_authorization_token()

        # specify required fields in input, and calls func to validate required fields are present and have values
        required_fields = ["scenario", "region", "group"]
        self.__validate_json(json, required_fields)

        # if edition value is missing, func to find the newest edition for given region is called
        if 'edition' not in json.keys() or not json['edition']:
            json['edition'] = self.__get_newest_edition(json['region'])

        # calls annual data API
        response = requests.post(self.annualData_url, headers=self.authorization_header, json=json)

        # if API call is successful, calls func to extract data and returns results df
        if response.status_code == 200:
            df = self.__extract_from_response(response, "data")
            if not df.empty:
                return df
            else:
                print("API returned no data")
                print("Make sure json file have values aligning with the information in master data")
                raise SystemExit
        else:
            self.__handle_unexpected_errors(response, "Annual data")

    def __extract_from_response(self, response, key):
        """
        Func to extract data from response object
        :param response(response object): API response object
        :param key(str): name of dict key where API data is located
        :return df(df): a df with the extracted data
        """

        # wraps it in a try except in case API returns something unexpected
        try:
            return pd.json_normalize(response.json()[0][key])
        except:
            print("API returned no data")
            print("Make sure json file have values aligning with the information in master data and try again")
            raise SystemExit

    def __handle_unexpected_errors(self, response, API_type):
        """
        Func to give user standardized feedback when API returns something unexpected
        :param response(response object): API response
        :param API_type(str): name of API called
        """

        # prints out feedback to user and aborts the program
        print(f"An unexpected error happened when fetching {API_type} from API")
        print(f"API response code: {response.status_code}")
        print(f"API response content: {response.json()}")
        raise SystemExit

    def __validate_json(self, json, required_fields):
        """
        Func to do lightweight validation of user input to API calls
        :param json(dict): user input to API call
        :param required_fields(list): the required fields in the API input
        """

        # generate a list of required fields missing from the user json input
        missing_fields = [field for field in required_fields if field not in json.keys()]

        # if any fields missing, these are printed out to user and program is aborted
        if not len(missing_fields) == 0:
            print("Aborting query")
            print(f"Missing required field(s) in json: {', '.join(missing_fields)}")
            raise SystemExit
        else:
            # generate a list of required fields where value is None or ""
            missing_field_values = [field for field in required_fields if json[field] is None or json[field] == ""]

            # if any None or "" values for required fields, these are printed to user and program is aborted
            if not len(missing_field_values) == 0:
                print("Aborting query")
                print(f"Missing values for required field(s) in json: {', '.join(missing_field_values)}")
                raise SystemExit


if __name__ == "__main__":

    # specify username and password
    username = "yourEmail@company.com"
    password = "yourPassword"

    # specify an existing folder for the output
    output_folder = "Thema_API_output/"

    # initiates an API object of type Thema_API
    API_object = Thema_API(username=username, password=password)

    # example of fetching master data
    master_data = API_object.get_master_data()

    # iterating over the different master data dfs
    for key, df in master_data.items():
        # prints out the master data dfs
        print(f"\n{key}")
        print(df)

        # saves the master data dfs to excel
        df.to_excel(f"{output_folder}{key}.xlsx")

    # Hourly Data input example
    # all values are mandatory, but program will set edition to the newest if not user specified
    json = {
        "scenario": "Base",
        "region": "Nordics",
        "edition": None,
        "country": "Norway",
        "zone": "NO2"
    }

    # example of calling the hourly data API and writing the results to excel
    hourly_data = API_object.get_hourly_data(json)
    hourly_data.to_excel(f"{output_folder}Hourly_data.xlsx")

    # Annual Data input example
    # indicator, country and zone are optional. Not specifying one or more of them will request all possible variations
    json = {
        "scenario": "Base",
        "group": "Real prices",
        "indicator": "Gas price",
        "region": "Nordics",
        "edition": "September 2022",
        "country": "Norway",
        "zone": "NO2"
    }

    # example of calling the annual data API and writing the results to excel
    annual_data = API_object.get_annual_data(json)
    annual_data.to_excel(f"{output_folder}Annual_data.xlsx")
