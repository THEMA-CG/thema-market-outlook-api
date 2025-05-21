import requests
import pandas as pd
import time
from datetime import datetime
import itertools
import os

class Thema_API:
    """
    Parent class for all thema API classes
    """

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

        # initiates token_timestamp and a token expiration time
        self.token_timestamp = 0
        self.token_validity_time = 600

        # initiate master_data dict
        self.master_data = {}

        # initiate flag for combination query and the rejected combinations dict
        self.combination_query = False
        self.rejected_combinations = {"Hourly": [], "Annual": [], "Monthly": [], "GO": [], "PPA": []}

    def _get_authorization_token(self):
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

    def _transfrom_to_date(self, date):
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
        
    def _create_query_combinations(self, json, hourly=False):
        """
        Func responsible for making all possible value combinations based on json input
        :param json(dict): input json where one or more parameters have multiple values
        :param yearly(bool): flag indicating if this is combinations for the yearly query
        :return: list of jsons with all possible json combinations
        """

        # sets combinations flag. Triggers different error handling
        self.combination_query = True

        # extract keys and values from input json
        keys = json.keys()
        values_list = json.values()

        # transform all not iterable values to iterables
        values_list = [{x} if not type(x)==set else x for x in values_list]

        # construct all possible combinations
        values_combinations = list(itertools.product(*values_list))

        # zips combinations and json keys back to list of json combinations
        jsons = list(map(lambda x: dict(zip(keys, x)), values_combinations))

        return jsons
            
    def _extract_from_response(self, response, key):
        #Checks if respons.json() is list of dictionaries or dictionary, normalize accordingly then return a dataframe
        try:
            if isinstance(response.json(), list):
                return pd.json_normalize(response.json()[0][key])
            elif isinstance(response.json(), dict):
                return pd.json_normalize(response.json()[key])
            else:
                raise ValueError("Unexpected JSON structure")
        except:
            # different error handling if combinations query
            if self.combination_query:
                return pd.DataFrame()
            else:
                print("API returned no data")
                print("Make sure json file have values aligning with the information in master data and try again")
                raise SystemExit
            
    def _handle_unexpected_errors(self, response, API_type):
        """
        Func to give user standardized feedback when API returns something unexpected
        :param response(response object): API response
        :param API_type(str): name of API called
        """

        # prints out feedback to user and aborts the program
        print(f"An unexpected error happened when fetching {API_type} from API")
        print(f"API response code: {response.status_code}")
        try:
            print(f"API response content: {response.json()}")
        except Exception:
            print(f"API response content (non-JSON): {response.text}")
        raise SystemExit

class Thema_data_API(Thema_API):

    def __init__(self, username, password):

        # initiate parent class and set API URLs
        Thema_API.__init__(self, username, password)
        self.masterdata_url = f"{self.api_root_url}masterdata"
        self.annualData_url = f"{self.api_root_url}annualData"
        self.hourlyData_url = f"{self.api_root_url}hourlyData"
        self.monthlyData_url = f"{self.api_root_url}monthlyData"
        self.goData_url = f"{self.api_root_url}go/data"
        self.PPAData_url = F"{self.api_root_url}ppa/data"

    def get_master_data(self, with_return=True):
        """
        A function to fetch master-/metadata for the API.
        Gives all the authorized combinations of input variables to other APIs.

        :param with_return(bool): specify if the result dict should be returned from the function
        :return self.master_data(dict): dictionary with all master data for API
        """

        # calls authorization func
        self._get_authorization_token()
        response = requests.get(self.masterdata_url, headers=self.authorization_header)

        # checks if API call was successful
        if response.status_code == 200:
            response = response.json()

            # extracts the scenario information, transforms it to df and adds it to dict
            self.master_data["scenario"] = pd.DataFrame(response['scenario'])
            self.master_data["scenario"].columns = ["scenario"]

            # calls functions to extracts and organize the other master data categories
            self.__unpack_masterdata_groups_response(response['groups'])
            self.__unpack_masterdata_regions_response(response['regions'])

            # returns data dict if specified
            if with_return:
                return self.master_data

        # calls function to handle unexpected error
        else:
            self._handle_unexpected_errors(response, "Master data")

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
            region_list_to_df = zip([region]*len(list_object['edition']), list_object['edition'])

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
                zones_list_to_df = zip([country_name]*len(country['zone']), country['zone'])

                # create country df and add to list
                country_df = pd.DataFrame(zones_list_to_df, columns=["country", 'zone'])
                zones_list.append(country_df)

            # concat country dfs in region together, add region name and append to countries list
            countries_df = pd.concat(zones_list, ignore_index=True)
            countries_df.insert(0, 'region', region)
            countries_df_list.append(countries_df)

        # concat all regions together and add to master data dict
        self.master_data['editions'] = pd.concat(region_df_list, ignore_index=True)
        self.master_data['countries'] = pd.concat(countries_df_list, ignore_index=True)

    def __get_newest_edition(self, region=None):
        """
        Private func to fetch the newest edition name for a given region
        :param region(str): name of region
        :return(str): the latest edition of given region
        """
        
        # if not master data is already fetched, master data API is called
        if region is not None:
            #If region is inside the function call
            if not bool(self.master_data):
                self.get_master_data(with_return=False)

            # checks if given region is in regions df from master data API
            if region in list(self.master_data['editions']['region']):
                # create subset of editions df for given region
                region_editions = self.master_data['editions'].loc[self.master_data['editions']['region']==region].copy()

                # add new data column with edition name transformed to datetime object. Sorts df based on this column
                region_editions["Date"] = list(map(self._transfrom_to_date, list(region_editions['edition'])))
                region_editions = region_editions.sort_values(by="Date", ascending=False)

                # extract and return the first edition in the sorted df
                return region_editions["edition"].iloc[0]

            else:
                print("Given region not in regions overview")
                print("Please make sure given region is in region overview from master data API")
                raise SystemExit
        
        else:
            #if region is not defined, then find newest edition in all regions
            if not bool(self.master_data):
                self.get_master_data(with_return=False)

            editions = self.master_data['editions'].copy()

            editions["Date"] = list(map(self._transfrom_to_date, list(editions['edition'])))

            editions = editions.sort_values(by="Date", ascending=False)

            # extract and return the first edition in the sorted df
            return editions["edition"].iloc[0]

    def get_hourly_data(self, json):
        """
        Public func to get hourly data.
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the hourly data returned from the API
        """

        # calls authorization token func
        self._get_authorization_token()

        # if not master data is already fetched, master data API is called
        if not bool(self.master_data):
            self.get_master_data(with_return=False)        

        if 'edition' not in json.keys() or not json['edition']:
            json['edition'] = self.__get_newest_edition(json["region"])

        if "scenario" not in json.keys() or not json["scenario"]:
            json["scenario"] = set(self.master_data["scenario"]["scenario"])

        if "zone" not in json.keys() or not json["zone"]:
            json["zone"] = set(self.master_data["countries"]["zone"])

        if "country" not in json.keys() or not json["country"]:
            json["country"] = set(self.master_data["countries"]["country"])

        if "region" not in json.keys() or not json["region"]:
            json["region"] = set(self.master_data["countries"]["region"])

        # checks if any json parameters have multiple values
        if any(list(map(lambda x: type(x) == set, json.values()))):

            # call func to create list of json combinations
            jsons = self._create_query_combinations(json, True)

            # calls func to sort out the most obvious invalid combinations
            jsons = self.__sort_out_invalid_combinations(jsons, hourly=True)

            try:
                # call func to query API with all json combinations and concat to one df
                df = pd.concat(list(map(self.__get_hourly_data, jsons)), ignore_index=True)
            except:
                print("No valid combinations for Hourly data")
                raise SystemExit

        else:
            df = self.__get_hourly_data(json)

        return df

    def __get_hourly_data(self, json):
        """
        Private func to call hourly data API
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the hourly data returned from the API
        """

        # specify required fields in input, and calls func to validate required fields are present and have values
        required_fields = ["scenario", "region", "country", "zone"]
        self.__validate_json(json, required_fields)

        # calls hourly data API
        response = requests.post(self.hourlyData_url, headers=self.authorization_header, json=json)

        # if API call is successful, calls func to extract data and returns results df if df is not empty
        if response.status_code == 200:
            df = self._extract_from_response(response, "data")
            if not df.empty:

                # add json key as df header and populate with value
                for key, value in json.items():
                    if value:
                        df[key] = value
                return df

            # append combination to dict if not valid
            elif self.combination_query:
                self.rejected_combinations["Hourly"].append(json)

            else:
                print("API returned no data")
                print("Make sure json file have values aligning with the information in master data")
                raise SystemExit

        # if error, func to handle unexpected errors is called
        else:
            self._handle_unexpected_errors(response, "Hourly data")

    def get_monthly_data(self, json):
        """
        Public func to get monthly data.
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the hourly data returned from the API
        """

        # calls authorization token func
        self._get_authorization_token()

        # if not master data is already fetched, master data API is called
        if not bool(self.master_data):
            self.get_master_data(with_return=False)

        if 'edition' not in json.keys() or not json['edition']:
            json['edition'] = self.__get_newest_edition(json["region"])

        if "scenario" not in json.keys() or not json["scenario"]:
            json["scenario"] = set(self.master_data["scenario"]["scenario"])

        if "zone" not in json.keys() or not json["zone"]:
            json["zone"] = set(self.master_data["countries"]["zone"])

        if "country" not in json.keys() or not json["country"]:
            json["country"] = set(self.master_data["countries"]["country"])

        if "region" not in json.keys() or not json["region"]:
            json["region"] = set(self.master_data["countries"]["region"])

        if "indicator" not in json.keys() or not json["indicator"]:
            json["indicator"] = set(self.master_data["groups"]["indicator"])

        if "group" not in json.keys() or not json["group"]:
            json["group"] = set(self.master_data["groups"]["group"])

        # checks if any json parameters have multiple values
        if any(list(map(lambda x: type(x) == set, json.values()))):

            # call func to create list of json combinations
            jsons = self._create_query_combinations(json)

            # calls func to sort out the most obvious invalid combinations
            jsons = self.__sort_out_invalid_combinations(jsons, hourly=False)

            try:
                # call func to query API with all json combinations and concat to one df
                df = pd.concat(list(map(self.__get_monthly_data, jsons)), ignore_index=True)
            except:
                print("No valid combinations for Monthly data")
                raise SystemExit
        else:
            df = self.__get_monthly_data(json)

        return df   

    def __get_monthly_data(self, json):
        """
        Private func to call monthly data API
        :param json(dict): the user specified dict/json with input parameters to the API
        :return response: the response object from the API
        """        

        # specify required fields in input, and calls func to validate required fields are present and have values
        required_fields = ["scenario", "region", "group"]
        self.__validate_json(json, required_fields)
  
        # calls annual data API
        response = requests.post(self.monthlyData_url, headers=self.authorization_header, json=json)
        
        # if API call is successful, calls func to extract data and returns results df if df is not empty
        if response.status_code == 200:
            df = self._extract_from_response(response, "data")
            if not df.empty:

                # add json key as df header and populate with value
                for key, value in json.items():
                    if value:
                        df[key] = value
                return df

            # append combination to dict if not valid
            elif self.combination_query:
                self.rejected_combinations["Monthly"].append(json)

            else:
                print("API returned no data")
                print("Make sure json file have values aligning with the information in master data")
                raise SystemExit

        # if error, func to handle unexpected errors is called
        else:
            self._handle_unexpected_errors(response, "Monthly data")

    def get_GO_data(self, json):
        """
        Public func to get GO data.
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the GO data returned from the API
        """

        # calls authorization token func and master data func
        self._get_authorization_token() 

        # if not master data is already fetched, master data API is called
        if not bool(self.master_data):
            self.get_master_data(with_return=False)

        # if edition value is missing, func to find the newest edition for given region is called
        if 'edition' not in json.keys() or not json['edition']:
            json['edition'] = self.__get_newest_edition()

        if "scenario" not in json.keys() or not json["scenario"]:
            json["scenario"] = set(self.master_data["scenarios"]["scenarios"])


        if "zone" not in json.keys() or not json["zone"]:
            json["zone"] = set(self.master_data["countries"]["zone"])

        if "group" not in json.keys() or not json["group"]:
            json["group"] = set(self.master_data["groups"]["groups"])

        if "indicator" not in json.keys() or not json["indicator"]:
            json["indicator"] = set(self.master_data["groups"]["indicator"])

        # checks if any json parameters have multiple values
        if any(list(map(lambda x: type(x) == set, json.values()))):

            # call func to create list of json combinations
            jsons = self._create_query_combinations(json)

            # calls func to sort out the most obvious invalid combinations
            jsons = self.__sort_out_invalid_combinations(jsons, hourly=False)

            try:
                # call func to query API with all json combinations and concat to one df
                df = pd.concat(list(map(self.__get_GO_data, jsons)), ignore_index=True)
            except:
                print("No valid combinations for GO data")
                raise SystemExit
        else:
            df = self.__get_GO_data(json)

        return df
    
    def __get_GO_data(self, json):
        """
        Private func to call GO data API
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the GO data returned from the API
        """    

        required_fields = ["scenario", "group"]
        self.__validate_json(json, required_fields)   

        response = requests.post(self.goData_url, headers=self.authorization_header, json=json)

        # if API call is successful, calls func to extract data and returns results df
        if response.status_code == 200:
            df = self._extract_from_response(response, 'data')
            if not df.empty:

                # add json key as df header and populate with value
                for key, value in json.items():
                    if value:
                        df[key] = value
                return df

            # append combination to dict if not valid
            elif self.combination_query:
                self.rejected_combinations["GO"].append(json)

            else:
                print("API returned no data")
                print("Make sure json file have values aligning with the information in master data")
                raise SystemExit
        else:
            self._handle_unexpected_errors(response, "GO data")

    def get_annual_data(self, json):
        """
        Public func to get annual data.
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the hourly data returned from the API
        """

        # calls authorization token func
        self._get_authorization_token()

        # if not master data is already fetched, master data API is called
        if not bool(self.master_data):
            self.get_master_data(with_return=False)

        if 'edition' not in json.keys() or not json['edition']:
            json['edition'] = self.__get_newest_edition(json["region"])

        if "scenario" not in json.keys() or not json["scenario"]:
            json["scenario"] = set(self.master_data["scenario"]["scenario"])

        if "zone" not in json.keys() or not json["zone"]:
            json["zone"] = set(self.master_data["countries"]["zone"])

        if "country" not in json.keys() or not json["country"]:
            json["country"] = set(self.master_data["countries"]["country"])

        if "region" not in json.keys() or not json["region"]:
            json["region"] = set(self.master_data["countries"]["region"])

        if "indicator" not in json.keys() or not json["indicator"]:
            json["indicator"] = set(self.master_data["groups"]["indicator"])

        if "group" not in json.keys() or not json["group"]:
            json["group"] = set(self.master_data["groups"]["groups"])

        # checks if any json parameters have multiple values
        if any(list(map(lambda x: type(x) == set, json.values()))):

            # call func to create list of json combinations
            jsons = self._create_query_combinations(json)

            # calls func to sort out the most obvious invalid combinations
            jsons = self.__sort_out_invalid_combinations(jsons, hourly=False)

            try:
                # call func to query API with all json combinations and concat to one df
                df = pd.concat(list(map(self.__get_annual_data, jsons)), ignore_index=True)
            except:
                print("No valid combinations for Annual data")
                raise SystemExit
        else:
            df = self.__get_annual_data(json)

        return df

    def __get_annual_data(self, json):
        """
        Private func to call annual data API
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the annual data returned from the API
        """        

        # specify required fields in input, and calls func to validate required fields are present and have values
        required_fields = ["scenario", "region", "group"]
        self.__validate_json(json, required_fields)

        # calls annual data API
        response = requests.post(self.annualData_url, headers=self.authorization_header, json=json)

        # if API call is successful, calls func to extract data and returns results df
        if response.status_code == 200:
            df = self._extract_from_response(response, "data")
            if not df.empty:

                # add json key as df header and populate with value
                for key, value in json.items():
                    if value:
                        df[key] = value
                return df

            # append combination to dict if not valid
            elif self.combination_query:
                self.rejected_combinations["Annual"].append(json)

            else:
                print("API returned no data")
                print("Make sure json file have values aligning with the information in master data")
                raise SystemExit
        else:
            self._handle_unexpected_errors(response, "Annual data")

    def get_PPA_data(self, json):
        """
        Public func to get PPA data.
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the hourly data returned from the API
        """

        # calls authorization token func
        self._get_authorization_token()

        # if not master data is already fetched, master data API is called
        if not bool(self.master_data):
            self.get_master_data(with_return=False)

        if 'edition' not in json.keys() or not json['edition']:
            json['edition'] = self.__get_newest_edition()

        if "scenario" not in json.keys() or not json["scenario"]:
            json["scenario"] = set(self.master_data["scenario"]["scenario"])

        if "zone" not in json.keys() or not json["zone"]:
            json["zone"] = set(self.master_data["countries"]["zone"])

        if "group" not in json.keys() or not json["group"]:
            json["group"] = set(self.master_data["groups"]["group"])
        
        # checks if any json parameters have multiple values
        if any(list(map(lambda x: type(x) == set, json.values()))):

            # call func to create list of json combinations
            jsons = self._create_query_combinations(json)

            # calls func to sort out the most obvious invalid combinations
            jsons = self.__sort_out_invalid_combinations(jsons, hourly=False)

            try:
                # call func to query API with all json combinations and concat to one df
                df = pd.concat(list(map(self.__get_PPA_data, jsons)), ignore_index=True)
            except:
                print("No valid combinations for PPA data")
                raise SystemExit
        else:
            df = self.__get_PPA_data(json)

        return df

    def __get_PPA_data(self, json):
        """
        Private func to call PPA data API
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the PPA data returned from the API
        """        
        required_fields = ["scenario", "group"]
        self.__validate_json(json, required_fields)

        # calls PPA data API
        response = requests.post(self.PPAData_url, headers=self.authorization_header, json=json)

        # if API call is successful, calls func to extract data and returns results df
        if response.status_code == 200:
            df = self._extract_from_response(response, "data")
            if not df.empty:

                # add json key as df header and populate with value
                for key, value in json.items():
                    if value:
                        df[key] = value
                return df

            # append combination to dict if not valid
            elif self.combination_query:
                self.rejected_combinations["PPA"].append(json)

            else:
                print("API returned no data")
                print("Make sure json file have values aligning with the information in master data")
                raise SystemExit
        else:
            self._handle_unexpected_errors(response, "PPA data")

    def __sort_out_invalid_combinations(self, jsons, hourly):
        """
        func to filter away invalid combinations
        :param jsons: list of jsons with all possible json combinations
        :param hourly(bool): flag indicating if this is combinations for the hourly query
        :return: filtered jsons list
        """
        boolean_filter = []

        if hourly:
            # iterates over all jsons and add boolean value indicating if combinations is valid or not to boolean filter
            for json in jsons:
                if json["zone"] in (list((self.master_data["countries"].loc[(self.master_data["countries"]["region"] == json["region"]
                    ) & (self.master_data["countries"]["country"] == json["country"]), "zone"]))):
                    boolean_filter.append(True)
                else:
                    boolean_filter.append(False)

            # removes non-valid combinations
            jsons = [jsons[i] for i in range(len(jsons)) if boolean_filter[i]]

        else:
            # iterates over all jsons and add boolean value indicating if combinations is valid or not to boolean filter
            for json in jsons:
                if json["group"] in list(self.master_data["groups"].loc[self.master_data["groups"]["indicator"]==json["indicator"], "group"]) and \
                        json["zone"] in list(self.master_data["countries"].loc[(self.master_data["countries"]["region"] == json["region"]
                                        ) & (self.master_data["countries"]["country"] == json["country"]), "zone"]):
                    boolean_filter.append(True)
                else:
                    boolean_filter.append(False)

            # removes non-valid combinations
            jsons = [jsons[i] for i in range(len(jsons)) if boolean_filter[i]]

        return jsons

    def get_rejected_combinations(self):
        """
        Func to create and return pandas df of rejected combinations
        :return df(df): overview of all rejected combinations
        """

        # if any rejected combinations
        if self.rejected_combinations["Hourly"] or self.rejected_combinations["Annual"]:  # if any rejected combinations
            df_list = []

            # creates df per query type
            for query_type in self.rejected_combinations.keys():
                if self.rejected_combinations[query_type]:
                    df = pd.DataFrame(self.rejected_combinations[query_type])
                    df.insert(0, "Query_type",  query_type)
                    df_list.append(df)

            # concat dfs and return
            df = pd.concat(df_list, ignore_index=True)
            return df

        # if no rejected combinations, return empty df
        else:
            return pd.DataFrame

    def __validate_json(self, json, required_fields):
        """
        Func to do lightweight validation of user input to API calls
        :param json(dict): user input to API call
        :param required_fields(list): the required fields in the API input
        """

        # generate a list of required fields missing from the user json input
        missing_fields = [field for field in required_fields if field not in json.keys()]

        # if any fields missing, these are printed out to user and program is aborted
        if not len(missing_fields)==0:
            print("Aborting query")
            print(f"Missing required field(s) in json: {', '.join(missing_fields)}")
            raise SystemExit
        else:
            # generate a list of required fields where value is None or ""
            missing_field_values = [field for field in required_fields if json[field] is None or json[field]==""]

            # if any None or "" values for required fields, these are printed to user and program is aborted
            if not len(missing_field_values) == 0:
                print("Aborting query")
                print(f"Missing values for required field(s) in json: {', '.join(missing_field_values)}")
                raise SystemExit


class Thema_technology_data_API(Thema_API):

    def __init__(self, username, password):
        # initiate parent class and API URLs
        Thema_API.__init__(self, username, password)        
        self.masterdata_url = f"{self.api_root_url}technology/masterdata"
        self.annualData_url = f"{self.api_root_url}technology/annualData"
        

    def get_master_data(self, with_return=True):
        """
    A function to fetch master-/metadata for the API.
    Gives all the authorized combinations of input variables to other APIs.

    :param with_return(bool): specify if the result dict should be returned from the function
    :return self.master_data(dict): dictionary with all master data for API
    """

        # calls authorization func
        self._get_authorization_token()
        response = requests.get(self.masterdata_url, headers=self.authorization_header)        

        # checks if API call was successful
        if response.status_code == 200:
            response = response.json()            
            
            for key, value in response.items():

                # extracts the scenario information, transforms it to df and adds it to dict
                df = pd.DataFrame(value)
                if len(df.columns) == 1:
                    df.columns = [key]

                self.master_data[key] = df.reset_index(drop=True)         

            # returns data dict if specified
            if with_return:
                return self.master_data

        # calls function to handle unexpected error
        else:
            self._handle_unexpected_errors(response, "Master data")


    def get_annual_data(self, json):
        """
        Public func to get annual data.
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the annual data returned from the API
        """

        # calls authorization token func and master data func
        self._get_authorization_token()
        self.get_master_data()

        # if edition value is missing, func to find the newest edition for given region is called
        if 'edition' not in json.keys() or not json['edition']:
            json['edition'] = self.__get_newest_edition()

        if "scenario" not in json.keys() or not json["scenario"]:
            json["scenario"] = set(self.master_data["scenarios"]["scenarios"])

        if "country" not in json.keys() or not json["country"]:
            json["country"] = set(self.master_data["countries"]["countries"])

        if "indicator" not in json.keys() or not json["indicator"]:
            json["indicator"] = set(self.master_data["indicators"]["indicator"])

        if "technology" not in json.keys() or not json["technology"]:
            json["technology"] = set(self.master_data["technologies"]["technology"])

        if "category" not in json.keys() or not json["category"]:
            category_list = list(self.master_data["technologies"]["categories"])
            categories = [category for sublist in category_list for category in sublist]
            json["category"] = set(categories)

        # checks if any json parameters have multiple values
        if any(list(map(lambda x: type(x) == set, json.values()))):

            # call func to create list of json combinations
            jsons = self._create_query_combinations(json)

            # removes invalid combinations of technology and category
            jsons = self.__remove_invalid_combinations(jsons)

            try:
                # call func to query API with all json combinations and concat to one df
                df = pd.concat(list(map(self.__get_annual_data, jsons)), ignore_index=True)
            except:
                print("No valid combinations for Annual data")
                raise SystemExit
        else:
            df = self.__get_annual_data(json)

        return df
    
    def __remove_invalid_combinations(self, json_list):
        """
        Private func to filter out invalid combinations of technology and category
        :param json_list(list): list of json combinations
        :return filtered_list(list): filtered list of json combinations
        """
        
        # extract technology masterdata and make hashable
        technology_masterdata = self.master_data["technologies"].copy().set_index('technology')["categories"]
        
        filtered_list = []
        # iterate over all combinations and check technology and category consistency
        for json in json_list:
            tech = json["technology"]
            category = json["category"]
            if category in technology_masterdata[tech]:
                filtered_list.append(json)

        return filtered_list

    

    def __get_annual_data(self, json=None):
        """
        Private func to call annual technology data API
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the annual data returned from the API
        """

        # calls annual data API
        response = requests.post(self.annualData_url, headers=self.authorization_header, json=json)

        # if API call is successful, calls func to extract data and returns results df
        if response.status_code == 200:
            df = self._extract_from_response(response, "data")
            if not df.empty:

                # add json key as df header and populate with value
                for key, value in json.items():
                    if value:
                        df[key] = value
                return df

            # append combination to dict if not valid
            elif self.combination_query:
                self.rejected_combinations["Annual"].append(json)

            else:
                print("API returned no data")
                print("Make sure json file have values aligning with the information in master data")
                raise SystemExit
        else:
            self._handle_unexpected_errors(response, "Annual data")


    def __get_newest_edition(self):
        """
        Private func to fetch the newest edition name
        :return(str): the latest edition
        """
        # if not master data is already fetched, master data API is called
        if not bool(self.master_data):
            self.get_master_data(with_return=False)

        editions = self.master_data['editions'].copy()

        # add new data column with edition name transformed to datetime object. Sorts df based on this column
        editions["Date"] = list(map(self._transfrom_to_date, list(editions['editions'])))
        editions = editions.sort_values(by="Date", ascending=False)

        # extract and return the first edition in the sorted df
        return editions["editions"].iloc[0]

    
class Thema_hydrogen_data_API(Thema_API):

    def __init__(self, username, password):
        # initiate parent class and API URLs
        Thema_API.__init__(self, username, password)        
        self.masterdata_url = f"{self.api_root_url}hydrogen/masterdata"
        self.annualData_url = f"{self.api_root_url}hydrogen/annualData"
        

    def get_master_data(self, with_return=True):
        """
    A function to fetch master-/metadata for the API.
    Gives all the authorized combinations of input variables to other APIs.

    :param with_return(bool): specify if the result dict should be returned from the function
    :return self.master_data(dict): dictionary with all master data for API
    """

        # calls authorization func
        self._get_authorization_token()
        response = requests.get(self.masterdata_url, headers=self.authorization_header)        

        # checks if API call was successful
        if response.status_code == 200:
            response = response.json()            
            
            for key, value in response.items():

                # extracts the scenario information, transforms it to df and adds it to dict
                if key=="groups":

                    groups_list = []
                    for group, indicators in value.items():
                        df = pd.DataFrame.from_dict(indicators)
                        df.insert(0, "group", group)
                        groups_list.append(df)
                    
                    df = pd.concat(groups_list, ignore_index=True)

                else:
                    df = pd.DataFrame(value)

                if len(df.columns) == 1:
                    df.columns = [key]

                self.master_data[key] = df.reset_index(drop=True)         

            # returns data dict if specified
            if with_return:
                return self.master_data

        # calls function to handle unexpected error
        else:
            self._handle_unexpected_errors(response, "Master data")


    def get_annual_data(self, json):
        """
        Public func to get annual data.
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the hourly data returned from the API
        """

        # calls authorization token func and master data func
        self._get_authorization_token()
        self.get_master_data()

        # if edition value is missing, func to find the newest edition for given region is called
        if 'edition' not in json.keys() or not json['edition']:
            json['edition'] = self.__get_newest_edition()

        if "scenario" not in json.keys() or not json["scenario"]:
            json["scenario"] = set(self.master_data["scenarios"]["scenarios"])

        if "country" not in json.keys() or not json["country"]:
            json["country"] = set(self.master_data["countries"]["countries"])

        if "group" not in json.keys() or not json["group"]:
            json["group"] = set(self.master_data["groups"]["group"])

        if "indicator" not in json.keys() or not json["indicator"]:
            json["indicator"] = set(self.master_data["groups"]["indicator"])
        

        # checks if any json parameters have multiple values
        if any(list(map(lambda x: type(x) == set, json.values()))):

            # call func to create list of json combinations
            jsons = self._create_query_combinations(json)

            # removes invalid combinations of group and indicator
            jsons = self.__remove_invalid_combinations(jsons)

            try:
                # call func to query API with all json combinations and concat to one df
                df = pd.concat(list(map(self.__get_annual_data, jsons)), ignore_index=True)
            except:
                print("No valid combinations for Annual data")
                raise SystemExit
        else:
            df = self.__get_annual_data(json)

        return df
    
    def __remove_invalid_combinations(self, json_list):
        """
        Private func to filter out invalid combinations of group and indicator
        :param json_list(list): list of json combinations
        :return filtered_list(list): filtered list of json combinations
        """
        
        # extract technology masterdata and make hashable
        groups_masterdata = self.master_data["groups"].copy()
        groups_masterdata = set(zip(list(groups_masterdata["group"]), list(groups_masterdata["indicator"])))

        filtered_list = []
        # iterate over all combinations and check group and indicator consistency
        for json in json_list:
             group = json["group"]
             indicator = json["indicator"]
             if (group, indicator) in groups_masterdata:
                 filtered_list.append(json)

        return filtered_list
        

    def __get_annual_data(self, json=None):
        """
        Private func to call annual technology data API
        :param json(dict): the user specified dict/json with input parameters to the API
        :return df[df): the annual data returned from the API
        """

        # calls annual data API
        response = requests.post(self.annualData_url, headers=self.authorization_header, json=json)

        # if API call is successful, calls func to extract data and returns results df
        if response.status_code == 200:
            df = self._extract_from_response(response, "data")
            if not df.empty:

                # add json key as df header and populate with value
                for key, value in json.items():
                    if value:
                        df[key] = value
                return df

            # append combination to dict if not valid
            elif self.combination_query:
                self.rejected_combinations["Annual"].append(json)

            else:
                print("API returned no data")
                print("Make sure json file have values aligning with the information in master data")
                raise SystemExit
        else:
            self._handle_unexpected_errors(response, "Annual data")


    def __get_newest_edition(self):
        """
        Private func to fetch the newest edition name
        :return(str): the latest edition
        """
        if not bool(self.master_data):
            self.get_master_data(with_return=False)

        editions = self.master_data['editions'].copy()

        # add new data column with edition name transformed to datetime object. Sorts df based on this column
        editions["Date"] = list(map(self._transfrom_to_date, list(editions['editions'])))
        editions = editions.sort_values(by="Date", ascending=False)

        # extract and return the first edition in the sorted df
        return editions["editions"].iloc[0]

    def API_test(self, json):
        # calls authorization token func
        self._get_authorization_token()
        response = requests.post(self.annualData_url, headers=self.authorization_header, json=json)
        if response.status_code == 200:
            df = self._extract_from_response(response, "data")
            return df
        else:
            print(response.status_code)
    
