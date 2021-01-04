import requests
import json
from datetime import datetime
from scripts.decorators import exception_catcher

@exception_catcher
def data_fetcher(url):
    """
    To fetch the data from the URL specified based on the option provided
    :url: - The main url for the required data set to be retrieved.
    """
    response = requests.get(url)
    if response.status_code == 200:
        output_formatted = json.loads(response.text)
        timer = datetime.fromtimestamp(output_formatted["last_updated"])
        ttl = output_formatted["ttl"]
        return output_formatted, timer, ttl
    else:
        print(response)
        raise Exception("ERROR: Unable to get the data from the URL - {} in the data_fetcher function".format(url))

@exception_catcher
def url_selector(url, option=None, lang=None):
    """
    Selection criteria of the URL is set using the option provided in the input and the appropriate language.
    :url: - Generic url for the script to get the options info. This is by default http://gbfs.citibikenyc.com/gbfs/gbfs.json
    :option: - Can be one of the following
        system_information
        station_information
        station_status
        free_bike_status
        system_hours
        system_calendar
        system_regions
        system_alerts
    :lang: - Appropriate language needed from options given below
        en
        es
        fr
    These are the options provided as per the http://gbfs.citibikenyc.com/gbfs/gbfs.json url
    """
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("ERROR in retrieving the data from the URL - {} in the url_selector function".format(url))
    
    if lang is None:
        return json.loads(response.text)
    elif option is None and lang is not None:
        return json.loads(response.text)["data"][lang]["feeds"]
    else:
        for item in json.loads(response.text)["data"][lang]["feeds"]:
            if item["name"] == option:
                return item["url"]

def data_driller(input, hierarchy=[]):
    if hierarchy == []:
        return input
    elif len(hierarchy) > 1:
        result = data_driller(input[hierarchy[0]], hierarchy=hierarchy[1:])
        return result
    else:
        return input[hierarchy[0]]