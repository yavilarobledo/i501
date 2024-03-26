import requests
import json
import os
from dotenv import load_dotenv
from multiprocessing import Pool
import pandas as pd
from tqdm import tqdm
import sys


try:
    assert load_dotenv()
except AssertionError:
    print(".env File not loaded")
    #Since .env file contains the environment variables, the program will not work unless these variables are provided


ACCESS_TOKEN = os.environ['ACCESS_TOKEN']

#For unauthorized user access(invalid API Token), no error is raised. A status code of 401 is indocates that an error should be raised
class UnauthorizedAccess(Exception):
    def __init__(self):
        super().__init__()

#For api rate limiting, the function does not raise an exception, however a status code of 429 is sent
class APIRateLimit(Exception):
    def __init__(self):
        super().__init__()

def getResultsBySearchTerm(search_term, results_limit=15):

    genius_api_url = f"http://api.genius.com/search?q={search_term}&" + \
                        f"access_token={ACCESS_TOKEN}&per_page={results_limit}"

    try:
        response = requests.get(genius_api_url)

        if response.status_code == 401:
            raise UnauthorizedAccess()
            
        elif response.status_code == 429:
            raise APIRateLimit()
        
    except UnauthorizedAccess:
        print("Unauthorized Access to the API, check API ACCESS TOKEN")
    except APIRateLimit:
        print("API Rate Limit Error, you have sent too many requests. Try again later")
    except requests.exceptions.ConnectionError:
        #For network error
        print("Network error, please check your internet connection")

    try:
        json_data = response.json()
    except json.JSONDecodeError:
        #For response error
        print("JSON Response error, received an ambiguous format in response")

    return json_data['response']['hits']


def geniusToDf(search_term, results_limit=15, savefile=False):
    json_data = getResultsBySearchTerm(search_term, results_limit)

    hits = [hit['result'] for hit in json_data]
    df = pd.DataFrame(hits)

    df_stats = df['stats'].apply(pd.Series)
    df_stats.rename(columns={c:'stat_' + c for c in df_stats.columns}, inplace=True)

    df_primary = df['primary_artist'].apply(pd.Series)
    df_primary.rename(columns={c:'primary_artist_' + c 
                               for c in df_primary.columns},
                      inplace=True)

    df = pd.concat((df, df_stats, df_primary), axis=1)

    if savefile:
        df.to_csv(savepath + f'genius-{search_term}.csv', index=False)

    return df


def searchMultipleTerms(search_terms, results_limit=15, savefile=False, use_multiprocessing=False, num_multiprocess=8):
    dfs = []

    if not use_multiprocessing:
        for search_term in search_terms:
            df = geniusToDf(search_term, results_limit, savefile=False) #Since, we are saving all files once, no need to save file from each search
            dfs.append(df)
    else:
        with Pool(num_multiprocess) as p:
            dfs = p.map(geniusToDf, search_terms)

    dfs = pd.concat(dfs)

    if savefile:
        dfs.to_csv("genius-multiple-terms-search.csv", index=False)
        
    return dfs