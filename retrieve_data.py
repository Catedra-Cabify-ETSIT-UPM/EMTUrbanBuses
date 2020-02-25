import pandas as pd
import geopandas as gpd
from csv import writer

import time
import datetime
from datetime import timedelta
from threading import Timer

class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


#Check if time is inside range
def time_in_range(start, end, x):
    """Return true if x is in the range [start, end]"""
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end

#Append list as row to a csv file
def append_list_as_row(file_name, list_of_elem):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)

# API FUNCTIONS
def requests_retry_session(retries=3,backoff_factor=0.3,status_forcelist=(500, 502, 504),session=None):
    '''
    Function to ensure we get a good response for the request
    '''
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_access_token(email,password) :
    '''
    Returns the access token of the EMT Madrid API
        
        Parameters
        ----------
        email : string
            The email of the account
        password : string
            Password of the account
    '''
    response = requests_retry_session().get(
        'https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/',
        headers={
            'email':email,
            'password':password
        },
        timeout=5
    )
    
    json_response = response.json()
    accessToken = json_response['data'][0]['accessToken']
    return accessToken


def main():
    
    rt_started = False
    
    #Normal buses hours range
    start_time_day = datetime.time(6,0,0)
    end_time_day = datetime.time(23,30,0)
    #Night buses hours range
    start_time_night = datetime.time(0,0,0)
    end_time_night = datetime.time(5,30,0)
    
    while True :
        #Retrieve data every interval seconds if we are between 6:00 and 23:30
        now = datetime.datetime.now() + timedelta(hours=1)
        #If we are not in the weekend
        if now.weekday() in [0,1,2,3,4] :
            #Retrieve data from lines 68,82,91,92,132
            if time_in_range(start_time_day,end_time_day,now.time()) :
                if not rt_started :
                    rt = RepeatedTimer(2, print, "Hello World")
                    rt_started = True
            else :
                #Stop timer if it exists
                if tl_started :
                    rt.stop()
                    rt_started = False
        #If we are in Saturday on Sunday
        else : 
            #Retrieve data from lines 68,82,132
            if time_in_range(start_time_day,end_time_day,now.time()) :
                pass
            #Retrieve data from lines 502,506
            elif time_in_range(start_time_night,end_time_night,now.time()) :
                pass
            else :
                #Stop timer if it exists
                if rt_started :
                    rt.stop()
                    rt_started = False 
                    
        #Wait 2 seconds till next loop (no need to run the loop faster)
        time.sleep(2)
        
if __name__== "__main__":
    main()