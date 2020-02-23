from csv import writer

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
 
def append_list_as_row(file_name, list_of_elem):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)

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
