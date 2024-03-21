if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def incremental_download(*args, **kwargs):
    """
    Download parquet file to a folder in the home directory of the local repo. 
    Use execution date to determine which files to download. 
    
    Returns:
        file_dict (dict): A dictionary containing the local path to downloaded data in the run. 
    """
    # Base URL to download files 
    import os
    import os.path
    # from datetime import datetime 
    from dateutil.relativedelta import relativedelta
    
    # Sample url
    # https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-02.parquet

    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    # Get runtime variables to determine what file to load 
    colors = kwargs.get("colors")
    execution_date = kwargs.get("execution_date")
    
    # # Debug
    # import datetime 
    # execution_date = datetime.datetime(2019,12,1)
    # colors = ['green', 'yellow', 'fhv']

    year = str(execution_date.year)
    month = str(execution_date.month).zfill(2)

    print(f"Execution date year-month: {year}-{month}")

    # Data is uploaded to their website with a 2 month delay 
    download_date = execution_date - relativedelta(months=2)
    download_year_month = f"{str(download_date.year)}-{str(download_date.month).zfill(2)}"
    print(f"Download date year-month: {download_year_month}\n")

    # Empty list for file names 
    file_dict = {}
    for color in colors:
        # Empty list to store file paths
        file_paths = []
        
        # Directory to save downloaded data
        DATA_DIR = f"/home/src/data/{color}"
        
        # Template for file name 
        FILE_NAME = f"{color}_tripdata_{download_year_month}.parquet"

        # Combine base URL with the file name to get the file URL
        FILE_URL = f"{BASE_URL}/{FILE_NAME}"
        print(f"URL for download: {FILE_URL}")
        
        # Combine data directory with file name to get the local path to save data to 
        DEST_PATH = f"{DATA_DIR}/{FILE_NAME}"
        print(f"Local save path: {DEST_PATH}")

        # Check if file exists; download locally if it does not exist
        if os.path.isfile(DEST_PATH):
            print(f"File already on disk: {DEST_PATH}\n")
            file_paths.append(DEST_PATH)
            file_dict[color] = file_paths
            continue
        
        else:
            print(f"File does not exist locally. Downloading file: {FILE_NAME}\n")
            file_paths.append(DEST_PATH)
            file_dict[color] = file_paths

            # Download files 
            os.system(f"wget -q {FILE_URL} -P {DATA_DIR}")
    

    return file_dict
    

@test
def test_output(file_dict, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert bool(file_dict) == True, 'File dictionary is empty and has no local file paths'