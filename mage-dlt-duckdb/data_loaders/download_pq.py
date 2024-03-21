if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def download_data(*args, **kwargs):
    """
    Download parquet file to a folder in the home directory of the local repo. 
    
    Returns:
        file_dict (dict): A dictionary containing the local path to downloaded data in the run. 
    """
    # Base URL to download files 
    import os
    import os.path
    
    # https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-02.parquet

    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    
    # Configure color, years and months for initial load - first 3 months of 2019 for all colors
    colors = ["green", "yellow", "fhv"]
    years = [2019]
    months = [m for m in range(1,4)]

    
    # Empty list for file names 
    file_dict = {}
    for color in colors:
        # Empty list to store file paths
        file_paths = []
        
        # Directory to save downloaded data
        DATA_DIR = f"/home/src/data/{color}"
        
        for year in years:
            for month in months:
                FILE_NAME = f"{color}_tripdata_{year}-{str(month).zfill(2)}.parquet"

                FILE_URL = f"{BASE_URL}/{FILE_NAME}"
                print(FILE_URL)
                DEST_PATH = f"{DATA_DIR}/{FILE_NAME}"
                print(DEST_PATH)

                # Check if file exists
                if os.path.isfile(DEST_PATH):
                    print(f"File already on disk: {DEST_PATH}")
                    file_paths.append(DEST_PATH)
                    continue
                
                else:
                
                    print(f"Downloading file: {FILE_NAME}")
                    file_paths.append(DEST_PATH)

                    # Download files 
                    os.system(f"wget -q {FILE_URL} -P {DATA_DIR}")
        
        # Save file paths to dictionary 
        file_dict[color] = file_paths


    return file_dict
    

@test
def test_output(*args) -> None:
    """
    Template code for testing the output of the block.
    """
    pass