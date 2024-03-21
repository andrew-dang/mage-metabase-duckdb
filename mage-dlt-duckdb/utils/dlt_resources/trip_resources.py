import dlt
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa

# Define resource
@dlt.resource
def read_fhv(file_paths:list):
    """
    Get all files in specified folder
    """
    for file_path in file_paths:
        print(f"Reading in file: {file_path}")
        table = pq.read_table(file_path)

        # Get year and month from file path
        year = file_path[-15:-11]
        month = file_path[-10:-8]

        # lowercase column names
        new_cols = [col.lower() for col in table.column_names]
        table = table.rename_columns(new_cols)

        # Set schema
        schema = pa.schema([
            ('dispatching_base_num', pa.string()),
            ('pickup_datetime', pa.timestamp('s')),
            ('dropoff_datetime', pa.timestamp('s')),
            ('pulocationid', pa.int64()),
            ('dolocationid', pa.int64()),
            ('sr_flag', pa.float64()),
            ('affiliated_base_number', pa.string())
        ])

        table = table.cast(target_schema=schema)

        # filter tables
        date_filter = (pc.month(pc.field('pickup_datetime')) == int(month)) & (pc.year(pc.field('pickup_datetime')) == int(year))
        table = table.filter(date_filter)
        
        yield table

@dlt.resource
def read_green(file_paths: list):
    """
    Read all file paths from file paths and yield table
    """
    for file_path in file_paths:
        print(f"Reading in file: {file_path}")
        table = pq.read_table(file_path)

        # Get year and month from file path
        year = file_path[-15:-11]
        month = file_path[-10:-8]

        # Create schema
        green_schema = pa.schema([
            ('vendor_id', pa.int64()),
            ('lpep_pickup_datetime', pa.timestamp('s')),
            ('lpep_dropoff_datetime', pa.timestamp('s')),
            ('store_and_fwd_flag', pa.string()),
            ('ratecode_id', pa.float64()),
            ('pu_location_id', pa.int64()), 
            ('do_location_id', pa.int64()), 
            ('passenger_count', pa.float64()), 
            ('trip_distance', pa.float64()),
            ('fare_amount', pa.float64()),
            ('extra', pa.float64()),
            ('mta_tax', pa.float64()),
            ('tip_amount', pa.float64()),
            ('tolls_amount', pa.float64()),
            ('ehail_fee', pa.float64()),
            ('improvement_surcharge', pa.float64()),
            ('total_amount', pa.float64()),
            ('payment_type', pa.float64()),
            ('trip_type', pa.float64()),
            ('congestion_surcharge', pa.float64())
        ])
        

        # Rename columns
        new_cols = [
            'vendor_id',
            'lpep_pickup_datetime',
            'lpep_dropoff_datetime',
            'store_and_fwd_flag',
            'ratecode_id',
            'pu_location_id', 
            'do_location_id',
            'passenger_count', 
            'trip_distance',
            'fare_amount', 
            'extra',        
            'mta_tax',        
            'tip_amount',           
            'tolls_amount',
            'ehail_fee',            
            'improvement_surcharge',
            'total_amount', 
            'payment_type', 
            'trip_type',         
            'congestion_surcharge'
        ]
        
        # Rename and cast columns
        table = table.rename_columns(new_cols)
        table = table.cast(target_schema=green_schema)
        
        # filter tables
        date_filter = (pc.month(pc.field('lpep_pickup_datetime')) == int(month)) & (pc.year(pc.field('lpep_pickup_datetime')) == int(year))
        table = table.filter(date_filter)

        yield table

@dlt.resource
def read_yellow(file_paths: list):
    """
    Read all file paths from file paths and yield table
    """
    for file_path in file_paths:
        print(f"Reading in file: {file_path}")
        table = pq.read_table(file_path)

        # Get year and month from file path
        year = file_path[-15:-11]
        month = file_path[-10:-8]
        
         # Create schema
        yellow_schema = pa.schema([
            ('vendor_id', pa.int64()),
            ('tpep_pickup_datetime', pa.timestamp('s')),
            ('tpep_dropoff_datetime', pa.timestamp('s')),
            ('passenger_count', pa.float64()),
            ('trip_distance', pa.float64()),
            ('ratecode_id', pa.float64()),
            ('store_and_fwd_flag', pa.string()),
            ('pu_location_id', pa.int64()), 
            ('do_location_id', pa.int64()),  
            ('payment_type', pa.int64()),
            ('fare_amount', pa.float64()),
            ('extra', pa.float64()),
            ('mta_tax', pa.float64()),
            ('tip_amount', pa.float64()),
            ('tolls_amount', pa.float64()),
            ('improvement_surcharge', pa.float64()),
            ('total_amount', pa.float64()),
            ('congestion_surcharge', pa.float64()),
            ('airport_fee', pa.float64())
        ])

         # Rename columns
        new_cols = [
            'vendor_id',
            'tpep_pickup_datetime',
            'tpep_dropoff_datetime',
            'passenger_count', 
            'trip_distance',
            'ratecode_id',
            'store_and_fwd_flag',
            'pu_location_id', 
            'do_location_id',
            'payment_type',
            'fare_amount', 
            'extra',        
            'mta_tax',        
            'tip_amount',           
            'tolls_amount',            
            'improvement_surcharge',
            'total_amount',          
            'congestion_surcharge',
            'airport_fee'
        ]
        table = table.rename_columns(new_cols)
        table = table.cast(target_schema=yellow_schema)

        # filter tables
        date_filter = (pc.month(pc.field('tpep_pickup_datetime')) == int(month)) & (pc.year(pc.field('tpep_pickup_datetime')) == int(year))
        table = table.filter(date_filter)

        yield table