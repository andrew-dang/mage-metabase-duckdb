if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(file_dict, *args, **kwargs):
    """
    Use dlt to export taxi data to Postgres

    Args:
       file_dict (dict): Dictionary containing file paths for each service type to be uploaded

    """
    import dlt

    import pyarrow.parquet as pq
    import pyarrow as pa
    from importlib import import_module
    tr = import_module('mage-dlt-duckdb.utils.dlt_resources.trip_resources')

    # DuckDB credentials 
    credentials = '/home/src/duckdb_files/database.db'

    # Loop through each color and run dlt pipelines
    for color in file_dict.keys():
        print(f"Working on {color} pipeline...")
        
        # Get file paths list from file_dict
        file_paths = file_dict[color]

        pipeline = dlt.pipeline(
            pipeline_name=f"{color}_pipeline_test",
            destination=dlt.destinations.duckdb(credentials=credentials),
            dataset_name='test',
            export_schema_path=f'/home/src/dlt_schemas/export/{color}'
        )

        # Use correct read function depending on color
        if color == 'green':
            pipeline.run(
                tr.read_green(file_paths),
                table_name=f"{color}_pq_test",
                write_disposition='merge',
                loader_file_format='parquet'
            )

        elif color == 'yellow':
            pipeline.run(
                tr.read_yellow(file_paths),
                table_name=f"{color}_pq_test",
                write_disposition='merge',
                loader_file_format='parquet'
            )

        else:
            pipeline.run(
                tr.read_fhv(file_paths),
                table_name=f"{color}_pq_test",
                write_disposition='merge',
                loader_file_format='parquet'
            )
