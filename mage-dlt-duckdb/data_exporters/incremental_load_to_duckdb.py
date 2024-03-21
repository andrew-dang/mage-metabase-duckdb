if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(file_dict: dict, *args, **kwargs):
    """
    Use dlt to export taxi data to Postgres

    Args:
       file_dict (dict): Dictionary containing file paths for each service type to be uploaded

    """
    import dlt
    import pyarrow.parquet as pq
    import pyarrow as pa
    import pyarrow.compute as pc

    # # Run dlt pipelines
    # for color in file_dict.keys():
    #     # Create dlt resource
    #     if color == 'green':
    #         primary_key = ('vendor_id', 'lpep_pickup_datetime')
    #     elif color == 'yellow':
    #         primary_key = ('vendor_id', 'tpep_pickup_datetime')
    #     else:
    #         primary_key = ('vendor_id', 'pickup_datetime')
    
    # Define dlt resources
    @dlt.resource(primary_key=('vendor_id', 'lpep_pickup_datetime'), write_disposition='merge')
    def read_parquet(file_paths, color: str):
        """
        Get all files in specified folder
        """
        for file_path in file_paths:
            print(f"Reading in file: {file_path}")
            table = pq.read_table(file_path)

            # fill null/nan values with 0
            table = table.set_column(
                    i=4,
                    field_='RatecodeID',
                    column=pc.fill_null(table.column("RatecodeID"), fill_value=pa.scalar(0, pa.int64()))
                    )

            if color == 'fhv':
                # lowercase column names
                new_cols = [col.lower() for col in table.column_names]
                table = table.rename_columns(new_cols)

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
            
            yield table


    # Run dlt pipelines
    for color in file_dict.keys():

        
        # Run pipelines
        file_paths = file_dict[color]

        pipeline = dlt.pipeline(
            pipeline_name=f"{color}_pipeline_test",
            destination="duckdb",
            credentials='/home/src/duckdb_files/database.db',
            dataset_name='test'
        )

        pipeline.run(
            read_parquet(file_paths, color),
            table_name=f"{color}_pq_test",
            write_disposition='merge',
            loader_file_format='parquet'
        )
    
        # pipeline.run(
        #     read_parquet(file_paths, color),
        #     table_name=f"{color}_pq_test",
        #     write_disposition='merge',
        #     loader_file_format='parquet'
        # )