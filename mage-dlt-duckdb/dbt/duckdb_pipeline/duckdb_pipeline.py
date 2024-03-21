import dlt
from dlt.sources.helpers import requests


@dlt.source
def duckdb_pipeline_source(api_secret_key=dlt.secrets.value):
    return duckdb_pipeline_resource(api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {"Authorization": f"Bearer {api_secret_key}"}
    return headers


@dlt.resource(write_disposition="append")
def duckdb_pipeline_resource(api_secret_key=dlt.secrets.value):
    headers = _create_auth_headers(api_secret_key)

    # check if authentication headers look fine
    print(headers)

    # make an api call here
    # response = requests.get(url, headers=headers, params=params)
    # response.raise_for_status()
    # yield response.json()

    # test data for loading validation, delete it once you yield actual data
    test_data = [{"id": 0}, {"id": 1}]
    yield test_data


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='duckdb_pipeline', destination='duckdb', dataset_name='duckdb_pipeline_data'
    )

    # print credentials by running the resource
    data = list(duckdb_pipeline_resource())

    # print the data yielded from resource
    print(data)
    exit()

    # run the pipeline with your parameters
    load_info = pipeline.run(duckdb_pipeline_source())

    # pretty print the information on data that was loaded
    print(load_info)
