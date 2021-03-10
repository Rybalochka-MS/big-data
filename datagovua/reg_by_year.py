import boto3
from time import sleep


def get_data_by_year() -> dict:
    """
    Function sending search request to Athena and return URL to result.

    :return: Link search result file.
    """

    # Connecting to Athena.
    client = boto3.client('athena')

    final_data_set: dict = {}

    for year in range(2017, 2021 + 1):
        # Creating SQL request.
        select = "SELECT count(*) as registrations \
                FROM \"data-gov-ua\".\"data-gov-ua\" \
                WHERE d_reg LIKE '%{}%';".format(year)

        # Starting query.
        query = client.start_query_execution(
            QueryString=select,
            QueryExecutionContext={
                'Database': 'data-gov-ua',
                'Catalog': 'AwsDataCatalog'
            },
            ResultConfiguration={
                'OutputLocation': 's3://fit-one-love/Glue/'
            },
            WorkGroup='primary'
        )

        sleep(5)

        execution_id = query['QueryExecutionId']

        # Processing SQL request.
        client.get_query_execution(QueryExecutionId=str(execution_id))

        response = client.get_query_results(QueryExecutionId=str(execution_id))

        try:
            rows = response['ResultSet']['Rows']
            data_set: list = []
            for row in rows:
                data = {
                    "count": row['Data'][0]['VarCharValue']
                }
                data_set.append(data)
            final_data_set[year] = data_set
        except IndexError:
            pass

        client.stop_query_execution(QueryExecutionId=str(execution_id))

    # Format URL and return
    return final_data_set
