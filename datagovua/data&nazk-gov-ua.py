"""
Lambda function for get personal info about teacher car.

This function use unless modules:
    * requests - for send request to data.gov.ua and avto-info.com.ua
    * json - for format response function message
    * boto3 - AWS SDK
    * re - regular library for parse avto-info.com.ua page

The function can not be a module but can be used as an API.
"""
import requests
import json
import boto3
import re


def get_personal_info(url: str) -> dict:
    """
    Function a send request to data.gov.ua API and getting
    property information from response JSON.

    :param url: API URL from data.gov.ua by person.
    :return: Property information.
    """
    # Send request to API
    response = requests.get(url)

    # Parsing response.
    data = response.json()['data']
    # Getting information about car.
    car_info = data['step_6']['data'][0]
    # A splitting date for future use a correct format.
    owning_date = str(car_info['owningDate']).split(".")

    # Creating data set for run searching in Athena.
    data_set = {
        # CSV column name : value
        "make_year": car_info['graduationYear'],
        "d_reg": "{0}-{1}-%".format(owning_date[2], owning_date[1]),
        "brand": str(car_info['brand']).upper() + "%",
        "model": str(car_info['model']).upper()
    }

    return data_set


def get_register_car_info(data: dict) -> str:
    """
    Function sending search request to Athena and return URL to result.

    :param data: Information about teacher car.
    :return: Link search result file.
    """
    # Creating SQL request.
    select = "SELECT * FROM \"data-gov-ua\".\"data-gov-ua-2017\" \
              WHERE make_year LIKE '{0}' \
              AND d_reg LIKE '{1}' \
              AND brand LIKE '{2}' \
              AND model LIKE '{3}' \
              AND color LIKE 'ЧОРНИЙ';".format(
        data['make_year'], data['d_reg'], data['brand'], data['model']
    )

    # Connecting to Athena.
    client = boto3.client('athena')
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

    # Processing SQL request.
    response = client.get_query_execution(
        QueryExecutionId=query['QueryExecutionId']
    )

    # Getting result file location
    endpoint = response['QueryExecution']['ResultConfiguration']['OutputLocation']

    # Format URL and return
    return str(endpoint).replace("s3://fit-one-love", "https://fit-one-love.s3.eu-central-1.amazonaws.com")


def get_detailed_car_info(url: str) -> [dict, dict]:
    response = requests.get(url)
    result = re.findall(r'<li class="list-group-item">?.*', response.text)
    car_data_set = {}
    for item in result:
        data = str(item).replace('<li class="list-group-item">', '').replace('</li>', '').split(": ")
        car_data_set[data[0]] = data[1]

    data_set = {
        "make_year": car_data_set['Рік'],
        "d_reg": car_data_set['Дата реєстраціі'],
        "brand": car_data_set['Виробник'] + "%",
        "model": car_data_set['Модель']
    }

    return data_set, car_data_set


def lambda_handler(_event, _context):
    """
    Main function for run AWS Lambda.

    :param _event: Unused
    :param _context: Unused
    :return: None
    """
    # URLs
    __api_url = "https://public-api.nazk.gov.ua/v2/documents/4d920e72-76dd-4e79-8d64-e706a34e27ca"
    __car_url = "https://avto-info.com.ua/plate/АА2750РВ"

    # Getting information from declaration.
    declaration = get_personal_info(__api_url)
    # Getting information from cars registry by car number.
    search_data, all_car_info = get_detailed_car_info(__car_url)
    # Compare information with data.gov.ua register.
    search_by_declaration = get_register_car_info(declaration)
    search_by_car_number = get_register_car_info(search_data)

    # Creating response message in JSON format.
    response = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "Search car data by declaration info in the registry: ": search_by_declaration,
                "Search car data by register number": all_car_info,
                "Search car data by register number in the registry: ": search_by_car_number
            },
            sort_keys=True, indent=4, ensure_ascii=False
        ).encode('utf8')
    }

    return response
