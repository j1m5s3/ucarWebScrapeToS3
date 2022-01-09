import os
import logging
import time
import boto3
from boto3.dynamodb.conditions import Key


ucar_site = "https://data.cosmic.ucar.edu/gnss-ro/"

LOGGER = logging.getLogger(__name__)


def aws_dynamodb_table(profile, table_name):

    session = boto3.session.Session(profile_name=profile)
    dynamodb_table = session.resource('dynamodb').Table(table_name)

    return dynamodb_table


def dynamodb_table_create_entry(dynamodb_table, partition_key, sort_key, ucar_url, s3_url):

    # partition key: string --> mission-procType-fileType
    # sort key: number --> YYYYDDD (Year)(Day of year)
    response = dynamodb_table.put_item(Item={
        'mission-procType-fileType': partition_key,
        'YYYYDDD': sort_key,
        'ucar_url': ucar_url,
        's3_url': s3_url
    })

    return response


def dynamodb_table_batch_write_entries(dynamodb_table, item_list):

    with dynamodb_table.batch_writer(overwrite_by_pkeys=["mission-procType-fileType", "YYYYDDD"]) as batch_write:
        for item in item_list:
            print("Adding item to batch put_item: ", item)
            try:
                batch_write.put_item(Item=item)
            except Exception as e:
                print(e)
                time.sleep(10)
                batch_write.put_item(Item=item)

    return


def dynamodb_table_get_entry(dynamodb_table, the_partition_key, the_sort_key):

    response = dynamodb_table.query(
        KeyConditionExpression=Key('mission-procType-fileType').eq(the_partition_key) & Key('YYYYDDD').eq(the_sort_key)
    )

    return response


# partition key: string --> mission-procType-fileType
# sort key: number --> YYYYDDD (Year)(Day of year)
def get_varnames(file_url, mode):

    root_path, filename = os.path.split(file_url)

    if mode == 'ucar':
        part_sort_key_content = root_path.replace(ucar_site, '').split('/')
    if mode == 's3':
        part_sort_key_content = root_path.split('/')

    if 'atmPhs' in filename:
        filetype = 'atmPhs'
    if 'conPhs' in filename:
        filetype = 'conPhs'
    if 'atmPrf' in filename:
        filetype = 'atmPrf'
    if 'wetPrf' in filename:
        filetype = 'wetPrf'
    if 'wetPf2' in filename:
        filetype = 'wetPf2'

    mission = part_sort_key_content[0]
    if mission == 'spire' or mission == 'geoopt':
        proctype = part_sort_key_content[2]
        year = part_sort_key_content[4]
        doy = part_sort_key_content[5]
    else:
        proctype = part_sort_key_content[1]
        year = part_sort_key_content[3]
        doy = part_sort_key_content[4]

    return {'mission': mission, 'proctype': proctype, 'filetype': filetype, 'year': year, 'doy': doy}


def create_partition_key(mission, proctype, filetype):

    partition_key = f"{mission}-{proctype}-{filetype}"
    return partition_key


def create_sort_key(year, doy):

    sort_key = int(f"{year}{doy}")
    return sort_key


def aws_s3_bucket(profile, bucket_name):

    #bucket_name = 'ucar-earth-ro-archive'
    profile = 'aernasaprod'

    session = boto3.Session(profile_name=profile)
    bucket = session.resource('s3').Bucket(bucket_name)

    return bucket













