import logging
import pathlib
import os
import time
import json
from utilities.ucar_repo_status import recursive_scrape, ucar_urls, get_mission_level_urls, check_last_searched
from utilities.compare_in_s3 import compare_against_obj_key_file, get_obj_key_file_list, get_ucar_file_url_list

from aws_utilities.aws_boto3_calls import aws_dynamodb_table, get_varnames, create_partition_key, create_sort_key, \
    dynamodb_table_create_entry

from multiprocessing import Pool


logging.basicConfig(level=logging.INFO,
                    format='%(levelname)-2s [%(filename)s:%(lineno)d] %(message)s',
                    handlers=[
                        logging.FileHandler(filename='log.txt', mode='w+'),
                        #logging.StreamHandler(sys.stdout)
                    ])
LOGGER = logging.getLogger(__name__)


parentDir = pathlib.Path(__file__).parent.resolve()
ucar_site = "https://data.cosmic.ucar.edu/gnss-ro/"
mission_urls = get_mission_level_urls(ucar_site)

aws_profile = 'aernasaprod'
table_name = 'gnss-ro-available-tar-file-table'
dynamodb = aws_dynamodb_table(aws_profile, table_name)


def test_ucar_site_drill_down():

    recursive_scrape(ucar_site)
    print(ucar_urls)

    return


def test_file_content_compare():
    filepath = os.path.join(parentDir, 'champ.txt')
    compare_against_obj_key_file(filepath)


def test_boto3_calls(manifest_file):

    print(os.getpid(), " using file: ", manifest_file)
    ucar_url_list = get_ucar_file_url_list(manifest_file)

    list_of_item_dicts = []
    for file_url in ucar_url_list:
        varnames_dict = get_varnames(file_url, 'ucar')
        mission = varnames_dict['mission']
        proctype = varnames_dict['proctype']
        filetype = varnames_dict['filetype']
        year = varnames_dict['year']
        doy = varnames_dict['doy']

        filename = os.path.split(file_url)[1]
        obj_file_key_list = get_obj_key_file_list(mission)
        for obj_file_key in obj_file_key_list:
            if filename in obj_file_key:
                partition_key = create_partition_key(mission, proctype, filetype)
                sort_key = create_sort_key(year, doy)

                s3_url = obj_file_key
                ucar_url = file_url

                print("partkey: ", partition_key, "|  sortkey: ", sort_key, "|   ucar_url: ", ucar_url, "|   s3_url: ", s3_url)

                response = dynamodb_table_create_entry(dynamodb, partition_key, sort_key, ucar_url, s3_url)
                print(os.getpid(), "  got response: ", response)

    return


if __name__ == '__main__':

    start = time.perf_counter()

    #with Pool(len(mission_urls)) as p:
    #    p.map(recursive_scrape, mission_urls)
    #if len(mission_urls) ==

    #print(mission_urls)
    #for url in mission_urls:
    #    print(url)
    #    recursive_scrape(url)

    file_manifest_list = os.listdir(os.path.join(parentDir, 'ucar_file_manifests_per_mission', ""))
    file_manifests_path_list = []
    for manifest_file in file_manifest_list:
        manifest_filepath = os.path.join(parentDir, 'ucar_file_manifests_per_mission', manifest_file)
        file_manifests_path_list.append(manifest_filepath)

    #with Pool(len(file_manifest_list)) as p:
    #    p.map(test_boto3_calls, file_manifests_path_list)
    #last_url_searched_list = []
    if len(file_manifest_list) > 0:
        diff_dict = {}
        for manifest_file in file_manifest_list:
            manifest_filepath = os.path.join(parentDir, 'ucar_file_manifests_per_mission', manifest_file)
            #last_url_searched_list.append(check_last_searched(manifest_filepath))
            mission = manifest_file.split('.')[0]
            diff_dict.update(compare_against_obj_key_file(manifest_filepath, mission))
        with open("diff.json", 'w+') as diff_json:
            json.dump(diff_dict, diff_json)
    end = time.perf_counter()
    print(f"runtime = {end - start}")

    #test_ucar_site_drill_down()
    #test_file_content_compare()
    pass
