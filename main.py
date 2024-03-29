import logging
import pathlib
import os
import time
import json
from datetime import date
from utilities.ucar_repo_status import recursive_scrape, get_mission_level_urls, check_last_searched, \
    create_last_searched_json, check_for_new_ucar_entries, ucar_urls, download_file, create_s3_obj_key_file, \
    check_new_proctype, check_new_doy, check_new_proctype_year, check_before_doy, check_before_proctype_year

from utilities.compare_in_s3 import compare_against_obj_key_file, get_obj_key_file_list, get_ucar_file_url_list

from aws_utilities.aws_boto3_calls import aws_dynamodb_table, get_varnames, create_partition_key, create_sort_key, \
    dynamodb_table_create_entry, dynamodb_table_batch_write_entries, aws_s3_bucket

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

#table_name = 'gnss-ro-available-tar-file-table'
#dynamodb = aws_dynamodb_table(aws_profile, table_name)

bucket_url = 's3://ucar-earth-ro-archive/'
test_bucket_urls = 's3://processed-nasa-access-data-in-work/'
bucket_name = 'ucar-earth-ro-archive'
test_bucket_name = 'processed-nasa-access-data-in-work'


def test_file_content_compare():
    filepath = os.path.join(parentDir, 'champ.txt')
    compare_against_obj_key_file(filepath)


def test_boto3_calls(the_manifest_file):

    print(os.getpid(), " using file: ", the_manifest_file)
    ucar_url_list = get_ucar_file_url_list(the_manifest_file)
    mission = os.path.basename(the_manifest_file).replace('.txt', '')
    obj_file_key_list = get_obj_key_file_list(mission)

    list_of_item_dicts = []
    for file_url in ucar_url_list:
        varnames_dict = get_varnames(file_url, 'ucar')
        proctype = varnames_dict['proctype']
        filetype = varnames_dict['filetype']
        year = varnames_dict['year']
        doy = varnames_dict['doy']

        ucar_file_key = [file_url.replace(ucar_site, '')]
        intersection = set(ucar_file_key).intersection(set(obj_file_key_list))
        if len(intersection) > 0:
            partition_key = create_partition_key(mission, proctype, filetype)
            sort_key = create_sort_key(year, doy)

            s3_url = os.path.join(bucket_url, ucar_file_key[0])
            ucar_url = file_url

            the_item = {'mission-procType-fileType': partition_key, 'YYYYDDD': sort_key,
                        'ucar_url': ucar_url, 's3_url': s3_url}
            LOGGER.info(f"partkey:  {partition_key} | sortkey:  {sort_key} | ucar_url:  {ucar_url} | s3_url: {s3_url}")
            list_of_item_dicts.append(the_item)
        else:
            partition_key = create_partition_key(mission, proctype, filetype)
            sort_key = create_sort_key(year, doy)

            s3_url = ''
            ucar_url = file_url

            the_item = {'mission-procType-fileType': partition_key, 'YYYYDDD': sort_key,
                        'ucar_url': ucar_url, 's3_url': s3_url}
            LOGGER.info(f"partkey:  {partition_key} | sortkey:  {sort_key} | ucar_url:  {ucar_url} | s3_url: {s3_url}")
            list_of_item_dicts.append(the_item)

    dynamodb_table_batch_write_entries(dynamodb, list_of_item_dicts)

    LOGGER.info(f"{os.getpid()}, finished")

    return


def live_run():
    bucket = aws_s3_bucket(aws_profile, test_bucket_name)

    file_manifest_list = os.listdir(os.path.join(parentDir, 'ucar_file_manifests_per_mission', ""))
    file_manifests_path_list = []

    to_download_list = []

    if len(file_manifest_list) > 0:
        for manifest_file in file_manifest_list:
            manifest_filepath = os.path.join(parentDir, 'ucar_file_manifests_per_mission', manifest_file)
            file_manifests_path_list.append(manifest_filepath)
    else:
        for obj in bucket.objects.filter(Delimiter='/', Prefix='ucar_file_manifest_per_mission/'):
            obj_key = obj.key
            filename = obj_key.split('/')[1]
            local_path = os.path.join(parentDir, 'ucar_file_manifests_per_mission', filename)
            print("local: ", local_path, " | obj.key: ", obj_key)
            bucket.download_file(obj_key, local_path)

        file_manifest_list = os.listdir(os.path.join(parentDir, 'ucar_file_manifests_per_mission', ""))
        for manifest_file in file_manifest_list:
            manifest_filepath = os.path.join(parentDir, 'ucar_file_manifests_per_mission', manifest_file)
            file_manifests_path_list.append(manifest_filepath)
    print(file_manifests_path_list)
    # Change logic for determining if last_searched_info.json is available 1/8/2022
    if "last_searched_info.json" not in os.listdir(parentDir):
        print("Creating new last_searched_info.json...")
        last_searched_json_path = create_last_searched_json(file_manifests_path_list)
        with open(last_searched_json_path, 'r+') as the_json_file:
            last_searched_dict = json.load(the_json_file)
    else:
        with open("last_searched_info.json", 'r+') as the_json_file:
            last_searched_dict = json.load(the_json_file)

    new_ucar_urls = check_for_new_ucar_entries(last_searched_dict)
    for new_url in new_ucar_urls:
        recursive_scrape(new_url)
    to_download_list.extend(ucar_urls)

    print(to_download_list)
    if len(to_download_list) > 0:

        for to_download_item in to_download_list:
            # download locally
            # conditional for testing only 1/9/2022
            if "conPhs" not in to_download_item and "atmPhs" not in to_download_item:
                path_to_local_file = download_file(to_download_item)
                s3_key = to_download_item.replace(ucar_site, '')

                # upload to s3
                bucket.upload_file(path_to_local_file, s3_key)

        new_proctype_signal_file = f'signal_new_proc_type_{date.today()}.json'
        if new_proctype_signal_file in os.listdir(parentDir):
            bucket.upload_file(new_proctype_signal_file, new_proctype_signal_file)
        # publish new s3_obj_key_file
        local_key_file_path = create_s3_obj_key_file(bucket)
        obj_key_filename = os.path.basename(local_key_file_path)
        bucket.upload_file(local_key_file_path, obj_key_filename)
        # publish new last_searched_info.json

    return


def use_policies_json():
    """
    Driver for web scraping utility that utilizes policies.json for determining
    :return:
    """
    with open("policies.json", 'r+') as the_json_file:
        policy_dict = json.load(the_json_file)

    to_search_urls = []
    to_download_list = []
    if len(mission_urls['non_data_description']) > 0:
        LOGGER.info(f"NEW MISSIONS FOUND: {mission_urls['non_data_description']}")

    for mission in policy_dict.keys():
        proc_type_list = []
        mission_url = os.path.join(ucar_site, mission, '')
        for proc_type in policy_dict[mission].keys():
            proc_type_list.append(proc_type)
            if policy_dict[mission][proc_type]['policy'] != "keep_none":
                policy_url = os.path.join(ucar_site, mission, proc_type, '')
                policy_start_year = policy_dict[mission][proc_type]['start_date'].split('-')[0]
                policy_start_doy = policy_dict[mission][proc_type]['start_date'].split('-')[1]
                policy_end_year = policy_dict[mission][proc_type]['end_date'].split('-')[0]
                policy_end_doy = policy_dict[mission][proc_type]['end_date'].split('-')[1]

                if policy_dict[mission][proc_type]['policy'] == 'keep_all':
                    to_search_urls.append(policy_url)
                if policy_dict[mission][proc_type]['policy'] == 'keep_after':
                    # keep after start date
                    to_search_urls.extend(check_new_doy(policy_url, policy_start_year, policy_start_doy))
                    to_search_urls.extend(check_new_proctype_year(policy_url, policy_start_year))
                if policy_dict[mission][proc_type]['policy'] == 'keep_before':
                    # keep before end date
                    to_search_urls.extend(check_before_doy(policy_url, policy_end_year, policy_end_doy))
                    to_search_urls.extend(check_before_proctype_year(policy_url, policy_end_year))
                if policy_dict[mission][proc_type]['policy'] == 'keep_after_and_before':
                    # keep between start and end date
                    to_search_urls.extend(check_new_doy(policy_url, policy_start_year, policy_start_doy))
                    to_search_urls.extend(check_before_doy(policy_url, policy_end_year, policy_end_doy))
                    year_before_end_urls = check_before_proctype_year(policy_url, policy_end_year)
                    for the_year_url in year_before_end_urls:
                        if policy_end_year not in the_year_url and policy_start_year not in the_year_url:
                            to_search_urls.append(the_year_url)

                #new_year_urls = check_new_proctype_year(policy_url, policy_end_year)
                #new_doy_urls = check_new_doy(policy_url, policy_end_year, policy_end_doy)
                #if len(new_year_urls) > 0:
                #    to_search_urls.extend(new_year_urls)
                #    new_year_end = new_year_urls[0].split('/')[-2]
                #    new_doy_end = check_new_doy(policy_url, new_year_end, '000')[-1].split('/')[-2]
                #    new_end_date = f"{new_year_end}-{new_doy_end}"
                #    policy_dict[mission][proc_type]["end_date"] = new_end_date
                #if len(new_doy_urls) > 0:
                #    to_search_urls.extend(new_doy_urls)

        new_proc_types = check_new_proctype(mission_url, proc_type_list)
        if len(new_proc_types) > 0:
            LOGGER.info(f"NEW PROC TYPES FOUND FOR {mission}: {new_proc_types}")

    print(to_search_urls)
    for url in to_search_urls:
        recursive_scrape(url)
    to_download_list.extend(ucar_urls)
    print(to_download_list)

    refined_to_download_list = compare_against_obj_key_file(to_download_list)
    # Then download files in refined_to_download_list

    return


def test_download():

    bucket = aws_s3_bucket(aws_profile, test_bucket_name)
    for obj in bucket.objects.filter(Delimiter='/', Prefix='ucar_file_manifest_per_mission/'):
        obj_key = obj.key
        filename = obj_key.split('/')[1]
        local_path = os.path.join(parentDir, 'ucar_file_manifests_per_mission', filename)
        print("local: ", local_path, " | obj.key: ", obj_key)
        bucket.download_file(obj_key, local_path)


def upload_manifest_files_to_s3():

    bucket = aws_s3_bucket(aws_profile, test_bucket_name)

    file_manifest_list = os.listdir(os.path.join(parentDir, 'ucar_file_manifests_per_mission', ""))
    file_manifests_path_list = []

    for manifest_file in file_manifest_list:
        manifest_filepath = os.path.join(parentDir, 'ucar_file_manifests_per_mission', manifest_file)
        file_manifests_path_list.append(manifest_filepath)

    for file_path in file_manifests_path_list:
        s3_key = os.path.join('ucar_file_manifest_per_mission', os.path.basename(file_path))
        bucket.upload_file(file_path, s3_key)

    return


if __name__ == '__main__':

    start = time.perf_counter()
    #print(mission_urls)
    #with Pool(len(mission_urls)) as p:
    #    p.map(recursive_scrape, mission_urls)
    #if len(mission_urls) ==

    #print(mission_urls)
    #for url in mission_urls:
    #    print(url)
    #    recursive_scrape(url)
    #test_mission_urls = ["https://data.cosmic.ucar.edu/gnss-ro/cosmic2/", "https://data.cosmic.ucar.edu/gnss-ro/cosmic1/"]
    #with Pool(4) as p:
    #    p.map(recursive_scrape, test_mission_urls)
    #for url in test_mission_urls:
    #    recursive_scrape(url)
    #file_manifest_list = os.listdir(os.path.join(parentDir, 'ucar_file_manifests_per_mission', ""))
    #file_manifests_path_list = []
    #for manifest_file in file_manifest_list:
    #    manifest_filepath = os.path.join(parentDir, 'ucar_file_manifests_per_mission', manifest_file)
    #    file_manifests_path_list.append(manifest_filepath)

    #with Pool(len(file_manifests_path_list)) as p:
    #    p.map(test_boto3_calls, file_manifests_path_list)
    #last_url_searched_list = []
    #if len(file_manifest_list) > 0:
    #    diff_dict = {}
    #    for manifest_file in file_manifest_list:
    #        manifest_filepath = os.path.join(parentDir, 'ucar_file_manifests_per_mission', manifest_file)
            #last_url_searched_list.append(check_last_searched(manifest_filepath))
    #        mission = manifest_file.split('.')[0]
    #        diff_dict.update(compare_against_obj_key_file(manifest_filepath, mission))
    #    with open("diff.json", 'w+') as diff_json:
    #        json.dump(diff_dict, diff_json)
    #live_run()
    use_policies_json()
    #test_download()
    #upload_manifest_files_to_s3()
    end = time.perf_counter()
    print(f"runtime = {end - start}")

    #test_ucar_site_drill_down()
    #test_file_content_compare()
    pass
