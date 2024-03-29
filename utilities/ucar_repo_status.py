import os
import requests
import logging
import json
import re
import numpy as np
from datetime import date
from bs4 import BeautifulSoup

ucar_manifests_loc = "/home/i28373/ucar_webscrape/ucarWebScrapeToS3/ucar_file_manifests_per_mission/"
ucar_site = "https://data.cosmic.ucar.edu/gnss-ro/"
LOGGER = logging.getLogger(__name__)


ucar_urls = []
current_yr = date.today().year
date_today = date.today()
doy_arr = [str(doy).zfill(3) for doy in np.arange(0,367,1)]
year_arr = [str(yr) for yr in np.arange(1990, current_yr + 1, 1)]

data_description_missions = ['gpsmet/', 'gpsmetas/', 'grace/', 'sacc/', 'champ/', 'cosmic1/', 'cosmic2/',
                             'tsx/', 'tdx/', 'cnofs/', 'metopa/', 'metopb/', 'metopc/', 'kompsat5/', 'paz/']


home_path = os.environ['HOME']

valid_proc_types = ['repro2016/', 'postProc/', 'repro2013/', 'nrt/']


def check_if_valid_proc_type(url):
    """
    Function used to check if the program is searching under a valid processing type. Takes a ucar url as input and
    returns True if under a valid processing type and False otherwise.
    :param url:
    :return True/False:
    """
    for valid_proc in valid_proc_types:
        if valid_proc in url:
            return True
    return False


def check_if_correct_level(url):
    """
    Function used to check if program is searching under the correct level (level1b or level2). Takes a ucar url as
    input and returns True if under correct level and False otherwise.
    :param url:
    :return True/False:
    """
    excluded_criteria = ['level0/', 'level1a/', 'provisional/', 'tools/']
    for excluded_level in excluded_criteria:
        if excluded_level in url:
            return False
    return True


def check_if_correct_filetype(new_url):
    """
    Function used to check if program has found correct filetypes for download (conPhs, atmPhs, atmPrf, wetPrf, wetPf2).
    Takes a ucar url as input and returns True if a valid filetype is found and False otherwise.
    :param new_url:
    :return True/False:
    """
    filetype_list = ['conPhs', 'atmPhs', 'atmPrf', 'wetPrf', 'wetPf2']
    if new_url.endswith(".tar.gz"):
        for filetype in filetype_list:
            if filetype in new_url:
                return True

    return False


def check_if_in_doy_level(new_url):
    """
    Function used to check if program is searching under the day of year level of the ucar site (example. 001, 056, 300).
    Takes a ucar url as input and returns True if under day of year level and False otherwise.
    :param new_url:
    :return True/False:
    """
    doy = new_url.split('/')[-2]

    if doy in doy_arr:
        return True

    return False


# Only to be used with base ucar url: "https://data.cosmic.ucar.edu/gnss-ro/"
def get_mission_level_urls(url):
    """
    Function used to get mission urls under ucar gnss-ro repo that are described in the data description and not described.
    Takes the base ucar gnss-ro url as input and returns a dictionary that contains a list of urls for missions in the
    data description and missions that are not.
    :param url:
    :return dictionary:
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    href_list = []
    for a_tag in soup.findAll('a'):
        href_tag_text = a_tag.attrs.get('href')
        href_list.append(href_tag_text)

    data_description_mission_urls = [os.path.join(url, mission) for mission in href_list[1:]
                                    if mission in data_description_missions]
    non_data_description_mission_urls = [os.path.join(url, mission) for mission in href_list[1:]
                                    if mission not in data_description_missions and mission !='fid/']

    mission_url_dict = {'data_description': data_description_mission_urls,
                        'non_data_description': non_data_description_mission_urls}

    return mission_url_dict


def recursive_scrape(url):
    """
    Function recursively searches input ucar url until correct filetype is found. If the correct .tar.gz file is found,
    that file url is added to a global list called ucar_urls which will contain the urls needed to download the files.
    Additionally the urls are written to a file named <mission>.txt for use after the search has concluded.
    :param url:
    :return:
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    mission = url.split('/')[4]
    filename = f"{mission}.txt"
    # Change filepath so that it is not using hard coded paths such as "ucar_manifests_loc" 1/5/2022
    filepath = os.path.join(ucar_manifests_loc, filename)

    href_list = []
    for a_tag in soup.findAll('a'):
        href_tag_text = a_tag.attrs.get('href')
        href_list.append(href_tag_text)

    for link in href_list[1:]:
        new_url = os.path.join(url, link)

        if new_url.endswith('tar.gz'):
            if check_if_correct_filetype(new_url):
                if new_url not in ucar_urls:
                    LOGGER.info(new_url)
                    with open(filepath, 'a') as mission_file:
                        mission_file.write(f"{new_url},")
                    ucar_urls.append(new_url)

        if new_url.endswith('/'):
            if check_if_valid_proc_type(new_url):
                if check_if_correct_level(new_url):
                    #if check_if_in_doy_level(new_url):
                    #LOGGER.info(new_url)
                    print("drilling down ----> ", new_url)
                    recursive_scrape(new_url)

    return


def check_last_searched(manifest_filepath):

    with open(manifest_filepath, 'r+') as manifest_doc:
        last_searched_url = manifest_doc.read().split(',')[-2]

    return last_searched_url


def store_proc_levels(manifest_file_path):

    proc_type_list = []
    with open(manifest_file_path, 'r+') as manifest_doc:
        manifest_urls = manifest_doc.read().replace(ucar_site, '').split(',')

    for url in manifest_urls:
        url_contents = url.split('/')
        if "spire" in url_contents or "geoopt" in url_contents:
            if len(url_contents) > 1:
                proc_type = url_contents[2]
                if proc_type not in proc_type_list:
                    proc_type_list.append(proc_type)
        else:
            if len(url_contents) > 1:
                proc_type = url_contents[1]
                if proc_type not in proc_type_list:
                    proc_type_list.append(proc_type)

    return proc_type_list


def create_last_searched_info(manifest_file_path):

    with open(manifest_file_path, 'r+') as manifest_doc:
        manifest_urls = manifest_doc.read().replace(ucar_site, '').split(',')

    proctype_list = store_proc_levels(manifest_file_path)

    last_url = manifest_urls[-2].split('/')

    if len(last_url) > 1:
        if "spire" in last_url or 'geoopt' in last_url:
            mission = last_url[0]
            last_searched_proctype = last_url[2]
            last_searched_yr = last_url[4]
            last_searched_doy = last_url[5]
        else:
            mission = last_url[0]
            last_searched_proctype = last_url[1]
            last_searched_yr = last_url[3]
            last_searched_doy = last_url[4]

        last_searched_dict = { mission: {
            "last_searched_proctype": last_searched_proctype,
            "last_searched_yr": last_searched_yr,
            "last_searched_doy": last_searched_doy,
            "mission_proctypes": proctype_list
        }}
    else:
        return None

    return last_searched_dict


def create_last_searched_json(manifest_file_path_list):

    manifest_last_searched_dict = {}
    for manifest_file in manifest_file_path_list:
        manifest_last_searched_dict.update(create_last_searched_info(manifest_file))

    print(manifest_last_searched_dict.keys())
    with open("last_searched_info.json", 'w+') as json_file:
        json.dump(manifest_last_searched_dict, json_file)
        path_to_file = json_file.name

    return path_to_file


def check_for_new_ucar_entries(manifest_last_searched_dict):

    new_url_entries = []
    signal_new_proctype_dict = {}

    mission_level_url_list = get_mission_level_urls(ucar_site)
    if len(mission_level_url_list) > len(manifest_last_searched_dict.keys()):
        mission_no_slash_list = [the_mission_link.replace('/', '') for the_mission_link in mission_level_url_list]
        new_missions = set(mission_no_slash_list).difference(set(manifest_last_searched_dict.keys()))
        new_url_entries.extend(list(new_missions))

    for mission in manifest_last_searched_dict.keys():

        if mission == 'spire' or mission == 'geoopt':
            mission_url = os.path.join(ucar_site, mission, 'noaa', '')
            proctype_url = os.path.join(mission_url, manifest_last_searched_dict[mission]['last_searched_proctype'], '')

            new_proc_type_urls = check_new_proctype(mission_url,
                                                    manifest_last_searched_dict[mission]['mission_proctypes'])
            if len(new_proc_type_urls) > 0:
                signal_new_proctype_dict.update({mission: new_proc_type_urls})

            new_year_urls = check_new_proctype_year(proctype_url,
                                                    manifest_last_searched_dict[mission]['last_searched_yr'])
            new_doy_urls = check_new_doy(proctype_url, manifest_last_searched_dict[mission]['last_searched_yr'],
                                         manifest_last_searched_dict[mission]['last_searched_doy'])

            new_url_entries.extend(new_proc_type_urls)
            new_url_entries.extend(new_year_urls)
            new_url_entries.extend(new_doy_urls)

        else:
            mission_url = os.path.join(ucar_site, mission, '')
            proctype_url = os.path.join(mission_url, manifest_last_searched_dict[mission]['last_searched_proctype'], '')

            new_proc_type_urls = check_new_proctype(mission_url, manifest_last_searched_dict[mission]['mission_proctypes'])
            if len(new_proc_type_urls) > 0:
                signal_new_proctype_dict.update({mission: new_proc_type_urls})

            new_year_urls = check_new_proctype_year(proctype_url, manifest_last_searched_dict[mission]['last_searched_yr'])
            new_doy_urls = check_new_doy(proctype_url, manifest_last_searched_dict[mission]['last_searched_yr'],
                                         manifest_last_searched_dict[mission]['last_searched_doy'])

            new_url_entries.extend(new_proc_type_urls)
            new_url_entries.extend(new_year_urls)
            new_url_entries.extend(new_doy_urls)

    if len(signal_new_proctype_dict.keys()) > 0:
        the_curr_date = date_today
        filename = f"signal_new_proc_type_{the_curr_date}.json"
        with open(filename, 'w+') as signal_file:
            json.dump(signal_new_proctype_dict, signal_file)

    return new_url_entries


def check_new_proctype(mission_url, proctype_list):
    """
    Function is used to check if there are new processing types under a ucar mission url using a list of already existing
    processing types for the mission as reference. If a processing type is not in the list of given list of processing
    types, then the processing type url is added to new_url_entries. After all potential processing types are checked the
    function will retrun a list of all new processing type urls, if any have been found.
    :param mission_url:
    :param proctype_list:
    :return new_url_entries:
    """
    new_url_entries = []

    response = requests.get(mission_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    href_list = []
    for a_tag in soup.findAll('a'):
        href_tag_text = a_tag.attrs.get('href')
        href_list.append(href_tag_text)

    for proctype_link in href_list[1:]:
        proctype = proctype_link.split('/')[0]
        if proctype != 'tools' and proctype != 'provisional':
            #print("mission_url: ", mission_url, " | ", proctype)
            if proctype not in proctype_list:
                #print(f"NEW PROC TYPE: {proctype}")
                new_url_entries.append(os.path.join(mission_url, proctype, ''))

    return new_url_entries


def check_new_proctype_year(proctype_url, last_searched_year):
    """
    Function used to check if any new year links exist for a processing type. The function takes a processing type url and
    a last_searched_year or the year that will be used as the floor to determine if any years greater than exist. If an years
    greater than the last_searched_year exist the url will be added to a list and that list will be returned after the search is
    complete
    :param proctype_url:
    :param last_searched_year:
    :return new_url_entries:
    """
    new_url_entries = []

    for level in ['level1b/', 'level2/']:
        url_with_level = os.path.join(proctype_url, level)

        response = requests.get(url_with_level)
        soup = BeautifulSoup(response.text, 'html.parser')

        href_list = []
        for a_tag in soup.findAll('a'):
            href_tag_text = a_tag.attrs.get('href')
            href_list.append(href_tag_text)

        #last_yr_from_url = href_list[-1].split('/')[0]
        for yr_link in href_list[1:]:
            the_yr = yr_link.split('/')[0]
            if int(last_searched_year) < int(the_yr):
                url_with_year = os.path.join(url_with_level, the_yr, '')
                new_url_entries.append(url_with_year)

    return new_url_entries


def check_before_proctype_year(proctype_url, last_searched_year):
    """
    Function used to get year links under the proctype_url before the last_searched_year. Function will return a list of
    year urls less than the input last_searched_year.
    :param proctype_url:
    :param last_searched_year:
    :return new_url_entries:
    """
    new_url_entries = []

    for level in ['level1b/', 'level2/']:
        url_with_level = os.path.join(proctype_url, level)

        response = requests.get(url_with_level)
        soup = BeautifulSoup(response.text, 'html.parser')

        href_list = []
        for a_tag in soup.findAll('a'):
            href_tag_text = a_tag.attrs.get('href')
            href_list.append(href_tag_text)

        #last_yr_from_url = href_list[-1].split('/')[0]
        for yr_link in href_list[1:]:
            the_yr = yr_link.split('/')[0]
            if int(last_searched_year) > int(the_yr):
                url_with_year = os.path.join(url_with_level, the_yr, '')
                new_url_entries.append(url_with_year)

    return new_url_entries


def check_new_doy(proctype_url, last_searched_year, last_searched_doy):
    """
    Function used to check if an new day of year links exist under the input proctype_url using the input last_searched_year
    and the input last_searched_doy. The function will return a list of all day of year urls found that contain a day of year
    greater than the input doy
    :param proctype_url:
    :param last_searched_year:
    :param last_searched_doy:
    :return new_url_entries:
    """
    new_url_entries = []

    for level in ['level1b/', 'level2/']:
        url_with_level = os.path.join(proctype_url, level, last_searched_year, '')

        response = requests.get(url_with_level)
        soup = BeautifulSoup(response.text, 'html.parser')

        href_list = []
        for a_tag in soup.findAll('a'):
            href_tag_text = a_tag.attrs.get('href')
            href_list.append(href_tag_text)

        for doy_link in href_list[1:]:
            doy = doy_link.split('/')[0]
            if int(last_searched_doy) < int(doy):
                url_with_year = os.path.join(url_with_level, doy, '')
                new_url_entries.append(url_with_year)

    return new_url_entries


def check_before_doy(proctype_url, last_searched_year, last_searched_doy):
    """
    Function checks for days of year before the input last_searched_doy under the input proctype_url. If a day of year is
    less than the input doy then it is added to a list of urls containing days of year less than the input
    :param proctype_url:
    :param last_searched_year:
    :param last_searched_doy:
    :return new_url_enetries:
    """
    new_url_entries = []

    for level in ['level1b/', 'level2/']:
        url_with_level = os.path.join(proctype_url, level, last_searched_year, '')

        response = requests.get(url_with_level)
        soup = BeautifulSoup(response.text, 'html.parser')

        href_list = []
        for a_tag in soup.findAll('a'):
            href_tag_text = a_tag.attrs.get('href')
            href_list.append(href_tag_text)

        for doy_link in href_list[1:]:
            doy = doy_link.split('/')[0]
            if int(last_searched_doy) > int(doy):
                url_with_year = os.path.join(url_with_level, doy, '')
                new_url_entries.append(url_with_year)

    return new_url_entries

def download_file(url):
    """
    Function will use the input url (ending in .tar.gz) to download the file at the url to local storage. The function will
    return the path to the file
    :param url:
    :return full_path:
    """
    local_filename = url.split('/')[-1]
    local_root_path = create_local_dir_mirror_ucar(url)
    full_path = os.path.join(local_root_path, local_filename)

    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(full_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)
    print("File downloaded to: ", full_path)
    return full_path


def create_local_dir_mirror_ucar(url):
    """
    Function creates local directories from input url. Function returns the resulting local path
    :param url:
    :return local_path:
    """
    root_path = os.path.split(url.replace(ucar_site, ''))[0]
    local_path = os.path.join(home_path, 'ucar_repo', root_path)

    os.makedirs(local_path, exist_ok=True)

    return local_path


def create_s3_obj_key_file(bucket):
    """
    Function will create an obj key file for the contents of the input s3 bucket. Function will return the location of
    the file.
    :param bucket:
    :return file_loc:
    """
    bucket_obj_list = []
    for my_bucket_object in bucket.objects.all():
        key = my_bucket_object.key
        if "conPhs" in key or "atmPhs" in key or "atmPrf" in key or "wetPrf" in key or "wetPf2" in key:
            bucket_obj_list.append(key)
            print(my_bucket_object.key)

    the_date = date.today().isoformat()
    filename = f"s3_obj_keys_{the_date}.txt"
    with open(filename, 'w') as new_obj_file:
        for key in bucket_obj_list:
            new_obj_file.write(f"{key}\n")
        file_loc = new_obj_file.name

    return file_loc


# To be run on ucar-earth-ro-archive bucket obj_key_file 1/10/2022
def add_zero_pad_doy_to_key_file(s3_obj_key_file_path):
    """
    Function will add zero padding to obj key file entries that do not have proper zero padding
    :param s3_obj_key_file_path:
    :return:
    """
    with open(s3_obj_key_file_path, 'r') as s3_obj_key_file:
        with open("/home/i28373/zero_pad_ucar_objKey.txt", 'w') as new_s3_obj_key_file:
            for line in s3_obj_key_file:
                obj_file_key = check_zero_pad_doy(line.strip())
                new_file_str = obj_file_key + "\n"
                new_s3_obj_key_file.write(new_file_str)
    return


def check_zero_pad_doy(obj_file_key):
    """
    Function checks if obj file keys in obj key file have proper zero pad. If zero pad is incorrect in entry then the function
    will add it to the entry and return a properly zero padded entry. If the entry is correct then the function will return
    the input obj_file_key
    :param obj_file_key:
    :return zero_pad_key or obj_file_key:
    """
    print(f"obj_file_key: {obj_file_key}")
    no_zero_pad_two_digit = re.search("/([0-9][0-9])/", obj_file_key)
    no_zero_pad_one_digit = re.search("/([0-9])/", obj_file_key)
    if no_zero_pad_one_digit != None:
        print(f"no_zero_pad_one_digit: {no_zero_pad_one_digit.groups()}")
        number          = no_zero_pad_one_digit.group(0).split('/')[1].zfill(3)
        replace_str     = f"/{number}/"
        zero_pad_key    = re.sub(r"/([0-9])/", replace_str, obj_file_key)
        return zero_pad_key
    if no_zero_pad_two_digit != None:
        print(f"no_zero_pad_two_digit: {no_zero_pad_two_digit.groups()}")
        number          = no_zero_pad_two_digit.group(0).split('/')[1].zfill(3)
        replace_str     = f"/{number}/"
        zero_pad_key    = re.sub(r"/([0-9][0-9])/", replace_str, obj_file_key)
        return zero_pad_key

    return obj_file_key

"""
    Get initial manifests 
"""

if __name__ == "__main__":
    #fp = "/home/i28373/ucar_webscrape/ucarWebScrapeToS3/ucar_file_manifests_per_mission/cosmic2.txt"

    manifest_root = "/home/i28373/ucar_webscrape/ucarWebScrapeToS3/ucar_file_manifests_per_mission/"
    manifest_file_list = os.listdir(manifest_root)
    file_path_list = []
    for file in manifest_file_list:
        file_path_list.append(os.path.join(manifest_root, file))
    create_last_searched_json(file_path_list)
    #print(store_proc_levels(fp))

    with open("/home/i28373/ucar_webscrape/ucarWebScrapeToS3/last_searched_info.json", 'r+') as the_json_file:
        the_dict = json.load(the_json_file)
    print(the_dict)
    print(check_for_new_ucar_entries(the_dict))
    pass

