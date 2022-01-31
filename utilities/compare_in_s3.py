import os
import logging
from .ucar_repo_status import check_if_correct_level, check_if_correct_filetype


LOGGER = logging.getLogger(__name__)

local_s3_obj_key_file = "/home/i28373/zero_pad_ucar_objKey.txt"
ucar_site = "https://data.cosmic.ucar.edu/gnss-ro/"


def compare_against_obj_key_file(to_download_list):
    """
    Function uses an input list of urls (ending in .tar.gz) to compare against a list of s3 obj_file_keys to find what is
    not in the list of s3 keys and returns a list of those entries
    :param to_download_list:
    :return c:
    """
    with open(local_s3_obj_key_file, 'r') as the_s3_obj_file:
        obj_file_content = the_s3_obj_file.read().split('\n')

    #with open(filepath, 'r') as the_ucar_inventory_file:
    #    the_ucar_inventory = the_ucar_inventory_file.read().split(',')

    the_ucar_keys = []
    for the_item in to_download_list:
        if len(the_item.split(ucar_site)) == 2:
            the_ucar_keys.append(the_item.split(ucar_site)[1])

    the_obj_file_keys = []
    for the_item in obj_file_content:
        #obj_doy_key = os.path.join(os.path.split(the_item)[0], '')
        #print(the_item, '--->', obj_doy_key)
        #if mission in the_item:
        if check_if_correct_level(the_item):
            if check_if_correct_filetype(the_item):
                if the_item not in the_obj_file_keys:
                    the_obj_file_keys.append(the_item)

    a = set(the_ucar_keys)
    b = set(the_obj_file_keys)
    c = a.difference(b)
    #with open('diff.txt', 'a') as diff_txt:
    #    diff_txt.write(f"{mission}---------------------------------------\n")
    #    diff_txt.write(f"{c}, \n")

    LOGGER.info("SET DIFFERENCES ----------------------------------------")
    LOGGER.info(c)

    return list(c)


def get_obj_key_file_list(mission):

    #bucket_url = 's3://ucar-earth-ro-archive/'
    with open(local_s3_obj_key_file, 'r') as the_s3_obj_file:
        obj_file_content = the_s3_obj_file.read().split('\n')

    the_obj_file_keys = []
    for the_item in obj_file_content:
        if check_if_correct_level(the_item):
            if the_item not in the_obj_file_keys and mission in the_item:
                #the_obj_file_keys.append(os.path.join(bucket_url, the_item))
                the_obj_file_keys.append(the_item)

    return the_obj_file_keys


def get_ucar_file_url_list(filepath):

    with open(filepath, 'r') as the_ucar_inventory_file:
        #the_ucar_inventory = the_ucar_inventory_file.read().split(',')
        the_ucar_inventory = the_ucar_inventory_file.read().split(',')[:-1]

    #the_ucar_urls = []
    #for the_item in the_ucar_inventory:
    #    if len(the_item.split(ucar_site)) == 2:
    #        the_ucar_urls.append(the_item)

    #return the_ucar_urls
    return the_ucar_inventory

