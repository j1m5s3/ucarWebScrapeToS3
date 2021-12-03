import os
import requests
import logging
from bs4 import BeautifulSoup


LOGGER = logging.getLogger(__name__)

ucar_urls = []


def check_if_correct_level(new_url):

    excluded_criteria = ['level0/', 'level1a/']
    if new_url.endswith("/"):
        for excluded_level in excluded_criteria:
            if excluded_level in new_url:
                return False
        return True

    return False


def check_if_correct_filetype(new_url):

    filetype_list = ['conPhs', 'atmPhs', 'atmPrf', 'wetPrf', 'wetPf2']
    if new_url.endswith(".tar.gz"):
        for filetype in filetype_list:
            if filetype in new_url:
                return True

    return False


def recursive_scrape(url):

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    href_list = []
    for a_tag in soup.findAll('a'):
        href_tag_text = a_tag.attrs.get('href')
        href_list.append(href_tag_text)

    for link in href_list[1:]:

        new_url = os.path.join(url, link)
        if check_if_correct_level(new_url):
            if new_url not in ucar_urls:
                LOGGER.info(new_url)
                recursive_scrape(new_url)
        if check_if_correct_filetype(new_url):
            if new_url not in ucar_urls:
                LOGGER.info(new_url)
                ucar_urls.append(new_url)

    return
