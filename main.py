import logging
from utilities.ucar_repo_status import recursive_scrape, ucar_urls


logging.basicConfig(level=logging.INFO,
                    format='%(levelname)-2s [%(filename)s:%(lineno)d] %(message)s',
                    handlers=[
                        logging.FileHandler(filename='log.txt', mode='w+'),
                        #logging.StreamHandler(sys.stdout)
                    ])
LOGGER = logging.getLogger(__name__)

ucar_site = "https://data.cosmic.ucar.edu/gnss-ro/"


def test_ucar_site_drill_down():

    recursive_scrape(ucar_site)
    print(ucar_urls)

    return


if __name__ == '__main__':
    test_ucar_site_drill_down()
    pass
