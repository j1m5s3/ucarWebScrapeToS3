# ucarWebScrapeToS3

Web scraping utility written for NASA ACCESS project for keeping AER s3 bucket up to date with ucar repo

Required Dependencies:

    Base:
            * python 3.8
    
    External Libs:
            * beautifulsoup4==4.10.0
            * boto3==1.20.26
            * numpy==1.21.5
            * requests==2.26.0

To install run "pip install -r requirements.txt"

In order to run ensure that an up to date policies.json file exists under the **/ucarWebScrapeToS3 project directory.
Additionally ensure that there is a directory named ucar_file_manifests_per_mission exists for the storage of 
the text files created during runtime that will contain the urls to the .tar.gz file that are to be downloaded.

The flow of the program will follow as such...

open policies.json --> collect search urls based on policies --> recursive search using collected urls --> 
compare urls found with what exists in s3 --> Download what is not in s3 
