import requests
import argparse
import time
import json
import StringIO
import gzip
import pickle
import os
#import boto3
from bs4 import BeautifulSoup
from itertools import islice, chain
from multiprocessing import Process
from multiprocessing import Pool
import sys
reload(sys)
sys.setdefaultencoding('utf8')

# parse the command line arguments
ap = argparse.ArgumentParser()
ap.add_argument("-d","--domain", required=True, help="The domain to target ie. youtube.com")
ap.add_argument("-o","--output_folder", required=True, help="The folder where files would be output")
ap.add_argument("-p","--parallel_threads", required=True, help="Enable parallelisation and set the number of parallel threads")
args = vars(ap.parse_args())

domain = args['domain']
output_folder = args['output_folder']
parallel_threads = args['parallel_threads']

### Path from https://stackoverflow.com/questions/44509423/python-requests-chunkedencodingerrore-requests-iter-lines
import httplib

def patch_http_response_read(func):
    def inner(*args):
        try:
            return func(*args)
        except httplib.IncompleteRead, e:
            return e.partial
    return inner

httplib.HTTPResponse.read = patch_http_response_read(httplib.HTTPResponse.read)


### -----------------------
### Searches the Common Crawl Index for a domain.
### -----------------------
def search_domain(domain):
    record_list = []
    #print "[*] Target domain: %s" % domain
    index = "2018-09"

    #print "[*] Trying index %s" % index
    cc_url  = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
    cc_url += "url=%s&matchType=domain&output=json" % domain.strip() # this allows the crawl of the whole domain from which the URL comes from
    #cc_url += "url=%s&matchType=exact&output=json" % domain #this allows *exact* match of URL

    response = requests.get(cc_url)

    if response.status_code == 200:
        records = response.content.splitlines()
        for record in records:
            record_list.append(json.loads(record))
            #print "[*] Added %d results." % len(records)
    #print "[*] Found a total of %d hits." % len(record_list)
    print "{}\t{}".format(domain.strip(), len(record_list))
    filepath = output_folder + domain.strip()
    file = open(filepath, "w")
    file.write("{}\t{}".format(domain.strip(), len(record_list)))
    file.close()

### -----------------------
###     Main Function
### -----------------------
def main():
    #print("Starting CommonCrawl Search")
    #Finds all relevant domains

    if parallel_threads==0:
        #the input is a domain, not a file with a list of domains
        search_domain(domain)
    else:
        #domain is a pointer to a file containing the list of domains to source, one per line
        pool = Pool(processes=int(parallel_threads))
        with open(domain) as f:
            domain_list = f.readlines()
        pool.map(search_domain, domain_list)


if __name__ == '__main__':
    main()
    #Fin
