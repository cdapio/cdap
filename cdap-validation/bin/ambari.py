# -*- coding: utf-8 -*-
#
# Copyright © 2015 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import urllib2
import json
import re
import glob, os
import test_helpers as helpers

# Start Ambari configuration retrieval process
def get_ambari_configs(host, subdir, base, cluster_info):
    ## get ambari configs
    manager = 'ambari' 
    cu = get_config_urls(host, cluster_info)
    if cluster_info['verbose'] == 2: print 'cu=%s' % (cu)
    config_url_href = cu.read()
    if cluster_info['verbose'] == 2: print 'configurlref=%s' % (config_url_href)

    ## extract the ambari config urls
    data = json.loads(config_url_href)
    config_urls = [ item['href'] for item in data['items'] ]

    ## run API commands:
    ## iterate through ambari config urls, and run api requests against those
    ##   This creates config files in a subdirectory
    get_configs(config_urls, subdir, cluster_info)

    # process tmp configs and store results
    ambari_stored_configs = base[manager]['stored_results']
    store_ambari_results(subdir, ambari_stored_configs, cluster_info)

# get config urls
def get_config_urls(host, cluster_info):
    cluster = cluster_info['cluster']  
    append = '/clusters/' + cluster + '/configurations' 
    url = host + append
    if cluster_info['verbose'] == 2:  print 'url=%s' % (url)
    config_urls = helpers.run_request(url, cluster_info)
    return config_urls

# get configs through config urls
def get_configs(urls, subdir, cluster_info):
    for uri in urls:
        # extract service file name
        m = re.search('\?type=(.+?)&tag', uri)
        file = m.group(1)

        # run api config retrieval command and write result to file
        helpers.get_config_and_write(uri, subdir, file, cluster_info)

# process tmp configs and store results
def store_ambari_results(subdir, stored_configs, cluster):
    # get list of tmp config files and read
    if cluster['verbose'] == 2: print "Get and store all Ambari configurations in %s\n" % (stored_configs)
    s = open(stored_configs, 'w')
    os.chdir(subdir) ## better: capture current directory
    for file in glob.glob('*'):

        # open file
        f = open(file, 'r')
        tmp_config_file = f.read()
        f.close()
        service = get_service_name(file)
        if cluster['verbose'] == 2: print 'service=%s' % (service)

        # read json
        data = json.loads(tmp_config_file)

        # extract properties
        try:
            properties = [ items['properties'] for items in data['items']]
        except KeyError:
            pass # we need to do this for config files with no properties

        # get the properties we want and parse
        for key, value in properties[0].iteritems():
            if key == 'content': ### filter out properties with key = 'content'
                continue
            s.write('%s:%s=\'%s\'\n' % (service, key, value))
        
    # return
    os.chdir('..') ## returned to captured current directory above

def get_service_name(service_config_file):
    # take file name, extract first part (before first '-') treat that as the service
    # extra the part before the '-' and return it
    service = service_config_file.split('-')[0]
    if service_config_file == 'zoo.cfg':
        service = 'zookeeper'
    return service

