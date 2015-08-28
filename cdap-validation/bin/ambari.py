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
from os import listdir
from os.path import isfile
from os.path import join
import test_helpers as helpers


# Start Ambari configuration retrieval process
def get_ambari_configs(host, subdir, base, cluster_info):
    # get ambari configs
    manager = 'ambari'
    verbose = cluster_info['verbose']
    cu = get_ambari_config_urls(host, cluster_info)
    # and do the same thing for service check URLs ==> make these more generic so they can be reused
    helpers.vprint('cu=%s' % (cu), verbose)
    config_url_href = cu.read()
    helpers.vprint('configurlref=%s' % (config_url_href), verbose)

    # extract the ambari config urls
    data = json.loads(config_url_href)
    config_urls = [item['href'] for item in data['items']]

    # run API commands:
    # iterate through ambari config urls, and run api requests against those
    #     This creates config files in a subdirectory
    get_ambari_service_configs(config_urls, subdir, cluster_info)

    # process tmp configs and store results
    ambari_stored_configs = base[manager]['stored_results']
    store_ambari_results(subdir, ambari_stored_configs, cluster_info)


# get Ambari config urls
def get_ambari_config_urls(host, cluster_info):
    cluster = cluster_info['cluster']
    append = '/clusters/' + cluster + '/configurations'
    url = host + append
    helpers.vprint('url=%s' % (url), cluster_info['verbose'])
    ambari_config_urls = helpers.run_request(url, cluster_info)
    return ambari_config_urls


# get Ambari services urls
def get_ambari_service_urls(host, cluster_info):
    cluster = cluster_info['cluster']
    append = '/clusters/' + cluster + '/services'
    url = host + append
    helpers.vprint('url=%s' % (url), cluster_info['verbose'])
    ambari_service_urls = helpers.run_request(url, cluster_info)
    return ambari_service_urls


# get Ambari configs through config urls
def get_ambari_service_configs(urls, subdir, cluster_info):
    for uri in urls:
        # extract service file name
        m = re.search('\?type=(.+?)&tag', uri)
        file = m.group(1)

        # run api config retrieval command and write result to file
        helpers.get_config_and_write(uri, subdir, file, cluster_info)


# process tmp configs and store results
def store_ambari_results(subdir, stored_configs, cluster):
    # get list of tmp config files and read
    verbose = cluster['verbose']
    helpers.vprint('Get and store all Ambari configurations in %s\n' % (stored_configs), verbose)
    s = open(stored_configs, 'w')
    config_file_list = [c for c in listdir(subdir) if isfile(join(subdir, c))]

    for file in config_file_list:

        # open file
        subfile = subdir + file
        helpers.vprint('file=%s  subfile=%s' % (file, subfile), verbose)
        try:
            f = open(subfile, 'r')
        except IOError:
            print 'cannot open config file', subfile  # runtime error -- need to stop this modules' run
            exit(1)  # replace with functionality to end module's run (but with the chance to start another module if applicable)

        tmp_config_file = f.read()
        f.close()
        service = get_service_name(file)
        helpers.vprint('service=%s' % (service), verbose)

        # read json
        data = json.loads(tmp_config_file)

        # extract properties
        try:
            properties = [items['properties'] for items in data['items']]
        except KeyError:
            pass  # we need to do this for config files with no properties

        # get the properties we want and parse
        for key, value in properties[0].iteritems():
            if key == 'content':  ### filter out properties with key = 'content'
                continue
            s.write('%s:%s=\'%s\'\n' % (service, key, value))

    # return


def get_service_name(service_config_file):
    # take file name, extract first part (before first '-') treat that as the service
    # extra the part before the '-' and return it
    service = service_config_file.split('-')[0]
    if service_config_file == 'zoo.cfg':
        service = 'zookeeper'
    return service
