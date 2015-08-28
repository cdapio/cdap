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

import os
import base64
import urllib2
import json
import sys
import ambari
import cloudera


def usage():
    print """
                              CDAP Validation Tool

    Its purpose is to check A Hadoop or CDAP cluster for its CDAP readiness.
    It is designed in a modular way (validation modules are pluggable).
    Configurations, service checks, etc are gathered via the Hadoop Manager's API.
    These are stored and made available to the validation modules.
    Three pluggable modules are provided with this tool:
    * Configuration Validation
    * Service Checks -- coming soon
    * Cluster Layout checks -- coming soon
 """ 
    print 'Usage: \n'+sys.argv[0]+' [-v, --verbose | -d] -c, --cluster \'<cluster>\' [-m, --modules \'<modules>]\' -u --user <user:password> -U --uri <URI>\n'

    print """
    -h, --help  help menu
    -c, --cluster       cluster name used in API calls to the install manager
    -m, --modules       Run plugabble test modules, separated by ';'.  E.g. -m 'module1;module2'
                        Each module is a self-contained unit under the modules subdirectory
                        * config=configuration validation
                        * service=service checks
                        * layout=cluster layout checks
    -u, --user          Authentication, e.g. --user 'george:pass'
    -U, --uri           URI: e.g. --uri 'http://10.240.0.8:7180'
    -v, --verbose       Adds more output (shows successful checks)
    -d, --debug         Full debug output of validation runs (if both verbose and debug are used, the last one wins)
 """


# function used to (only) print certain content when verbose is set to highest level (e.g. debug)
def vprint(content, verbose):
    if verbose == 2:
        print content


def onetime_auth(host, info):
    # this should work with all, but when authentication is not set up correctly, this fails (http 403)
    passman = urllib2.HTTPPasswordMgrWithDefaultRealm()  # this creates a password manager
    passman.add_password(None, host, info['username'], info['password'])
    authhandler = urllib2.HTTPBasicAuthHandler(passman)  # create the AuthHandler
    opener = urllib2.build_opener(authhandler)
    urllib2.install_opener(opener)
    return urllib2


def run_request(host, info):
    user = info['username']
    passwd = info['password']
    try:
        handle = urllib2.urlopen(host)
        return handle
    except:  #except HTTPError, h: ### would like to do something like this
        # try a different way (encoding headers)
        req = urllib2.Request(host)
        base64string = base64.encodestring('%s:%s' % (user, passwd))[:-1]
        authheader = 'Basic %s' % base64string
        req.add_header('Authorization', authheader)
        try:
            handle = urllib2.urlopen(req)
            return handle
        except IOError:
            # here we shouldn't fail if the username/password is right
            vprint('It looks like the username or password is wrong.', info['verbose'])
            return 'noapi'


def write_file(config, dir, file, createdir):
    path = dir + file
    if not os.path.exists(dir):
        if createdir is True:
            os.makedirs(dir)
        else:
            print 'Directory missing. Unable to write configurations. Exiting.'
            exit(1)
    f = open(path, 'w')
    f.write(config)
    f.close()


# The following function is needed for Cloudera Manager and other configs that require config API calls based on lists
#   of services, retrieved through another API call.
# In order to programmatically retrieve certain configurations via API, Cloudera manager requires at least two
#   and in many cases three or four API calls, each dependent on information retrieved in the previous one or ones.
#
# Here is an example that only requires three API calls, to obtain a specific configuration file
# To get the contents of the hdfs-site.xml on the namenode, we would need to do:
#  * GET /api/v10/cm/deployment
#  * Find the namenode value e.g. hdfs-NAMENODE-cd4bc7dac120e30f653e076328de207d, in the config obtained with previous call
#  * GET /api/v10/clusters/<cluster>/serviceTypes
#  * iterate through the list of services (to verify service exists) from the serviceTypes call, and for each service:
#    - Using the value obtained from the first API call and from the second API call, run an API call that would look like:
#    GET /api/v10/clusters/<cluster>/services/hdfs/roles/hdfs-NAMENODE-cd4bc7dac120e30f653e076328de207d/process/configFiles/hdfs-site.xml
#
# To find and iterate through all configurations needed, we need to make the following API calls and extraction
#  * GET /api/v10/cm/deployment
#  * GET /api/v10/clusters/<cluster>/serviceTypes
#  * then for every service found in serviceType:
#    - GET /api/v10/clusters/<cluster>/services/<service>/config?view=full
#    - GET /api/v10/clusters/<cluster>/services/<service>/roleTypes
#    - then for every role found in roleTypes find and make all API calls that use roles (such as the example above)
def convert_types_to_list(types):
    data = json.loads(types)
    services = []
    upper_services = [item for item in data['items']]
    for service in upper_services:
        services.append(service.lower())
    services.sort()
    return services


def get_config_and_write(url, subdir, file, cluster_info):
    # run api config retrieval commands and write to individual files
    config_in = run_request(url, cluster_info)
    config = config_in.read()
    create_directory_if_missing = True
    write_file(config, subdir, file, create_directory_if_missing)


def safe_get_config_and_write(url, user, passwd, subdir, file):
    # run api config retrieval commands and write to individual files (safer)
    config_in = ''
    try:
        config_in = urllib2.urlopen(url)
        config = config_in.read()
        create_directory_if_missing = True
        write_file(config, subdir, file, create_directory_if_missing)
    except:
        # need to find a good way to ignore API calls that return nothing (for unused services)
        pass


def get_config_from_managermgr(mgr, host_url, configs_subdir, base, cluster):
    if mgr == 'cloudera':
        vprint('Hadoop install manager: Cloudera Manager', cluster['verbose'])
        # cloudera.get_cloudera_configs(host_url, configs_subdir, base, cluster) ## disabling for now

    elif mgr == 'ambari':
        vprint('Hadoop install manager: Ambari', cluster['verbose'])
        ambari.get_ambari_configs(host_url, configs_subdir, base, cluster)

    else:
        print 'Your Hadoop install manager is not recognized'
