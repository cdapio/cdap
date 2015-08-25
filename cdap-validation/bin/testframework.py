#!/usr/bin/python
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
import test_helpers as helpers
import json
import re
from subprocess import call
import ambari
import cloudera

##### TEST FRAMEWORK FUNCTIONS #####

input_json = 'input.json'

# calls all other functions
def testing(base_vars,cluster_vars):
    if cluster_vars['verbose'] == 2: print 'start testing'

    # prep input variables
    #cluster_vars = {}
    input = read_input(input_json)
    cluster_vars['host'] = input['params']['uri']
    cluster_vars['username'],cluster_vars['password']= input['params']['userpass'].split(':')
    cluster_vars['cluster'] = input['params']['cluster_name']
    cluster_vars['modules'] = input['params']['modules']

    if cluster_vars['verbose'] == 2:
        for k,v in cluster_vars.iteritems():
            print 'cluster vars: %s=%s' % (k,v)

    # detect_installed_services -- placeholder for future function

    # prep the url
    find_api='/api'
    if cluster_vars['host'].find(find_api) == -1:
        cluster_vars['host'] += 'api/'
    if cluster_vars['verbose'] == 2:  print 'uri=%s' % (cluster_vars['host'])

    # validate_connection
    if cluster_vars['verbose'] == 2:  print 'Validating connection'
    api_tests = [{'cloudera': base_vars['cloudera']['api_test']},{'ambari': base_vars['ambari']['api_test']}]
    version_test = validate_connection(base_vars, cluster_vars, api_tests)
    if cluster_vars['verbose'] == 2:  print ''
    if cluster_vars['verbose'] == 2:  print 'version_test=%s' % (version_test)

    # now we determine the install manager from the info we have
    if cluster_vars['verbose'] == 2:  print 'Determine Hadoop install manager'
    install_manager = get_install_manager(version_test)
    cluster_vars['manager'] = install_manager
    if cluster_vars['verbose'] == 2:  print 'Hadoop install manager is %s' % (install_manager)

    # get version for API call
    if install_manager == 'cloudera':
        cluster_vars['version'] = version_test
    else:
        cluster_vars['version'] = 'v1'
    
    cluster_vars['base_url'] = cluster_vars['host'] + cluster_vars['version']
    
    # get preliminary info
    cluster_vars = get_install_manager_info(cluster_vars)
      # do we need these (below) or can we just work with the dict values?
    if cluster_vars['verbose'] == 2:
        for key, value in cluster_vars.iteritems():
            print "%s=%s" % (key, value)

    # get and run commands (API commands for now)
    get_and_run_api_commands(base_vars, cluster_vars)

    #########################################

    ##### MODULES #####
    
    # find_modules
    # navigate through ./module/* directories and look for module.json files
    # parse through those to determine modules that will be run
    # save (follow design)

    if cluster_vars['verbose'] == 2:  print 'verbose=%s' % (cluster_vars['verbose'])
    if cluster_vars['verbose'] == 2:  print '\nfind modules:\n'

    module_list = find_modules('module.json', 'modules')
    if cluster_vars['verbose'] == 2:
        for module in module_list:
            print 'module = %s' % (module)

    modules = AutoVivification()
    module_name_list = []
   
    for module in module_list:
        m = re.search('/(.+?)/', module)
        if m:
            module_name = m.group(1)
            modules[module_name] = create_module_json(module_list)
            module_name_list.append(module_name)

    if cluster_vars['verbose'] == 2: print modules

    # run modules
    # execute modules: for every known module, run specific functions (see module_functions)
    if cluster_vars['verbose'] == 2: print '\nrun modules:\n'
    run_modules(base_vars, cluster_vars, modules, module_name_list, install_manager) 
    # send params necessary for module to run
    # e.g. config_validator needs to know location of the base ref and stored results files

###########################################################################################
# reads JSON object passed to the framework
def read_input(input_file):
    with open(input_file) as json_file:
        data = json.load(json_file)
    return data

def find_all(name, path):
    result = []
    for root, dirs, files in os.walk(path):
        if name in files:
            result.append(os.path.join(root, name))
    return result

def find_modules(file, path):
    results = find_all(file, path)
    return results

###def detect_installed_services
# coming soon

def validate_connection(base_info, cluster_info, api_tests):
# test api connection, etc.
    version = test_api_connection(base_info, cluster_info, api_tests)
    # placeholder for testing other connections
    return version

def test_api_connection(base_info, cluster_info, api_tests):
    if cluster_info['verbose'] == 2: print 'test api connection'
    # let us iterate through potential hadoop manager tests and see what we get back
    # the first one that works should be saved and set from that point on for the rest of the testing 
    host_url = cluster_info['host']
    for hash in api_tests:
        if cluster_info['verbose'] == 2: print ''
        if cluster_info['verbose'] == 2: print hash
        for manager, test in hash.iteritems():
            mgr_test_url = host_url + test
            #print "manager=%s, test=%s, mgr_test_url=%s" % (manager, test, mgr_test_url)
            if cluster_info['verbose'] == 2: print 'Running onetime_auth'
            helpers.onetime_auth(mgr_test_url,cluster_info) # set up password manager and install opener
            if cluster_info['verbose'] == 2: print 'run_request'
            h = helpers.run_request(mgr_test_url,cluster_info) # run url request
            # if it equals noapi, it means the api login test failed for that install manager
            if h == 'noapi':
                if cluster_info['verbose'] == 2: print 'No API for %s, trying the next or exiting' % (manager)
                break
            mgr_test = h.read()
            # add logic to do next iteration or not, depending on whether we succeeded on first one
            # if good: create array or hash with manager and mgr_test and return that?
            if mgr_test == 'v10':
                return mgr_test
                break
        else:
            break

def get_install_manager(version):
    if version == 'v10':
        manager = 'cloudera'
    else:
        manager = 'ambari'

    return manager

# get static info needed to connect to a specific API
def get_install_manager_info(cluster_info):
    my_cluster = '/clusters/' + cluster_info['cluster']
    cluster_info['my_cluster'] = my_cluster
    base_url = cluster_info['host']
    base_url += cluster_info['version']
    if cluster_info['verbose'] == 2: print 'base_url=%s' % (base_url)
    cluster_info['base_url'] = base_url
    return cluster_info

# get (and run) API commands
def get_and_run_api_commands(base,cluster):
    if cluster['verbose'] == 2: print 'Now running API commands'
    mgr = cluster['manager']
    host_url = cluster['base_url']
    username = cluster['username']
    password = cluster['password']
    configs_subdir = base[mgr]['subdir']
    my_cluster = cluster['my_cluster']
     
    if mgr == 'cloudera':
        print 'Hadoop install manager: Cloudera Manager'
        #cloudera.cloudera_commands(host_url, configs_subdir, base, cluster) ## disabling for now
    
    elif mgr == 'ambari':
        if cluster['verbose'] ==2: print 'Hadoop install manager: Ambari'
        ambari.ambari_commands(host_url, configs_subdir, base, cluster)
    
    else:
        print 'Your Hadoop install manager is not recognized'

def create_module_json(modules):
    module_info = read_input(modules[0])
    return module_info 

def run_modules(base,cluster,modules,name_list,mgr):
    if cluster['verbose'] == 2:
        print 'run modules'
        print name_list

    for module_name in name_list:
        command = ''
        full_command = ''
        command = modules[module_name]['groups'][0]['command']  
        params = base[mgr]['baseref'] + ' ' + base[mgr]['stored_results'] + ' ' + str(cluster['verbose'])
        if cluster['verbose'] == 2: print modules
        full_command = 'modules/' + command + ' ' + params
        if cluster['verbose'] == 2: print full_command
        os.system(full_command)

def process_runtime_errors():
    print "process runtime errors"

def clean_up():
# note: make note of temp and dirs and files we create
    print "clean up"

class AutoVivification(dict):
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

