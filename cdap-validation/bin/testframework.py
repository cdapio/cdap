#!/usr/bin/python
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
import test_helpers as helpers
import json
import re
import ambari
# import cloudera

##### TEST FRAMEWORK FUNCTIONS #####

input_json = 'input.json'


# calls all other functions
def test(base_vars, cluster_vars):
    helpers.vprint('start testing', cluster_vars['verbose'])

    # prep input variables
    # cluster_vars = {}
    input = read_file_as_json(input_json)
    cluster_vars['host'] = input['params']['uri']
    cluster_vars['username'] = input['params']['user']
    cluster_vars['password'] = input['params']['password']
    cluster_vars['cluster'] = input['params']['cluster_name']
    cluster_vars['modules'] = input['params']['modules']
    verbose = cluster_vars['verbose']

    # confirm that all cluster_vars key/value pairs include something
    # if they do not (other than modules param, throw an error)
    # iterate through keys and verify

    for k, v in cluster_vars.iteritems():
        helpers.vprint('cluster vars: %s=%s' % (k, v), verbose)

    # detect_installed_services -- placeholder for future function

    # prep the url
    find_api = '/api'
    if cluster_vars['host'].find(find_api) == -1:
        cluster_vars['host'] += 'api/'
    helpers.vprint('uri=%s' % (cluster_vars['host']), verbose)

    # validate_connection
    helpers.vprint('Validating connection', verbose)
    api_tests = [{'cloudera': base_vars['cloudera']['api_test']}, {'ambari': base_vars['ambari']['api_test']}]
    version_test = validate_connection(base_vars, cluster_vars, api_tests)
    helpers.vprint('', verbose)
    helpers.vprint('version_test=%s' % (version_test), verbose)

    # now we determine the install manager from the info we have
    helpers.vprint('Determine Hadoop install manager', verbose)
    install_manager = get_install_manager(version_test)
    cluster_vars['manager'] = install_manager
    helpers.vprint('Hadoop install manager is %s' % (install_manager), verbose)

    # get version for API call
    if install_manager == 'cloudera':
        cluster_vars['version'] = version_test
    else:
        cluster_vars['version'] = 'v1'

    cluster_vars['base_url'] = cluster_vars['host'] + cluster_vars['version']

    # get preliminary info
    cluster_vars = get_install_manager_info(cluster_vars)
    # do we need these (below) or can we just work with the dict values?
    for key, value in cluster_vars.iteritems():
        helpers.vprint("%s=%s" % (key, value), verbose)

    # get and run commands (API commands for now)
    get_and_run_api_commands(base_vars, cluster_vars)

    #########################################

    ##### MODULES #####

    # find_modules
    # navigate through ./module/* directories and look for module.json files
    # parse through those to determine modules that will be run
    # save (follow design)

    helpers.vprint('verbose=%s' % (verbose), verbose)
    helpers.vprint('\nfind modules:\n', verbose)

    module_list = find_modules('module.json', 'modules')
    for module in module_list:
        helpers.vprint('module = %s' % (module), verbose)

    modules = AutoVivification()
    module_name_list = []

    for module in module_list:
        m = re.search('/(.+?)/', module)
        if m:
            module_name = m.group(1)
            helpers.vprint ('module_name=%s' % (module_name), verbose)
            module_name_list.append(module_name)
            helpers.vprint ('module_name_list=%s' % (module_name_list), verbose)
        else:
            # throw error (no plugin modules found)
            print 'no plugin modules found'

    helpers.vprint ('\niterate through list of module names', verbose)
    module_path = 'modules/' 
    helpers.vprint ('module_path=%s' % (module_path), verbose)
    for module_name in module_name_list:
        helpers.vprint ('module_name=%s' % (module_name), verbose)
        modules[module_name] = create_module_json(module_name, module_path, cluster_vars)

    helpers.vprint(modules, verbose)

    # run modules
    # execute modules: for every known module, run specific functions (see module_functions)
    helpers.vprint('\nrun modules:\n', verbose)
    run_modules(base_vars, cluster_vars, modules, module_name_list, install_manager)
    # send params necessary for module to run
    # e.g. config_validator needs to know location of the base ref and stored results files


###########################################################################################
# reads JSON object passed to the framework
def read_file_as_json(input_file):
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

# def detect_installed_services()
# coming soon


def validate_connection(base_info, cluster_info, api_tests):
    # test api connection, etc.
    version = test_api_connection(base_info, cluster_info, api_tests)
    # placeholder for testing other connections
    return version


def test_api_connection(base_info, cluster_info, api_tests):
    verbose = cluster_info['verbose']
    helpers.vprint('test api connection', verbose)
    # let us iterate through potential hadoop manager tests and see what we get back
    # the first one that works should be saved and set from that point on for the rest of the testing
    host_url = cluster_info['host']
    for hash in api_tests:
        helpers.vprint('', verbose)
        helpers.vprint(hash, verbose)
        for manager, test in hash.iteritems():
            mgr_test_url = host_url + test
            helpers.vprint('manager=%s, test=%s, mgr_test_url=%s' % (manager, test, mgr_test_url), verbose)
            helpers.vprint('Running onetime_auth', verbose)
            helpers.onetime_auth(mgr_test_url, cluster_info)  # set up password manager and install opener
            helpers.vprint('run_request', verbose)
            h = helpers.run_request(mgr_test_url, cluster_info)  # run url request
            # if it equals noapi, it means the api login test failed for that install manager
            if h == 'noapi':
                helpers.vprint('No API for %s, trying the next or exiting' % (manager), verbose)
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
    helpers.vprint('base_url=%s' % (base_url), cluster_info['verbose'])
    cluster_info['base_url'] = base_url
    return cluster_info


# get (and run) API commands
def get_and_run_api_commands(base, cluster):
    helpers.vprint('Now running API commands', cluster['verbose'])
    mgr = cluster['manager']
    host_url = cluster['base_url']
    configs_subdir = base[mgr]['subdir']
    helpers.get_config_from_managermgr(mgr, host_url, configs_subdir, base, cluster)


def create_module_json(module_name, module_path, cluster):
    verbose = cluster['verbose']
    helpers.vprint ('\nrunning create_module_json function', verbose)
    helpers.vprint ('module name: %s    module path: %s' % (module_name, module_path), verbose)
    module_file_path = module_path + module_name + '/module.json'
    helpers.vprint ('module_file_path=%s' % (module_file_path), verbose)
    module_info = read_file_as_json(module_file_path)
    return module_info


def run_modules(base, cluster, modules, name_list, mgr):
    verbose = cluster['verbose']
    helpers.vprint('run modules', verbose)
    helpers.vprint(name_list, verbose)

    # note: we want to modify this to only pass processed stored configurations
    #       baseref_configs will only be used by config validator module
    for module_name in name_list:
        command = ''
        full_command = ''
        # command should have the format: <module name>/<command path>
        command = module_name + '/' + modules[module_name]['groups'][0]['command']
        params = base[mgr]['baseref'] + ' ' + base[mgr]['stored_results'] + ' ' + str(cluster['verbose'])
        helpers.vprint(modules, verbose)
        # full command should have the format: modules/<module name>/<command path> <params>
        full_command = 'modules/' + command + ' ' + params
        helpers.vprint(full_command, verbose)
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
