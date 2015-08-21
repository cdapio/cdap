#!/usr/bin/python

import test_helpers as helpers
import json
import ambari
import cm

##### TEST FRAMEWORK FUNCTIONS #####

input_json = "input.json"

# calls all other functions
def testing(base_vars):
    print "start testing"

    # prep input variables
    cluster_vars = {}
    input = read_input(input_json)
    cluster_vars['host'] = input['params']['uri']
    cluster_vars['username'],cluster_vars['password']= input['params']['userpass'].split(':')
    cluster_vars['verbose'] = input['params']['verbose']
    cluster_vars['cluster'] = input['params']['cluster_name']
    cluster_vars['modules'] = input['params']['modules']

    # find_modules
    # hold on for now

    # detect_installed_services
    # hold on for now

    # prep the url
    find_api='/api'
    if cluster_vars['host'].find(find_api) == -1:
        cluster_vars['host'] += 'api/'
    print 'uri=%s' % (cluster_vars['host'])

    # validate_connection
    print "validate connection"
    api_tests = [{'cm': base_vars['cm']['api_test']},{'ambari': base_vars['ambari']['api_test']}]
    version_test = validate_connection(base_vars, cluster_vars, api_tests)
    print ""
    print 'version_test=%s' % (version_test)

    # now we determine the install manager from the info we have
    print "determine install manager"
    install_manager = get_install_manager(version_test)
    cluster_vars['manager'] = install_manager
    print 'install manager is %s' % (install_manager)

    # get version for API call
    if install_manager == 'cm':
        cluster_vars['version'] = version_test
    else:
        cluster_vars['version'] = 'v1'
    
    cluster_vars['base_url'] = cluster_vars['host'] + cluster_vars['version']
    
    # get preliminary info
    cluster_vars = get_install_manager_info(base_vars, cluster_vars, install_manager)
      # do we need these (below) or can we just work with the dict values?
    for key, value in cluster_vars.iteritems():
        print "%s=%s" % (key, value)

    # get and run commands (API commands for now)
    get_commands(base_vars, cluster_vars)

    #########################################

    ##### MODULES #####
    
    # identify and run modules


###########################################################################################
# reads JSON object passed to the framework
def read_input(input_file):
    with open(input_file) as json_file:
        data = json.load(json_file)
    return data

###def find_modules
# navigate through ./module/* directories and look for module.json files
# parse through those to determine modules that will be run
# save (follow design)

###def detect_installed_services
# coming soon

def validate_connection(base_info, cluster_info, api_tests):
# test api connection, etc.
    version = test_api_connection(base_info, cluster_info, api_tests)
    #print version
    return version

def test_api_connection(base_info, cluster_info, api_tests):
    print "test api connection"
    # let us iterate through potential hadoop manager tests and see what we get back
    # the first one that works should be saved and set from that point on for the rest of the testing 
    host_url = cluster_info['host']
    for hash in api_tests:
        print ""
        print hash
        for manager, test in hash.iteritems():
            mgr_test_url = host_url + test
            print "manager=%s, test=%s, mgr_test_url=%s" % (manager, test, mgr_test_url)
            print "running onetime_auth"
            helpers.onetime_auth(mgr_test_url,cluster_info) # set up password manager and install opener
            print "run_request"
            h = helpers.run_request(mgr_test_url,cluster_info) # run url request
            # if it equals noapi, it means the api login test failed for that install manager
            if h == 'noapi':
                print 'no api for %s, trying the next or exiting' % (manager)
                break
            else:
                print '%s\'s api may be reachable' % (manager)
            mgr_test = h.read()
            # add logic to do next iteration or not, depending on whether we succeeded on first one
            # if good: create array or hash with manager and mgr_test and return that?
            if mgr_test == 'v10':
                return mgr_test
                break
        else:
            break

def get_install_manager(version):
    if version == "v10":
        manager = 'cm'
    else:
        manager = 'ambari'

    return manager

# get static info needed to connect to a specific API
def get_install_manager_info(base_info,cluster_info,mgr):
    my_cluster = '/clusters/' + cluster_info['cluster']
    cluster_info['my_cluster'] = my_cluster
    base_url = cluster_info['host']
    base_url += cluster_info['version']
    print 'base_url=%s' % (base_url)
    cluster_info['base_url'] = base_url
    return cluster_info

def get_commands(base,cluster):
    # get API commands
    print "now running API commands"
    mgr = cluster['manager']
    host_url = cluster['base_url']
    username = cluster['username']
    password = cluster['password']
    configs_subdir = base[mgr]['subdir']
    my_cluster = cluster['my_cluster']
     
    if mgr == "cm":
        print "Hadoop install manager: Cloudera Manager"
        #cm.cm_commands(host_url, configs_subdir, base, cluster) ## disabling for now
    
    elif mgr == "ambari":
        print "Hadoop install manager: Ambari"
        ambari.ambari_commands(host_url, configs_subdir, base, cluster)
    
    else:
        print "Your Hadoop install manager is not recognized"

