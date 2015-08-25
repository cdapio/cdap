#!/usr/bin/python

import testframework
import sys
import input
import getopt

# generic static values used regardless of user input
base_vars = dict(
cloudera = { 'api_test': 'version', 'version': '', 'subdir': 'cloudera_configs/', 'stored_results': 'stored_configs', 'baseref': 'baseref_configs' },
ambari = { 'api_test': 'v1/clusters', 'version': 'v1', 'subdir': 'ambari_configs/', 'stored_results': 'stored_configs', 'baseref': 'baseref_configs' }
)

def main(argv):

    ### input processing functions
    cluster_vars = input.process_input(argv)
    # input is in progress 

    # To test the framework, create a JSON formatted input file called 'input.json' in the same directory as the scripts.
    # file should have the following structure:
    
    #{
    #  'params':
    #  {
    #    'verbose': 'default',
    #    'uri': 'http://ambari_server_fqdn:8080/',
    #    'userpass': 'username:password',
    #    'cluster_name': 'clustername',
    #    'modules': 'config'
    #  }
    #}

    ### test framework functions
    testframework.testing(base_vars,cluster_vars)

if __name__ == '__main__':
    main(sys.argv[1:])

