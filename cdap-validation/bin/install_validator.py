#!/usr/bin/python

import testing

# generic static values used regardless of user input
base_variables = dict(
cm = { 'api_test': 'version', 'version': '', 'subdir': 'cm_configs/', 'stored_results': 'stored_configs' },
ambari = { 'api_test': 'v1/clusters', 'version': 'v1', 'subdir': 'ambari_configs/', 'stored_results': 'stored_configs' }
)

### input processing functions
# coming soon

# To test the framework, create a JSON formatted input file called 'input.json' in the same directory as the scripts.
# file should have the following structure:

#{
#  "params":
#  {
#    "verbose": "default",
#    "uri": "http://ambari_server_fqdn:8080/",
#    "userpass": "username:password",
#    "cluster_name": "clustername",
#    "modules": "config"
#  }
#}

### test framework functions
testing.testing(base_variables)

