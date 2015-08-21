#!/usr/bin/python

import testing

# generic static values used regardless of user input
base_variables = dict(
cm = { 'api_test': 'version', 'version': '', 'subdir': 'cm_configs/', 'stored_results': 'ambari_stored' },
ambari = { 'api_test': 'v1/clusters', 'version': 'v1', 'subdir': 'ambari_configs/', 'stored_results': 'cm_stored' }
)

### input processing functions
# coming soon

### test framework functions
testing.testing(base_variables)

