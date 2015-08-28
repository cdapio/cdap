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

import testframework
import sys
import input

# generic static values used regardless of user input
base_vars = dict(
    cloudera={'api_test': 'version', 'version': '', 'subdir': 'cloudera_configs/', 'stored_results': 'stored_configs'},
    ambari={'api_test': 'v1/clusters', 'version': 'v1', 'subdir': 'ambari_configs/', 'stored_results': 'stored_configs'}
)

# add services

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
    #    'host': 'http://ambari_server_fqdn:8080/',
    #    'user': 'username',
    #    'pass': 'password',
    #    'cluster': 'clustername',
    #    'modules': 'config'
    #  }
    #}

    ### test framework functions
    testframework.test(base_vars, cluster_vars)

if __name__ == '__main__':
    main(sys.argv[1:])
