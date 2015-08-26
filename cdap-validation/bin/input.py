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

import getopt
import sys
import test_helpers as helpers

##### INPUT FUNCTIONS #####


def process_input(argv):
    input_vars = process_params(argv)
    return input_vars


def process_params(params):
    input_vars = {}
    input_vars['verbose'] = 0
    try:
        opts, args = getopt.getopt(params, 'hc:m:u:U:vd', ['help', 'cluster=', 'module=', 'user=', 'uri=', 'verbose'])
    except getopt.GetoptError:
        helpers.usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            helpers.usage()
            sys.exit()
        elif opt in ('-c', '--cluster'):
            input_vars['cluster'] = arg
        elif opt in ('-m', '--module'):
            input_vars['modules'] = arg
        elif opt in ('-u', '--user'):
            # split user auth info to user and password and store accordingly
            input_vars['user'], input_vars['password'] = arg.split(':')
        elif opt in ('-U', '--uri'):
            input_vars['host'] = arg
        elif opt in ('-v', '--verbose'):
            input_vars['verbose'] = 1
        elif opt == '-d':
            global _debug
            print 'debug not implemented yet'
            input_vars['verbose'] = 2
            _debug = 1

    if input_vars['verbose'] == 2:
        for k, v in input_vars.iteritems():
            print 'input vars = %s=%s' % (k, v)

    return input_vars

### Placeholders for future functions
# validate_params() ## ensures parameters are valid and minimum number of parameters necesssary is present
# process_module_params() ## process what is in between quotes in --modules=""
# convert_input() ## creates JSON object from processed parameters
