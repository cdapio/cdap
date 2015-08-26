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

################################################################
### config validator module
################################################################

import sys
import re
import install_validator.module_helpers as module_helpers
import install_validator.test_helpers as helpers
#import module_helpers as module_helpers


def strip_crlf(var):
    #print 'attempting to strip \\n from %s' % (var)
    return var.rstrip()


def check_numeric_unit(value):
    # if string ends in digits followed by [a-z], m is a Match object, else None
    m = re.search(r'\d+[a-z]$', value)
    #print 'm=%s' % (m)
    if m is not None:
        return value[:-1]
    else:
        return value


def process_range_reference(x, y):
    #print 'processing range reference'
    # 3 scenarios
    #   range = x-y => x <= value <= y
    #   range = x-  => x <= value
    #   range = -y  => value <= y
    # look for hyphen and split string -- return colon-separated values (x:y)
    if x == '' and y == '':
        print 'invalid reference range'
        exit(1)
    elif x == '':
        return 'x:' + str(y)
    elif y == '':
        return str(x) + ':y'
    else:
        return str(x) + ':' + str(y)


def exact_eval(a, b):
    #print 'testing exact_eval with %s and %s' % (a, b)
    if a == b:
        return '0'
        #print 'a = b'
    else:
        return '1'
        #print 'a != b'


def range_eval(a, x, y):
    #print 'evaluating a to see if it is within range x:y'
    #print 'a=%d x=%d y=%d' % (a, x, y)
    if x == 'x':
        #print 'x=x'
        if a > y:
            return '1'
    elif y == 'y':
        #print 'y=y'
        if a < x:
            return '1'
    else:
        #print 'x and y exist'
        if a < x or a > y:
            return '1'
        return '0'


################################################################

# process inputs:
#   base reference configurations
#   actual configurations
#   verbose level

# the goal is to iterate through baseref_configs and find corresponding actual configurations

# from the the base reference configurations, we want to determine if they represent:
#    exact value
#    range: 1-8
#           -8 less than or equal to 8, can be negative
#           1- greater than or equal to 1
#    multiple values or ranges
#    condition based

#print 'Argument List:', str(sys.argv)
#print len(sys.argv)

baseref_configs = str(sys.argv[1])
actual_configs = str(sys.argv[2])
# need to add: pass actual verbose level from framework
if len(sys.argv) == 3:
    verbose = 0
else:
    verbose = int(sys.argv[3])

module = 'config'
validation_results = {}
output = ''


class AutoVivification(dict):
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

actual = AutoVivification()
results = AutoVivification()

### get actual configs

with open(actual_configs) as l:
    for line in l:
        line = strip_crlf(line)
        service, key, value = re.split(':|=\'', line, 2)
        value = re.sub('[\']', '', value)
        value = strip_crlf(value)
        actual[service][key] = value

### get baseref_configs

f = open(baseref_configs, 'r')
for line in f:
    # baseref_configs format:
    # service:property='value/range':datatype:comparison_type

    # ignore lines starting with '#' or empty lines
    if line.startswith('#') or not line.strip():
        continue

    # placeholder: need to handle bad/improperly formatted data

    try:
        bservice, bproperty, datatype, type = line.split(':', 3)
    except:
        print '\ninvalid baseref_config line:  ', line
        continue

    bvalue = ''
    bvaluenum = 0

    datatype = strip_crlf(datatype)
    type = strip_crlf(type)
    bkey, bvalue = bproperty.split('=')
    bvalue = re.sub('[\']', '', bvalue)
    helpers.vprint ('%s:%s=%s:%s:%s' % (bservice, bkey, bvalue, datatype, type), verbose)

    # set it to a safe, impossible value
    result_value = -1

    # COMPARISONS

    value = actual[bservice][bkey]
    if datatype == 'alpha':  # alpha type -- simple comparison
        helpers.vprint ('type alpha: simple comparison', verbose)
        helpers.vprint ('value=%s    bvalue=%s' % (value, bvalue), verbose)
        result_value = exact_eval(value, bvalue)

    elif datatype == 'bytes':  # e.g. can be any of regular number, of number appended with k,m,g

        valuenum = module_helpers.convert_mult_to_bytes(value)

        if type == 'exact':
            helpers.vprint ('type = exact', verbose)
            bvaluenum = module_helpers.convert_mult_to_bytes(bvalue)
            result_value = exact_eval(bvaluenum, valuenum)
            helpers.vprint ('bvaluenum=%s  valuenum=%s' % (bvaluenum, valuenum), verbose)

        elif type == 'range':
            helpers.vprint ('type = range', verbose)
            min, max = bvalue.split('-')
            if min != '': min = module_helpers.convert_mult_to_bytes(min)
            if max != '': max = module_helpers.convert_mult_to_bytes(max)
            range = process_range_reference(min, max)
            helpers.vprint ('range=%s' % (range), verbose)
            bmin, bmax = range.split(':')
            bmin = long(bmin)
            result_value = range_eval(valuenum, bmin, bmax)
            helpers.vprint ('bvalue=%s range=%s  valuenum=%s' % (bvalue, range, valuenum), verbose)

        else:
            print 'unknown type'

    else:
        print 'unkown datatype'
        #exit(1) -- initiate runtime error

    helpers.vprint ('result_value=%s' % (result_value), verbose)
    # format output
    output = module + ' ' + service + ' ' + bkey + ' ' + bvalue + ' ' + value
    helpers.vprint ('output=%s' % (output), verbose)

    # send output
    helpers.vprint ('running vout for output', verbose)
    module_helpers.vout(result_value, line, output, verbose)
    helpers.vprint ('', verbose)
