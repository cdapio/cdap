# helper functions for modules

import re

def vout(result, line, output, verbose):
    # we want to follow nagios standard
    # the way it applies here:
    # 0 => ok     -- only send to output if verbose
    # 1 => not ok -- always send to output
    # 2 => critical (fatal) => runtime error and exit
    #example: not ok config hadoop hadoop_hdfs_replication_factor 3 2
    
    result_ref = {}
    result_ref['0'] = 'ok'
    result_ref['1'] = 'not ok'

    restext = result_ref[result]
    output_line = restext + ' ' + output

    #test for verbose level if verbose = yes, then print ok results as well
    if result == '0':
        if verbose >= 1:
            print output_line

    #always print not ok results
    elif result == '1':  # print
        print output_line

#############################

# Need to convert to bytes before we do anything

def convert_mult_to_bytes(alphavalue):
    # parse value string, e.g. 1024m and split the two 
    multipliers = {'k': 1, 'm': 2, 'g': 3 }
    num = re.search('(\d+)([a-z]?)', alphavalue)
    value = long(num.group(1))
    z = num.group(2)

    if z == '':    # if the multiplier is empty, set to 1
        multiplier = 1

    else:          # else it is 1024 ** power corresponding to multiplier
        zexp = multipliers[z]
        multiplier = 1024 ** int(zexp)

    value *= multiplier
    #print 'value=%d' % (value)
    return value
