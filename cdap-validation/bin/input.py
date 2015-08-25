
import getopt
import sys
import test_helpers as helpers

##### INPUT FUNCTIONS #####

def process_input(argv):
## Calls the functions below
# take input
#    print blah
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
            input_vars['user'] = arg
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
        for k,v in input_vars.iteritems():
            print 'input vars = %s=%s' % (k, v)

    return input_vars

# validate_params()
# process_modules()
# convert_input()

###def process_params
# parses parameters

###def validate_params
# validates command line parameters:
#   verify they are correct
#   verify all the parameters that are needed are there

###def process_modules
# process what is in between quotes in --modules=""

###def convert_input
# takes input and converts it to a JSON object
