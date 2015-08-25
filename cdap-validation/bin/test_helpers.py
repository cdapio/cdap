import os
import base64
import urllib2
import json
import sys

def usage():
    print "\nThis is the usage function\n"
    print 'Usage: \n'+sys.argv[0]+' [-v, --verbose] -c, --cluster \'<cluster>\' [-m, --modules \'<modules>]\' -u --user <user:password> -U --uri <URI>\n'

    print """
    -h, --help  help menu
    -c, --cluster       cluster name used in API calls to the install manager
    -m, --modules       Run plugabble test modules, separated by ';'.  E.g. -m 'module1;module2'
                        Each module is a self-contained unit under the modules subdirectory
    -u, --user          authentication, e.g. --user 'george:pass'
    -U, --uri           URI: e.g. --uri 'http://10.240.0.8:7180'
    -v, --verbose       adds more output (additional invocations increase verbosity)
 """

def onetime_auth(host,info):
    # this should work with all, but when authentication is not set up correctly, this fails (http 403)
    passman = urllib2.HTTPPasswordMgrWithDefaultRealm() # this creates a password manager
    passman.add_password(None, host, info['username'], info['password'])
    authhandler = urllib2.HTTPBasicAuthHandler(passman) # create the AuthHandler
    opener = urllib2.build_opener(authhandler)
    urllib2.install_opener(opener)
    return urllib2

def run_request(host,info):
    user = info['username']
    passwd = info['password']
    try:
        handle = urllib2.urlopen(host)
        return handle
    except: #except HTTPError, h: ### would like to do something like this
        # try a different way (encoding headers)
        req = urllib2.Request(host)
        base64string = base64.encodestring('%s:%s' % (user, passwd))[:-1]
        authheader =  "Basic %s" % base64string
        req.add_header("Authorization", authheader)
        try:
            handle = urllib2.urlopen(req)
            return handle
        except IOError, e:
            # here we shouldn't fail if the username/password is right
            if info['verbose'] == 2: print "It looks like the username or password is wrong."
            return 'noapi'

def write_config_file(config, dir, file, createdir):
    path = dir + file 
    if not os.path.exists(dir):
        if createdir == True:
            os.makedirs(dir)
        else:
            print 'Directory missing. Unable to write configurations. Exiting.'
            exit(1)
    f = open(path, 'w')
    f.write(config)
    f.close()


# The following function is needed for Cloudera Manager and other configs that require config API calls based on lists
#   of services, retrieved through another API call.
# In order to programmatically retrieve certain configurations via API, Cloudera manager requires at least two
#   and in many cases three or four API calls, each dependent on information retrieved in the previous one or ones.
#
# Here is an example that only requires three API calls, to obtain a specific configuration file
# To get the contents of the hdfs-site.xml on the namenode, we would need to do:
#  * GET /api/v10/cm/deployment
#  * Find the namenode value e.g. hdfs-NAMENODE-cd4bc7dac120e30f653e076328de207d, in the config obtained with previous call
#  * GET /api/v10/clusters/<cluster>/serviceTypes
#  * iterate through the list of services (to verify service exists) from the serviceTypes call, and for each service:
#    - Using the value obtained from the first API call and from the second API call, run an API call that would look like:
#    GET /api/v10/clusters/<cluster>/services/hdfs/roles/hdfs-NAMENODE-cd4bc7dac120e30f653e076328de207d/process/configFiles/hdfs-site.xml
#            
# To find and iterate through all configurations needed, we need to make the following API calls and extraction
#  * GET /api/v10/cm/deployment
#  * GET /api/v10/clusters/<cluster>/serviceTypes
#  * then for every service found in serviceType:
#    - GET /api/v10/clusters/<cluster>/services/<service>/config?view=full
#    - GET /api/v10/clusters/<cluster>/services/<service>/roleTypes
#    - then for every role found in roleTypes find and make all API calls that use roles (such as the example above)
def convert_types_to_list(types):
    data = json.loads(types)
    services = []
    upper_services = [ item for item in data["items" ] ]
    for service in upper_services:
        services.append(service.lower())
    services.sort()
    return services

def get_config_and_write(url, subdir, file, cluster_info):
    # run api config retrieval commands and write to individual files
    user = cluster_info['username']
    passwd = cluster_info['password']
    config_in = run_request(url, cluster_info)
    config = config_in.read()
    create_directory_if_missing = True
    write_config_file(config, subdir, file, create_directory_if_missing)

def safe_get_config_and_write(url, user, passwd, subdir, file):
    # run api config retrieval commands and write to individual files (safer)
    config_in = ''
    try:
        config_in = urllib2.urlopen(url)
        config = config_in.read()
        create_directory_if_missing = True
        write_config_file(config, subdir, file, create_directory_if_missing)
    except:
        # need to find a good way to ignore API calls that return nothing (for unused services)
        pass

