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
    -m, --modules       run modules, e.g. --modules 'module1;module2'
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

def write_config_file(config, dir, file):
    path = dir + file 
    f = open(path, 'w')
    f.write(config)
    f.close()

# needed for CM and other configs that require config API calls based on lists of services, etc.. 
#    retrieved through another API call, e.g. 
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
    write_config_file(config, subdir, file)

def safe_get_config_and_write(url, user, passwd, subdir, file):
    # run api config retrieval commands and write to individual files (safer)
    config_in = ''
    try:
        config_in = urllib2.urlopen(url)
        config = config_in.read()
        write_config_file(config, subdir, file)
    except:
        # need to find a good way to ignore API calls that return nothing (for unused services)
        pass

