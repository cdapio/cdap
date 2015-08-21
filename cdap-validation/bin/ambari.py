import urllib2
import json
import re
import glob, os
import test_helpers as helpers

# Start Ambari configuration retrieval process
def ambari_commands(host, subdir, base, cluster_info):
    ## get ambari configs
    manager = 'ambari' 
    cu = get_config_urls(host, cluster_info)
    #print 'cu=%s' % (cu)
    config_url_href = cu.read()
    #print 'configurlref=%s' % (config_url_href)

    ## extract the ambari config urls
    data = json.loads(config_url_href)
    config_urls = [ item['href'] for item in  data['items'] ]

    ## iterate through ambari config urls, and run api requests against those
    ##   This creates config files in a subdirectory
    get_configs(host, config_urls, subdir, cluster_info)

    # process tmp configs and store results
    ambari_stored_configs = base[manager]['stored_results']
    store_results(subdir, ambari_stored_configs, manager)

# get config urls
def get_config_urls(host, cluster_info):
    cluster = cluster_info['cluster']  
    append = '/clusters/' + cluster + '/configurations' 
    url = host + append
    print 'url=%s' % (url)
    config_urls = helpers.run_request(url, cluster_info)
    return config_urls

# get configs through config urls
def get_configs(host, urls, subdir, cluster_info):
    for uri in urls:
        # extract service file name
        m = re.search('\?type=(.+?)&tag', uri)
        file = m.group(1)

        # run api config retrieval command and write result to file
        helpers.get_config_and_write(uri, subdir, file, cluster_info)

# process tmp configs and store results
def store_results(subdir, stored_configs, mgr):
    # get list of tmp config files and read
    print "Get and store all %s configurations in %s\n" % (mgr, stored_configs)
    s = open(stored_configs, 'w')
    os.chdir(subdir)
    for file in glob.glob('*'):

        # open file
        f = open(file, 'r')
        tmp_config_file = f.read()
        f.close()
        service = get_service_name(file)
        print 'service=%s' % (service)

        # read json
        data = json.loads(tmp_config_file)

        # extract properties
        try:
            properties = [ items['properties'] for items in data['items']]
        except KeyError:
            pass # we need to do this for config files with no properties

        # get the properties we want and parse
        for key, value in properties[0].iteritems():
            if key == 'content': ### filter out properties with key = 'content'
                continue
            s.write('%s:%s=\'%s\'\n' % (service, key, value))
        
    # return
    os.chdir('..')

def get_service_name(service_config_file):
    # take file name, extract first part (before first '-') treat that as the service
    # extra the part before the '-' and return it
    service = service_config_file.split('-')[0]
    if service_config_file == 'zoo.cfg':
        service = 'zookeeper'
    return service

