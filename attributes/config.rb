# Default: conf.chef
default['cdap']['conf_dir'] = 'conf.chef'
# cdap-site.xml
default['cdap']['cdap_site']['cdap.namespace'] = 'cdap'
# ideally we could put the macro '/${cdap.namespace}' here but this attribute is used elsewhere in the cookbook
default['cdap']['cdap_site']['hdfs.namespace'] = "/#{node['cdap']['cdap_site']['cdap.namespace']}"
default['cdap']['cdap_site']['hdfs.user'] = 'yarn'
default['cdap']['cdap_site']['kafka.log.dir'] = '/data/cdap/kafka-logs'
default['cdap']['cdap_site']['kafka.broker.quorum'] = "#{node['fqdn']}:9092"
default['cdap']['cdap_site']['kafka.seed.brokers'] = "#{node['fqdn']}:9092"
default['cdap']['cdap_site']['kafka.default.replication.factor'] = '1'
default['cdap']['cdap_site']['log.retention.duration.days'] = '7'
default['cdap']['cdap_site']['zookeeper.quorum'] = "#{node['fqdn']}:2181"
default['cdap']['cdap_site']['router.bind.address'] = node['fqdn']
default['cdap']['cdap_site']['app.bind.address'] = node['fqdn']
default['cdap']['cdap_site']['data.tx.bind.address'] = node['fqdn']
default['cdap']['cdap_site']['metrics.query.bind.address'] = node['fqdn']
default['cdap']['cdap_site']['dashboard.bind.port'] = '9999'
default['cdap']['cdap_site']['gateway.server.address'] = node['fqdn']
default['cdap']['cdap_site']['gateway.server.port'] = '10000'
default['cdap']['cdap_site']['gateway.memory.mb'] = '512'
default['cdap']['cdap_site']['log.saver.run.memory.megs'] = '512'
