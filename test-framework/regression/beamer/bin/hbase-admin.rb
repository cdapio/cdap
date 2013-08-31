# To use this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main hbase.rb

require 'optparse'

operation = ARGV[0]

require 'java'

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.MasterNotRunningException
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables

# disable debug logging on this script for clarity
log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)

def truncate(admin, name, conf) 
 htable = org.apache.hadoop.hbase.client.HTable.new(conf, name)
 descriptor = htable.getTableDescriptor()

 if(admin.tableExists(name)) 
   if(! admin.isTableDisabled(name)) 
     admin.disableTable(name)
   end
   admin.deleteTable(name)
   admin.createTable(descriptor)
 end
end

def delete(admin, name) 
  if(admin.tableExists(name))
    if(! admin.isTableDisabled(name))
      admin.disableTable(name)
    end
    admin.deleteTable(name)
  end
end

config = HBaseConfiguration.create
config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)

# wait until the master is running
admin = nil
while true
  begin
    admin = HBaseAdmin.new config
    break
  rescue MasterNotRunningException => e
    print 'Waiting for master to start...\n'
    sleep 1
  end
end

htabledescriptors = admin.listTables()
htabledescriptors.each do |table|
  name=table.getNameAsString()
  if(operation == "delete")
    print " - " + name + "\n"
    delete(admin, name)  
  end
  if(operation == "truncate") 
    print " - " + name + "\n"
    truncate(admin, name, config)
  end
end

exit 0
