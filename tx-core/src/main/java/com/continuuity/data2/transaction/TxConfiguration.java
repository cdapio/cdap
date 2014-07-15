package com.continuuity.data2.transaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.DFSClient;

/**
 *  TxConfiguration Class with a static create method to create Configuration Object and adds default configurations
 */

public class TxConfiguration extends Configuration {
  public static Configuration create() {
    // Create a new configuration instance, but do NOT initialize with
    // the Hadoop default properties.
    Configuration conf = new Configuration(false);
    conf.addResource("tx-default.xml");
    conf.addResource("tx-site.xml");
    return conf;
  }
}
