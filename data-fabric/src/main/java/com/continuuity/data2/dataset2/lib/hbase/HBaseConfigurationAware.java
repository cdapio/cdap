package com.continuuity.data2.dataset2.lib.hbase;

import org.apache.hadoop.conf.Configuration;

/**
 * Implementation of this interface needs to know about HBase cluster.
 */
public interface HBaseConfigurationAware {
  /**
   * Sets {@link org.apache.hadoop.hbase.HBaseConfiguration}.
   * @param hConf instance of {@link org.apache.hadoop.hbase.HBaseConfiguration}
   */
  void setHBaseConfig(Configuration hConf);
}
