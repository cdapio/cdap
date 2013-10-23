package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.lib.table.TimeToLiveSupported;
import com.continuuity.weave.filesystem.LocationFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Data set manager for hbase metrics tables. Implements TimeToLiveSupported as an indication of TTL.
 */
public class HBaseMetricsTableManager extends HBaseOcTableManager implements TimeToLiveSupported {

  public HBaseMetricsTableManager(CConfiguration conf, Configuration hConf, LocationFactory locationFactory)
      throws IOException {
    super(conf, hConf, locationFactory);
  }
}
