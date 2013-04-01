package com.continuuity.machinedata;

/**
 *
 */

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.google.common.base.Splitter;
import org.slf4j.LoggerFactory;

import java.util.Date;


import java.lang.Long;


public class DiskStatsFlowlet extends MetricsCollector {

  private org.slf4j.Logger LOG = LoggerFactory.getLogger(CPUStatsFlowlet.class);

  public static final String NAME = "DISK_STAT_FLOWLET";
  public static final String DESC = "Disk usage stats flowlet";

  private static final Splitter TAG_SPLITTER
    = Splitter.on(",").trimResults().omitEmptyStrings();

  @Override
  public TimeseriesTable.Entry generateEntry(String metric) {

    TimeseriesTable.Entry entry = null;
    Date timestamp = new Date();
    long disk = 0;
    String hostname = "";

    // Extract tags, expecting "timestamp, memory, hostname"
    int idx = 0;
    if (!metric.isEmpty()) {
      Iterable<String> tags = TAG_SPLITTER.split(metric);

      for (String tag : tags) {
        switch (idx) {
          case 0:
            timestamp = new Date(Long.parseLong(tag));
            break;
          case 1:
            disk = Long.parseLong(tag);
            break;
          case 2:
            hostname = tag;
            break;
        }
        idx++;
      }

      // create time series entry, if and only if all 3 entrires have been parsed correctly
      if (idx == 3) {
        LOG.info("generating entry, timestamp: " + timestamp.getTime() + ",disk: " + disk + "hostname: " + hostname);
        entry = new TimeseriesTable.Entry(Bytes.toBytes("disk"), Bytes.toBytes(disk), timestamp.getTime(), Bytes.toBytes(hostname));
      }
      else {
        LOG.error("Unable to process metric:" + metric);
      }
    }

    return entry;
  }
}