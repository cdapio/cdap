package com.continuuity.machinedata;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable.Entry;
import org.slf4j.LoggerFactory;
import com.google.common.base.Splitter;
import java.lang.Long;
import java.util.Date;


public class CPUStatsFlowlet extends MetricsCollector {

  private org.slf4j.Logger LOG = LoggerFactory.getLogger(CPUStatsFlowlet.class);

  public static final String NAME = "CPU_STAT_FLOWLET";
  public static final String DESC = "CPU stats flowlet";

  private static final Splitter TAG_SPLITTER
    = Splitter.on(",").trimResults().omitEmptyStrings();

  @Override
  public TimeseriesTable.Entry generateEntry(String metric) {

    Entry entry = null;
    Date timestamp = new Date();
    int cpu = 0;
    String hostname = "";

    // Extract tags, expecting "timestamp, cpu, hostname"
    int idx = 0;
    if (!metric.isEmpty()) {
      Iterable<String> tags = TAG_SPLITTER.split(metric);

      for (String tag : tags) {
        switch (idx) {
          case 0:
            timestamp = new Date(Long.parseLong(tag));
            break;
          case 1:
            cpu = Integer.parseInt(tag);
            break;
          case 2:
            hostname = tag;
            break;
        }
        idx++;
      }

      // create time series entry, if and only if all 3 entrires have been parsed correctly
      if (idx == 3) {
        LOG.info("generating entry, timestamp: " + timestamp.getTime() + ",cpu: " + cpu + "hostname: " + hostname);
        entry = new Entry(Bytes.toBytes("cpu"), Bytes.toBytes(cpu), timestamp.getTime(), Bytes.toBytes(hostname));
      }
      else {
        LOG.error("Unable to process metric:" + metric);
      }
    }

    return entry;
  }
}