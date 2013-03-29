package com.continuuity.machinedata.data;

import java.util.Date;

/**
 *
 */
public class CPUStat {
  public Date timesStamp;
  public int cpuUsage;
  public String hostname;

  public CPUStat(Date ts, int cpu, String host) {
    this.timesStamp = ts;
    this.cpuUsage = cpu;
    this.hostname = host;
  }
}
