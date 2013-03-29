package com.continuuity.machinedata.data;

/**
 *
 */
public class CPUStat {
  public long timesStamp;
  public String cpuUsage;
  public String hostname;

  public CPUStat(long ts, String cpu, String host) {
    this.timesStamp = ts;
    this.cpuUsage = cpu;
    this.hostname = host;
  }
}
