package com.continuuity.machinedata.data;

/**
 *
 */
public class CPUStat {
  public long timesStamp;
  public int cpuUsage;
  public String hostname;

  public CPUStat(long ts, int cpu, String host) {
    this.timesStamp = ts;
    this.cpuUsage = cpu;
    this.hostname = host;
  }
}
