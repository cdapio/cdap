package com.continuuity.api.flow;

import com.google.common.base.Function;

/**
 *
 */
public final class ResourceSpecification {

  static <T> Builder<T> builder(Function<ResourceSpecification, T> setter) {
    return new Builder<T>(setter);
  }

  private final int cpu;
  private final int memoryInMB;
  private final int uplinkInMbps;
  private final int downlinkInMbps;

  private ResourceSpecification(int cpu, int memoryInMB, int uplinkInMbps, int downlinkInMbps) {
    this.cpu = cpu;
    this.memoryInMB = memoryInMB;
    this.uplinkInMbps = uplinkInMbps;
    this.downlinkInMbps = downlinkInMbps;
  }

  public int getCpu() {
    return cpu;
  }

  public int getMemoryInMB() {
    return memoryInMB;
  }

  public int getUplinkInMbps() {
    return uplinkInMbps;
  }

  public int getDownlinkInMbps() {
    return downlinkInMbps;
  }

  public static final class Builder<T> {

    private final Function<ResourceSpecification, T> afterFunc;
    private int cpu;
    private int memory;
    private int uplink;
    private int downlink;

    private Builder(Function<ResourceSpecification, T> afterFunc) {
      this.afterFunc = afterFunc;
    }

    public Builder setCpu(int cpu) {
      this.cpu = cpu;
      return this;
    }

    public Builder setMemoryInMB(int memoryInMB) {
      this.memory = memoryInMB;
      return this;
    }

    public Builder setUplinkInMbps(int uplinkInMbps) {
      this.uplink = uplinkInMbps;
      return this;
    }

    public Builder setDownlinkInMbps(int downlinkInMbps) {
      this.downlink = downlinkInMbps;
      return this;
    }

    public T apply() {
      return afterFunc.apply(new ResourceSpecification(cpu, memory, uplink, downlink));
    }
  }
}
