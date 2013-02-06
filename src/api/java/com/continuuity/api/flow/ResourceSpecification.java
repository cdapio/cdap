package com.continuuity.api.flow;

import com.google.common.base.Function;

/**
 * This class is for specifying resources that are needed for a
 * {@link com.continuuity.api.flow.flowlet.Flowlet Flowlet}.
 */
public final class ResourceSpecification {

  /**
   * Create a {@link Builder} for specifying resources.
   * This method is package access only and should only be called from {@link FlowSpecification}.
   *
   * @param setter A {@link Function} for setting the {@link ResourceSpecification} when the builder is done.
   * @param <T> Type of the object returned by the {@link Builder#apply()} method.
   * @return A new {@link Builder}.
   */
  static <T> Builder<T> builder(Function<ResourceSpecification, T> setter) {
    return new Builder<T>(setter);
  }

  /**
   * Number of CPU cores.
   */
  private final int cpu;

  /**
   * Amount of memory in MB.
   */
  private final int memoryInMB;

  /**
   * Uplink bandwidth in Mbps.
   */
  private final int uplinkInMbps;

  /**
   * Downlink bandwidth in Mbps.
   */
  private final int downlinkInMbps;

  /**
   * Private constructor to creates a {@link ResourceSpecification}. Only invoked from {@link Builder}.
   */
  private ResourceSpecification(int cpu, int memoryInMB, int uplinkInMbps, int downlinkInMbps) {
    this.cpu = cpu;
    this.memoryInMB = memoryInMB;
    this.uplinkInMbps = uplinkInMbps;
    this.downlinkInMbps = downlinkInMbps;
  }

  /**
   * @return Number of CPU cores.
   */
  public int getCpu() {
    return cpu;
  }

  /**
   * @return Amount of memory in MB.
   */
  public int getMemoryInMB() {
    return memoryInMB;
  }

  /**
   * @return Uplink bandwidth in Mbps.
   */
  public int getUplinkInMbps() {
    return uplinkInMbps;
  }

  /**
   * @return Downlink bandwidth in Mbps.
   */
  public int getDownlinkInMbps() {
    return downlinkInMbps;
  }

  /**
   * Builder class for building {@link ResourceSpecification}.
   *
   * @param <T> Type of the object returned by the {@link #apply()} method.
   */
  public static final class Builder<T> {

    /**
     * {@link Function} to invoke when {@link #apply()} is called.
     */
    private final Function<ResourceSpecification, T> afterFunc;

    /**
     * @see ResourceSpecification#cpu
     */
    private int cpu;

    /**
     * @see ResourceSpecification#memoryInMB
     */
    private int memory;

    /**
     * @see ResourceSpecification#uplinkInMbps
     */
    private int uplink;

    /**
     * @see ResourceSpecification#downlinkInMbps
     */
    private int downlink;

    /**
     * Creates a new Builder.
     *
     * @param afterFunc The {@link Function} to call when building is completed.
     */
    private Builder(Function<ResourceSpecification, T> afterFunc) {
      this.afterFunc = afterFunc;
    }

    /**
     * Sets number of CPU required by this flowlet.
     * @param cpu number of CPUs
     * @return this builder.
     */
    public Builder setCpu(int cpu) {
      this.cpu = cpu;
      return this;
    }

    /**
     * Sets amount of memory in MB required by this flowlet.
     * @param memoryInMB required by this flowlet.
     * @return this builder.
     */
    public Builder setMemoryInMB(int memoryInMB) {
      this.memory = memoryInMB;
      return this;
    }

    /**
     * Sets up the UpLink for this Flowlet in Mbps.
     * @param uplinkInMbps required by this flowlet.
     * @return this builder.
     */
    public Builder setUplinkInMbps(int uplinkInMbps) {
      this.uplink = uplinkInMbps;
      return this;
    }

    /**
     * Sets up the Downlink for this flowlet in Mbps.
     * @param downlinkInMbps required by this flowlet.
     * @return this builder.
     */
    public Builder setDownlinkInMbps(int downlinkInMbps) {
      this.downlink = downlinkInMbps;
      return this;
    }

    /**
     * Creates a {@link ResourceSpecification} based on what's being set to this {@link Builder} and
     * invoke the {@link #afterFunc}.
     *
     * @return An object of type {@code T}
     */
    public T apply() {
      return afterFunc.apply(new ResourceSpecification(cpu, memory, uplink, downlink));
    }
  }
}
