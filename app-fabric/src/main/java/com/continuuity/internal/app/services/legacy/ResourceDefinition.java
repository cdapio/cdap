package com.continuuity.internal.app.services.legacy;

/**
 * ResourceDefinition is a read-only interface for specifing the resources needed by a flowlet.
 */
public interface ResourceDefinition {
  /**
   * Returns number of CPUs to be allocated to flowlet.
   *
   * @return number of cpus
   */
  public int getCPU();

  /**
   * Returns amount of memory to be allocated to a flowlet in MB.
   * @return amount of memory in MB.
   */
  public int getMemoryInMB();

  /**
   * Returns uplink bandwidth specification in Mbps.
   *
   * @return uplink bandwidth in Mbps.
   */
  public int getUpLinkInMbps();

  /**
   * Returns downlink bandwidth specification in Mbps.
   *
   * @return downlink bandwidth in Mbps.
   */
  public int getDownLinkInMbps();
}
