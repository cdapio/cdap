package com.continuuity.internal.app.services.legacy;

/**
 * Default implementation of resource definition.
 */
public class ResourceDefinitionImpl implements ResourceDefinition, ResourceDefinitionModifier {
  /**
   * Number of CPUs that a flowlet should use.
   */
  private int cpu;

  /**
   * Amount of memory a flowlet should use.
   */
  private int memory;

  /**
   * Uplink bandwidth a flowlet can use.
   */
  private int uplink;

  /**
   * Downlink bandwidth a flowlet can use.
   */
  private int downlink;

  /**
   * Returns number of CPUs to be allocated to flowlet.
   *
   * @return number of cpus
   */
  @Override
  public int getCPU() {
    return cpu;
  }

  /**
   * Returns amount of memory to be allocated to a flowlet in MB.
   *
   * @return amount of memory in MB.
   */
  @Override
  public int getMemoryInMB() {
    return memory;
  }

  /**
   * Returns uplink bandwidth specification in Mbps.
   *
   * @return uplink bandwidth in Mbps.
   */
  @Override
  public int getUpLinkInMbps() {
    return uplink;
  }

  /**
   * Returns downlink bandwidth specification in Mbps.
   *
   * @return downlink bandwidth in Mbps.
   */
  @Override
  public int getDownLinkInMbps() {
    return downlink;
  }

  /**
   * Sets a new CPU count for a flowlet.
   *
   * @param newCPU count for a flowlet.
   * @return old CPU count.
   */
  @Override
  public int setCPU(int newCPU) {
    int old = cpu;
    cpu = newCPU;
    return old;
  }

  /**
   * Sets a new Memory allocation for a flowlet.
   *
   * @param newMemory for a flowlet.
   * @return previous memory allocated.
   */
  @Override
  public int setMemory(int newMemory) {
    int old = memory;
    memory = newMemory;
    return old;
  }

  /**
   * Sets a new uplink bandwidth specification for a flowlet.
   *
   * @param newUplink bandwidth to be allocated.
   * @return old allocation.
   */
  @Override
  public int setUplink(int newUplink) {
    int old = uplink;
    uplink = newUplink;
    return old;
  }

  /**
   * Sets a new downlink bandwidth specification for a flowlet.
   *
   * @param newDownlink bandwidth to be allocated
   * @return old allocation.
   */
  @Override
  public int setDownlink(int newDownlink) {
    int old = downlink;
    downlink = newDownlink;
    return old;
  }
}
