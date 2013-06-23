package com.continuuity.internal.app.services.legacy;

/**
 * Modifier used for making modification to a resource specification of a flowlet.
 */
public interface ResourceDefinitionModifier {
  /**
   * Sets a new CPU count for a flowlet.
   *
   * @param newCPU count for a flowlet.
   * @return old CPU count.
   */
  public int setCPU(int newCPU);

  /**
   * Sets a new Memory allocation for a flowlet.
   *
   * @param newMemory for a flowlet.
   * @return previous memory allocated.
   */
  public int setMemory(int newMemory);

  /**
   * Sets a new uplink bandwidth specification for a flowlet.
   *
   * @param newUplink bandwidth to be allocated.
   * @return old allocation.
   */
  public int setUplink(int newUplink);

  /**
   * Sets a new downlink bandwidth specification for a flowlet.
   *
   * @param newDownlink bandwidth to be allocated
   * @return old allocation.
   */
  public int setDownlink(int newDownlink);
}
