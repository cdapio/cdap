package com.continuuity.common.twill;

/**
 * Interface that defines a set of methods that will be used for management of Reactor Services.
 * Each individual service must provide an implementation.
 */
public interface ReactorServiceManagement {
  /**
   * Used to get the count of the instances of the Reactor Service
   * @return the number of instances of the Reactor Service
   */
  public int getInstanceCount();

  /**
   * Set the number of instances of the reactor service
   * @param instanceCount number of instances (should be greater than 0)
   * @return was the operation successful
   */
  public boolean setInstanceCount(int instanceCount);
}
