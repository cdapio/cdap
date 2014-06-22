package com.continuuity.common.twill;

/**
 * Interface that defines a set of methods that will be used for management of Reactor Services.
 * Each individual service must provide an implementation.
 */
public interface ReactorServiceManager {
  /**
   * Used to get the count of the instances of the Reactor Service that was requested.
   * @return the number of instances of the Reactor Service requested.
   */
  public int getInstances();

  /**
   * Set the number of instances of the reactor service.
   * @param instanceCount number of instances (should be greater than 0)
   * @return was the operation successful
   */
  public boolean setInstances(int instanceCount);

  /**
   * Get the minimum instance count for the service.
   * @return the required minimum number of instances of the Reactor Service.
   */
  public int getMinInstances();

  /**
   * Get the maximum instance count for the service.
   * @return the allowed maximum number of instances of the Reactor Service.
   */
  public int getMaxInstances();

  /**
   * Logging availability.
   * @return true if logs are available.
   */
  public boolean isLogAvailable();

  /**
   * Possible to check the status of the service.
   * @return true if the status of the service can be checked.
   */
  public boolean canCheckStatus();

  /**
   * Service's availability.
   * @return true if service is available.
   */
  public boolean isServiceAvailable();

  //TODO: Add method to get the metrics name to get event rate on UI
}
