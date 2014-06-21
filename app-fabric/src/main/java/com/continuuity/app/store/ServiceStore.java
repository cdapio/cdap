package com.continuuity.app.store;

import com.continuuity.data2.transaction.TransactionFailureException;

/**
 * Stores/Retrieves Information about System Services.
 */
public interface ServiceStore {

  /**
   * Get the service instance count.
   * @param serviceName Service Name.
   * @return Instance Count (can be null if no value was present for the given ServiceName).
   */
  Integer getServiceInstance(String serviceName) throws TransactionFailureException;

  /**
   * Set the service instance count.
   * @param serviceName Service Name.
   * @param instances Instance Count.
   */
  void setServiceInstance(String serviceName, int instances) throws TransactionFailureException;
}
