package com.continuuity.common.service.distributed;

import com.google.common.util.concurrent.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

/**
 * Client service API for managing interaction with resource and application master.
 */
public interface ClientService extends Service {
  /**
   * Returns the specification used to configure this service.
   *
   * @return specification to configure this service.
   */
  ClientSpecification getSpecification();

  /**
   * Returns the id associated with this application.
   *
   * @return id of this application.
   */
  ApplicationId getApplicationId();

  /**
   * Returns a report of the current application.
   *
   * @return report of current application.
   */
  ApplicationReport getReport();

  /**
   * Returns the state of the application.
   *
   * @return true if stopped; false otherwise.
   */
  boolean hasStopped();

  /**
   * Returns true if failed, else false.
   *
   * @return true if failed; false otherwise.
   */
  boolean hasFailed();

}
