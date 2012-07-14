package com.continuuity.common.service.distributed;


import com.continuuity.common.utils.ImmutablePair;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 *
 */
public interface ApplicationMasterService extends Service {
  /**
   * Returns an {@code ApplicationMasterSpecification} object.
   *
   * @return instance of {@code ApplicationMasterSpecification}
   */
  ApplicationMasterSpecification getSpecification();

  /**
   * Returns the application attempt Id.
   *
   * @return application instance id associated with run.
   */
  ApplicationAttemptId getApplicationAttemptId();

  /**
   * Adds a task to be executed.
   *
   * @param specification specification of the task to be added for execution.
   */
  void addTask(TaskSpecification specification);

  /**
   * Removes a task from run
   *
   * @param specification of the task to be removed.
   */
  void removeTask(TaskSpecification specification);
}
