package com.continuuity.common.service.distributed;


import com.continuuity.common.utils.ImmutablePair;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.List;

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
  void addTask(List<TaskSpecification> specification);

  /**
   * Removes a task from run
   *
   * @param specification of the task to be removed.
   */
  void removeTask(List<TaskSpecification> specification);

  /**
   * Number of tasks to be yet allocated containers to run.
   *
   * @return number of tasks that are still waiting to be allocated container.
   */
  int getPendingTasks();

  /**
   * Returns number of tasks that are pending release.
   *
   * @return number of tasks that are still pending release.
   */
  int getPendingReleases();
}
