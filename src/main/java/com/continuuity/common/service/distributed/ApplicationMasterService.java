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
}
