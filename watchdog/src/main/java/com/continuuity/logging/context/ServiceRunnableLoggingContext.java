package com.continuuity.logging.context;

import com.continuuity.common.logging.ApplicationLoggingContext;

/**
 *
 */
public class ServiceRunnableLoggingContext extends ApplicationLoggingContext {
  public static final String TAG_SERVICE_ID = ".serviceId";
  public static final String TAG_RUNNABLE_ID = ".runnableId";

  public ServiceRunnableLoggingContext(final String accountId,
                                       final String applicationId,
                                       final String serviceId,
                                       final String runnableId) {
    super(accountId, applicationId);
    setSystemTag(TAG_SERVICE_ID, serviceId);
    setSystemTag(TAG_RUNNABLE_ID, runnableId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_SERVICE_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/userservice-%s", super.getLogPathFragment(), getSystemTag(TAG_SERVICE_ID));
  }

}
