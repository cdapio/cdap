package com.continuuity.logging.context;

import com.continuuity.common.logging.ApplicationLoggingContext;

/**
 * Logging Context for Services defined by users.
 */
public class UserServiceLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_USERSERVICE_ID = ".userserviceid";
  public static final String TAG_RUNNABLE_ID = ".userrunnableid";

  public UserServiceLoggingContext(final String accountId,
                                   final String applicationId,
                                   final String serviceId,
                                   final String runnableId) {
    super(accountId, applicationId);
    setSystemTag(TAG_USERSERVICE_ID, serviceId);
    setSystemTag(TAG_RUNNABLE_ID, runnableId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_USERSERVICE_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/userservice-%s", super.getLogPathFragment(), getSystemTag(TAG_USERSERVICE_ID));
  }

}
