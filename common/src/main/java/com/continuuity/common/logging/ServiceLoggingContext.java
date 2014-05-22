package com.continuuity.common.logging;

/**
 *
 */
public class ServiceLoggingContext extends ComponentLoggingContext {
  public static final String TAG_SERVICE_ID = ".serviceId";

  public ServiceLoggingContext(final String systemId, final String componentId, final String serviceId) {
    super(systemId, componentId);
    setSystemTag(TAG_SERVICE_ID, serviceId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_SERVICE_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/service-%s", super.getLogPathFragment(), getSystemTag(TAG_SERVICE_ID));
  }

}
