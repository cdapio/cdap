/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging;

/**
 * Application logging context.
 */
public abstract class ApplicationLoggingContext extends AccountLoggingContext {
  public static final String TAG_APPLICATION_ID = ".applicationId";

  /**
   * Constructs ApplicationLoggingContext.
   * @param accountId account id
   * @param applicationId application id
   */
  public ApplicationLoggingContext(final String accountId, final String applicationId) {
    super(accountId);
    setSystemTag(TAG_APPLICATION_ID, applicationId);
  }

  @Override
  public String getLogPartition() {
    return super.getLogPartition() + String.format(":%s", getSystemTag(TAG_APPLICATION_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/%s", super.getLogPathFragment(), getSystemTag(TAG_APPLICATION_ID));
  }
}
