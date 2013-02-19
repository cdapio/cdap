/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging;

/**
 * Application logging context.
 */
public class ApplicationLoggingContext extends AbstractLoggingContext {
  public static final String TAG_ACCOUNT_ID = "accountId";
  public static final String TAG_APPLICATION_ID = "applicationId";

  /**
   * Constructs ApplicationLoggingContext
   * @param accountId account id
   * @param applicationId application id
   */
  public ApplicationLoggingContext(final String accountId, final String applicationId) {
    setSystemTag(TAG_ACCOUNT_ID, accountId);
    setSystemTag(TAG_APPLICATION_ID, applicationId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", getSystemTag(TAG_ACCOUNT_ID), getSystemTag(TAG_APPLICATION_ID));
  }
}
