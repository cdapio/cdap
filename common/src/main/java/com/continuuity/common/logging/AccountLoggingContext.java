/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging;

/**
 * Account logging context.
 */
public abstract class AccountLoggingContext extends AbstractLoggingContext {
  public static final String TAG_ACCOUNT_ID = ".accountId";

  /**
   * Constructs AccountLoggingContext.
   * @param accountId account id
   */
  public AccountLoggingContext(final String accountId) {
    setSystemTag(TAG_ACCOUNT_ID, accountId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s", getSystemTag(TAG_ACCOUNT_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s", getSystemTag(TAG_ACCOUNT_ID));
  }
}
