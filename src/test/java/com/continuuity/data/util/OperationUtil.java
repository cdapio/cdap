package com.continuuity.data.util;

import com.continuuity.data.operation.OperationContext;

public class OperationUtil {

  /** defaults to be used everywhere where we don't have authenticated accounts */
  public static final String DEFAULT_ACCOUNT_ID = "developer";
  public static final OperationContext DEFAULT = new OperationContext(DEFAULT_ACCOUNT_ID);
}
