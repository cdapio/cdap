package com.continuuity.data.util;

import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.OperationContext;

/**
 * Utility class.
 */
public class OperationUtil {

  /** defaults to be used everywhere where we don't have authenticated accounts. */
  public static final OperationContext DEFAULT = new OperationContext(Constants.DEVELOPER_ACCOUNT_ID);
}
