/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.context;

import com.continuuity.common.logging.ApplicationLoggingContext;

/**
 * Flowlet logging context.
 */
public class FlowletLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_FLOW_ID = ".flowId";
  public static final String TAG_FLOWLET_ID = ".flowletId";

  /**
   * Constructs the FlowletLoggingContext.
   * @param accountId account id
   * @param applicationId application id
   * @param flowId flow id
   * @param flowletId flowlet id
   */
  public FlowletLoggingContext(final String accountId,
                               final String applicationId,
                               final String flowId,
                               final String flowletId) {
    super(accountId, applicationId);
    setSystemTag(TAG_FLOW_ID, flowId);
    setSystemTag(TAG_FLOWLET_ID, flowletId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_FLOW_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/flow-%s", super.getLogPathFragment(), getSystemTag(TAG_FLOW_ID));
  }
}
