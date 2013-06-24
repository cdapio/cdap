package com.continuuity.logging.filter;

import com.continuuity.app.logging.FlowletLoggingContext;
import com.google.common.collect.ImmutableList;

/**
 * Helper class to generate filters.
 */
public class LogFilterGenerator {
  public static Filter createTailFilter(String accountId, String applicationId, String flowId) {
    return new AndFilter(ImmutableList.of(new MdcExpression(FlowletLoggingContext.TAG_ACCOUNT_ID, accountId),
                                          new MdcExpression(FlowletLoggingContext.TAG_APPLICATION_ID, applicationId),
                                          new MdcExpression(FlowletLoggingContext.TAG_FLOW_ID, flowId)));
  }
}
