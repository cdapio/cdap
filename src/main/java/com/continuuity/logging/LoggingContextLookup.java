/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging;

import com.continuuity.app.logging.FlowletLoggingContext;
import com.continuuity.app.logging.MapReduceLoggingContext;
import com.continuuity.app.logging.ProcedureLoggingContext;
import com.continuuity.common.logging.AccountLoggingContext;
import com.continuuity.common.logging.ApplicationLoggingContext;
import com.continuuity.common.logging.LoggingContext;

import java.util.Map;

/**
 * Returns the LoggingContext object based on the log tags.
 */
public final class LoggingContextLookup {
  private LoggingContextLookup() {}

  public static LoggingContext getLoggingContext(Map<String, String> tags) {
    // Tags are empty, cannot determine logging context.
    if (tags == null || tags.isEmpty()) {
      return null;
    }

    String accountId = tags.get(AccountLoggingContext.TAG_ACCOUNT_ID);
    String applicationId = tags.get(ApplicationLoggingContext.TAG_APPLICATION_ID);

    // No account id or application id present.
    if (accountId == null || applicationId == null) {
      return null;
    }

    if (tags.containsKey(FlowletLoggingContext.TAG_FLOW_ID)) {
      if (!tags.containsKey(FlowletLoggingContext.TAG_FLOWLET_ID)) {
        return null;
      }
      return new FlowletLoggingContext(accountId, applicationId, tags.get(FlowletLoggingContext.TAG_FLOW_ID),
                                       tags.get(FlowletLoggingContext.TAG_FLOWLET_ID));
    } else if (tags.containsKey(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID)) {
      return new MapReduceLoggingContext(accountId, applicationId,
                                         tags.get(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID));
    } else if (tags.containsKey(ProcedureLoggingContext.TAG_PROCEDURE_ID)) {
      return new ProcedureLoggingContext(accountId, applicationId,
                                         tags.get(ProcedureLoggingContext.TAG_PROCEDURE_ID));
    }

    return null;
  }
}
