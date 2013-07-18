/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.context;

import com.continuuity.common.logging.AccountLoggingContext;
import com.continuuity.common.logging.ApplicationLoggingContext;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.logging.filter.AndFilter;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.filter.MdcExpression;
import com.continuuity.logging.filter.OrFilter;
import com.google.common.collect.ImmutableList;

import java.util.Map;

/**
 * Helper class for LoggingContext objects.
 */
public final class LoggingContextHelper {
  private LoggingContextHelper() {}

  /**
   * Defines entity types.
   */
  public enum EntityType {
    FLOW, PROCEDURE, MAP_REDUCE
  }

  public static LoggingContext getLoggingContext(Map<String, String> tags) {
    // Tags are empty, cannot determine logging context.
    if (tags == null || tags.isEmpty()) {
      throw new IllegalArgumentException("Tags are empty, cannot determine logging context");
    }

    String accountId = tags.get(AccountLoggingContext.TAG_ACCOUNT_ID);
    String applicationId = tags.get(ApplicationLoggingContext.TAG_APPLICATION_ID);

    // No account id or application id present.
    if (accountId == null || applicationId == null) {
      throw new IllegalArgumentException("No account id or application id present");
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

    throw new IllegalArgumentException("Unsupported logging context");
  }

  public static LoggingContext getLoggingContext(String accountId, String applicationId, String entityId,
                                                 EntityType entityType) {
    switch (entityType) {
      case FLOW:
        return new FlowletLoggingContext(accountId, applicationId, entityId, "");
      case PROCEDURE:
        return new ProcedureLoggingContext(accountId, applicationId, entityId);
      case MAP_REDUCE:
        return new MapReduceLoggingContext(accountId, applicationId, entityId);
      default:
        throw new IllegalArgumentException(String.format("Illegal entity type for logging context: %s", entityType));
    }
  }

  public static Filter createFilter(LoggingContext loggingContext) {
    String accountId = loggingContext.getSystemTagsMap().get(ApplicationLoggingContext.TAG_ACCOUNT_ID).getValue();
    String applId = loggingContext.getSystemTagsMap().get(ApplicationLoggingContext.TAG_APPLICATION_ID).getValue();

    String tagName;
    String entityId;
    if (loggingContext instanceof FlowletLoggingContext) {
      tagName = FlowletLoggingContext.TAG_FLOW_ID;
      entityId = loggingContext.getSystemTagsMap().get(tagName).getValue();
    } else if (loggingContext instanceof ProcedureLoggingContext) {
      tagName = ProcedureLoggingContext.TAG_PROCEDURE_ID;
      entityId = loggingContext.getSystemTagsMap().get(tagName).getValue();
    } else if (loggingContext instanceof MapReduceLoggingContext) {
      tagName = MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID;
      entityId = loggingContext.getSystemTagsMap().get(tagName).getValue();
    } else if (loggingContext instanceof GenericLoggingContext) {
      entityId = loggingContext.getSystemTagsMap().get(GenericLoggingContext.TAG_ENTITY_ID).getValue();
      return createGenericFilter(accountId, applId, entityId);
    } else {
      throw new IllegalArgumentException(String.format("Invalid logging context: %s", loggingContext));
    }
    return new AndFilter(
      ImmutableList.of(new MdcExpression(FlowletLoggingContext.TAG_ACCOUNT_ID, accountId),
                       new MdcExpression(FlowletLoggingContext.TAG_APPLICATION_ID, applId),
                       new MdcExpression(tagName, entityId)
      )
    );
  }

  private static Filter createGenericFilter(String accountId, String applicationId, String entityId) {
    FlowletLoggingContext flowletLoggingContext = new FlowletLoggingContext(accountId, applicationId, entityId, "");
    ProcedureLoggingContext procedureLoggingContext = new ProcedureLoggingContext(accountId, applicationId, entityId);
    MapReduceLoggingContext mapReduceLoggingContext = new MapReduceLoggingContext(accountId, applicationId, entityId);

    return new OrFilter(
      ImmutableList.of(createFilter(flowletLoggingContext),
                       createFilter(procedureLoggingContext),
                       createFilter(mapReduceLoggingContext)
      )
    );
  }
}
