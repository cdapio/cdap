/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.context;

import com.continuuity.common.logging.AccountLoggingContext;
import com.continuuity.common.logging.ApplicationLoggingContext;
import com.continuuity.common.logging.ComponentLoggingContext;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.ServiceLoggingContext;
import com.continuuity.common.logging.SystemLoggingContext;
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
    FLOW, PROCEDURE, MAP_REDUCE, SERVICE
  }

  public static LoggingContext getLoggingContext(Map<String, String> tags) {
    // Tags are empty, cannot determine logging context.
    if (tags == null || tags.isEmpty()) {
      throw new IllegalArgumentException("Tags are empty, cannot determine logging context");
    }

    String accountId = tags.get(AccountLoggingContext.TAG_ACCOUNT_ID);
    String applicationId = tags.get(ApplicationLoggingContext.TAG_APPLICATION_ID);

    String systemId = tags.get(SystemLoggingContext.TAG_SYSTEM_ID);
    String componentId = tags.get(ComponentLoggingContext.TAG_COMPONENT_ID);

    // No account id or application id present.
    if (accountId == null || applicationId == null) {
      if (systemId == null || componentId == null) {
        throw new IllegalArgumentException("No account/application or system/component id present");
      }
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
    } else if (tags.containsKey(UserServiceLoggingContext.TAG_USERSERVICE_ID)) {
      if (!tags.containsKey(UserServiceLoggingContext.TAG_RUNNABLE_ID)) {
        return null;
      }
      return new UserServiceLoggingContext(accountId, applicationId,
                                           tags.get(UserServiceLoggingContext.TAG_USERSERVICE_ID),
                                           tags.get(UserServiceLoggingContext.TAG_RUNNABLE_ID));
    } else if (tags.containsKey(ServiceLoggingContext.TAG_SERVICE_ID)) {
      return new ServiceLoggingContext(systemId, componentId,
                                       tags.get(ServiceLoggingContext.TAG_SERVICE_ID));
    }

    throw new IllegalArgumentException("Unsupported logging context");
  }

  public static LoggingContext getLoggingContext(String systemId, String componentId, String serviceId) {
    return new ServiceLoggingContext(systemId, componentId, serviceId);
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
      case SERVICE:
        return new UserServiceLoggingContext(accountId, applicationId, entityId, "");
      default:
        throw new IllegalArgumentException(String.format("Illegal entity type for logging context: %s", entityType));
    }
  }

  public static Filter createFilter(LoggingContext loggingContext) {
    if (loggingContext instanceof ServiceLoggingContext) {
      String systemId = loggingContext.getSystemTagsMap().get(ServiceLoggingContext.TAG_SYSTEM_ID).getValue();
      String componentId = loggingContext.getSystemTagsMap().get(ServiceLoggingContext.TAG_COMPONENT_ID).getValue();
      String tagName = ServiceLoggingContext.TAG_SERVICE_ID;
      String entityId = loggingContext.getSystemTagsMap().get(ServiceLoggingContext.TAG_SERVICE_ID).getValue();
      return new AndFilter(
        ImmutableList.of(new MdcExpression(ServiceLoggingContext.TAG_SYSTEM_ID, systemId),
                         new MdcExpression(ServiceLoggingContext.TAG_COMPONENT_ID, componentId),
                         new MdcExpression(tagName, entityId)));
    } else {
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
      } else if (loggingContext instanceof UserServiceLoggingContext) {
        tagName = UserServiceLoggingContext.TAG_USERSERVICE_ID;
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
  }

  private static Filter createGenericFilter(String accountId, String applicationId, String entityId) {
    FlowletLoggingContext flowletLoggingContext = new FlowletLoggingContext(accountId, applicationId, entityId, "");
    ProcedureLoggingContext procedureLoggingContext = new ProcedureLoggingContext(accountId, applicationId, entityId);
    MapReduceLoggingContext mapReduceLoggingContext = new MapReduceLoggingContext(accountId, applicationId, entityId);
    UserServiceLoggingContext userServiceLoggingContext = new UserServiceLoggingContext(accountId, applicationId,
                                                                                        entityId, "");


    return new OrFilter(
      ImmutableList.of(createFilter(flowletLoggingContext),
                       createFilter(procedureLoggingContext),
                       createFilter(mapReduceLoggingContext),
                       createFilter(userServiceLoggingContext)
      )
    );
  }
}
