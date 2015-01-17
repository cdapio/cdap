/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.logging.context;

import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.ComponentLoggingContext;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.logging.SystemLoggingContext;
import co.cask.cdap.logging.filter.AndFilter;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.filter.MdcExpression;
import co.cask.cdap.logging.filter.OrFilter;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableList;

import java.util.Map;

/**
 * Helper class for LoggingContext objects.
 */
public final class LoggingContextHelper {
  private LoggingContextHelper() {}

  public static LoggingContext getLoggingContext(Map<String, String> tags) {
    // Tags are empty, cannot determine logging context.
    if (tags == null || tags.isEmpty()) {
      throw new IllegalArgumentException("Tags are empty, cannot determine logging context");
    }

    String namespaceId = tags.get(NamespaceLoggingContext.TAG_NAMESPACE_ID);
    String applicationId = tags.get(ApplicationLoggingContext.TAG_APPLICATION_ID);

    String systemId = tags.get(SystemLoggingContext.TAG_SYSTEM_ID);
    String componentId = tags.get(ComponentLoggingContext.TAG_COMPONENT_ID);

    // No namespace id or application id present.
    if (namespaceId == null || applicationId == null) {
      if (systemId == null || componentId == null) {
        throw new IllegalArgumentException("No namespace/application or system/component id present");
      }
    }

    if (tags.containsKey(FlowletLoggingContext.TAG_FLOW_ID)) {
      if (!tags.containsKey(FlowletLoggingContext.TAG_FLOWLET_ID)) {
        return null;
      }
      return new FlowletLoggingContext(namespaceId, applicationId, tags.get(FlowletLoggingContext.TAG_FLOW_ID),
                                       tags.get(FlowletLoggingContext.TAG_FLOWLET_ID));
    } else if (tags.containsKey(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID)) {
      return new MapReduceLoggingContext(namespaceId, applicationId,
                                         tags.get(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID));
    } else if (tags.containsKey(SparkLoggingContext.TAG_SPARK_JOB_ID)) {
        return new SparkLoggingContext(namespaceId, applicationId, tags.get(SparkLoggingContext.TAG_SPARK_JOB_ID));
    } else if (tags.containsKey(ProcedureLoggingContext.TAG_PROCEDURE_ID)) {
      return new ProcedureLoggingContext(namespaceId, applicationId,
                                         tags.get(ProcedureLoggingContext.TAG_PROCEDURE_ID));
    } else if (tags.containsKey(UserServiceLoggingContext.TAG_USERSERVICE_ID)) {
      if (!tags.containsKey(UserServiceLoggingContext.TAG_RUNNABLE_ID)) {
        return null;
      }
      return new UserServiceLoggingContext(namespaceId, applicationId,
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

  public static LoggingContext getLoggingContext(String namespaceId, String applicationId, String entityId,
                                                 ProgramType programType) {
    switch (programType) {
      case FLOW:
        return new FlowletLoggingContext(namespaceId, applicationId, entityId, "");
      case PROCEDURE:
        return new ProcedureLoggingContext(namespaceId, applicationId, entityId);
      case MAPREDUCE:
        return new MapReduceLoggingContext(namespaceId, applicationId, entityId);
      case SPARK:
        return new SparkLoggingContext(namespaceId, applicationId, entityId);
      case SERVICE:
        return new UserServiceLoggingContext(namespaceId, applicationId, entityId, "");
      default:
        throw new IllegalArgumentException(String.format("Illegal entity type for logging context: %s", programType));
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
      String namespaceId = loggingContext.getSystemTagsMap().get(ApplicationLoggingContext.TAG_NAMESPACE_ID).getValue();
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
      } else if (loggingContext instanceof SparkLoggingContext) {
        tagName = SparkLoggingContext.TAG_SPARK_JOB_ID;
        entityId = loggingContext.getSystemTagsMap().get(tagName).getValue();
      } else if (loggingContext instanceof UserServiceLoggingContext) {
        tagName = UserServiceLoggingContext.TAG_USERSERVICE_ID;
        entityId = loggingContext.getSystemTagsMap().get(tagName).getValue();
      } else if (loggingContext instanceof GenericLoggingContext) {
        entityId = loggingContext.getSystemTagsMap().get(GenericLoggingContext.TAG_ENTITY_ID).getValue();
        return createGenericFilter(namespaceId, applId, entityId);
      } else {
        throw new IllegalArgumentException(String.format("Invalid logging context: %s", loggingContext));
      }

      return new AndFilter(
        ImmutableList.of(new MdcExpression(FlowletLoggingContext.TAG_NAMESPACE_ID, namespaceId),
                         new MdcExpression(FlowletLoggingContext.TAG_APPLICATION_ID, applId),
                         new MdcExpression(tagName, entityId)
        )
      );
    }
  }

  private static Filter createGenericFilter(String namespaceId, String applicationId, String entityId) {
    FlowletLoggingContext flowletLoggingContext = new FlowletLoggingContext(namespaceId, applicationId, entityId, "");
    ProcedureLoggingContext procedureLoggingContext = new ProcedureLoggingContext(namespaceId, applicationId, entityId);
    MapReduceLoggingContext mapReduceLoggingContext = new MapReduceLoggingContext(namespaceId, applicationId, entityId);
    SparkLoggingContext sparkLoggingContext = new SparkLoggingContext(namespaceId, applicationId, entityId);
    UserServiceLoggingContext userServiceLoggingContext = new UserServiceLoggingContext(namespaceId, applicationId,
                                                                                        entityId, "");


    return new OrFilter(
      ImmutableList.of(createFilter(flowletLoggingContext),
                       createFilter(procedureLoggingContext),
                       createFilter(mapReduceLoggingContext),
                       createFilter(sparkLoggingContext),
                       createFilter(userServiceLoggingContext)
      )
    );
  }
}
