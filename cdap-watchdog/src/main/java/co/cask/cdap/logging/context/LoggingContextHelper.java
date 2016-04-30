/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helper class for LoggingContext objects.
 */
public final class LoggingContextHelper {

  private static final String ACCOUNT_ID = ".accountId";
  private static final Logger LOG = LoggerFactory.getLogger(LoggingContext.class);

  private static final Map<String, String> LOG_TAG_TO_METRICS_TAG_MAP =
    ImmutableMap.<String, String>builder()
      .put(FlowletLoggingContext.TAG_FLOWLET_ID, Constants.Metrics.Tag.FLOWLET)
      .put(WorkflowLoggingContext.TAG_WORKFLOW_ID, Constants.Metrics.Tag.WORKFLOW)
      .put(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID, Constants.Metrics.Tag.MAPREDUCE)
      .put(SparkLoggingContext.TAG_SPARK_JOB_ID, Constants.Metrics.Tag.SPARK)
      .put(UserServiceLoggingContext.TAG_USERSERVICE_ID, Constants.Metrics.Tag.HANDLER)
      .put(WorkerLoggingContext.TAG_WORKER_ID, Constants.Metrics.Tag.WORKER)
    .build();

  private LoggingContextHelper() {}

  public static String getNamespacedBaseDir(String logBaseDir, String logPartition) {
    Preconditions.checkArgument(logBaseDir != null, "Log Base dir cannot be null");
    Preconditions.checkArgument(logPartition != null, "Log partition cannot be null");
    String [] partitions = logPartition.split(":");
    Preconditions.checkArgument(partitions.length == 3,
                                "Expected log partition to be in the format <ns>:<entity>:<sub-entity>");
    // don't care about the app or the program, only need the namespace
    GenericLoggingContext loggingContext = new GenericLoggingContext(partitions[0], partitions[1], partitions[2]);
    return loggingContext.getNamespacedLogBaseDir(logBaseDir);
  }

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
                                       tags.get(FlowletLoggingContext.TAG_FLOWLET_ID),
                                       tags.get(ApplicationLoggingContext.TAG_RUNID_ID),
                                       tags.get(ApplicationLoggingContext.TAG_INSTANCE_ID));
    } else if (tags.containsKey(WorkflowLoggingContext.TAG_WORKFLOW_ID)) {
      return new WorkflowLoggingContext(namespaceId, applicationId,
                                        tags.get(WorkflowLoggingContext.TAG_WORKFLOW_ID),
                                        tags.get(ApplicationLoggingContext.TAG_RUNID_ID));
    } else if (tags.containsKey(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID)) {
      return new MapReduceLoggingContext(namespaceId, applicationId,
                                         tags.get(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID),
                                         tags.get(ApplicationLoggingContext.TAG_RUNID_ID));
    } else if (tags.containsKey(SparkLoggingContext.TAG_SPARK_JOB_ID)) {
        return new SparkLoggingContext(namespaceId, applicationId, tags.get(SparkLoggingContext.TAG_SPARK_JOB_ID),
                                       tags.get(ApplicationLoggingContext.TAG_RUNID_ID));
    } else if (tags.containsKey(UserServiceLoggingContext.TAG_USERSERVICE_ID)) {
      if (!tags.containsKey(UserServiceLoggingContext.TAG_HANDLER_ID)) {
        return null;
      }
      return new UserServiceLoggingContext(namespaceId, applicationId,
                                           tags.get(UserServiceLoggingContext.TAG_USERSERVICE_ID),
                                           tags.get(UserServiceLoggingContext.TAG_HANDLER_ID),
                                           tags.get(ApplicationLoggingContext.TAG_RUNID_ID),
                                           tags.get(ApplicationLoggingContext.TAG_INSTANCE_ID));
    } else if (tags.containsKey(ServiceLoggingContext.TAG_SERVICE_ID)) {
      return new ServiceLoggingContext(systemId, componentId,
                                       tags.get(ServiceLoggingContext.TAG_SERVICE_ID));
    } else if (tags.containsKey(WorkerLoggingContext.TAG_WORKER_ID)) {
      return new WorkerLoggingContext(namespaceId, applicationId, tags.get(WorkerLoggingContext.TAG_WORKER_ID),
                                      tags.get(ApplicationLoggingContext.TAG_RUNID_ID),
                                      tags.get(ApplicationLoggingContext.TAG_INSTANCE_ID));
    }

    throw new IllegalArgumentException("Unsupported logging context");
  }

  public static LoggingContext getLoggingContext(String systemId, String componentId, String serviceId) {
    return new ServiceLoggingContext(systemId, componentId, serviceId);
  }

  public static LoggingContext getLoggingContext(String namespaceId, String applicationId, String entityId,
                                                 ProgramType programType) {
    return getLoggingContext(namespaceId, applicationId, entityId, programType, null, null);
  }

  public static LoggingContext getLoggingContextWithRunId(String namespaceId, String applicationId, String entityId,
                                                          ProgramType programType, String runId,
                                                          Map<String, String> systemArgs) {
    return getLoggingContext(namespaceId, applicationId, entityId, programType, runId, systemArgs);
  }

  public static LoggingContext getLoggingContext(String namespaceId, String applicationId, String entityId,
                                                 ProgramType programType, @Nullable String runId,
                                                 @Nullable Map<String, String> systemArgs) {
    switch (programType) {
      case FLOW:
        return new FlowletLoggingContext(namespaceId, applicationId, entityId, "", runId, null);
      case WORKFLOW:
        return new WorkflowLoggingContext(namespaceId, applicationId, entityId, runId);
      case MAPREDUCE:
        if (systemArgs != null && systemArgs.containsKey("workflowRunId")) {
          String workflowRunId = systemArgs.get("workflowRunId");
          String workflowId = systemArgs.get("workflowName");
          return new WorkflowProgramLoggingContext(namespaceId, applicationId, workflowId, workflowRunId, programType,
                                                   entityId);
        }
        return new MapReduceLoggingContext(namespaceId, applicationId, entityId, runId);
      case SPARK:
        if (systemArgs != null && systemArgs.containsKey("workflowRunId")) {
          String workflowRunId = systemArgs.get("workflowRunId");
          String workflowId = systemArgs.get("workflowName");
          return new WorkflowProgramLoggingContext(namespaceId, applicationId, workflowId, workflowRunId, programType,
                                                   entityId);
        }
        return new SparkLoggingContext(namespaceId, applicationId, entityId, runId);
      case SERVICE:
        return new UserServiceLoggingContext(namespaceId, applicationId, entityId, "", runId, null);
      case WORKER:
        return new WorkerLoggingContext(namespaceId, applicationId, entityId, runId, null);
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

      LoggingContext.SystemTag entityTag = getEntityId(loggingContext);

      ImmutableList.Builder<Filter> filterBuilder = ImmutableList.builder();

      // For backward compatibility: The old logs before namespace have .accountId and developer as value so we don't
      // want them to get filtered out if they belong to this application and entity
      OrFilter namespaceFilter = new OrFilter(ImmutableList.of(new MdcExpression(
                                                                 NamespaceLoggingContext.TAG_NAMESPACE_ID, namespaceId),
                                                               new MdcExpression(ACCOUNT_ID,
                                                                                 Constants.DEVELOPER_ACCOUNT)));
      filterBuilder.add(namespaceFilter);
      filterBuilder.add(new MdcExpression(ApplicationLoggingContext.TAG_APPLICATION_ID, applId));
      filterBuilder.add(new MdcExpression(entityTag.getName(), entityTag.getValue()));

      if (loggingContext instanceof WorkflowProgramLoggingContext) {
        // Program is started by Workflow. Add Program information to filter.
        Map<String, LoggingContext.SystemTag> systemTagsMap = loggingContext.getSystemTagsMap();
        LoggingContext.SystemTag programTag
          = systemTagsMap.get(WorkflowProgramLoggingContext.TAG_WORKFLOW_MAP_REDUCE_ID);
        if (programTag != null) {
          filterBuilder.add(new MdcExpression(WorkflowProgramLoggingContext.TAG_WORKFLOW_MAP_REDUCE_ID,
                                              programTag.getValue()));
        }
        programTag = systemTagsMap.get(WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID);
        if (programTag != null) {
          filterBuilder.add(new MdcExpression(WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID,
                                              programTag.getValue()));
        }
      }

      // Add runid filter if required
      LoggingContext.SystemTag runId = loggingContext.getSystemTagsMap().get(ApplicationLoggingContext.TAG_RUNID_ID);
      if (runId != null && runId.getValue() != null) {
        filterBuilder.add(new MdcExpression(ApplicationLoggingContext.TAG_RUNID_ID, runId.getValue()));
      }

      return new AndFilter(filterBuilder.build());
    }
  }

  public static LoggingContext.SystemTag getEntityId(LoggingContext loggingContext) {
    final String tagName;
    if (loggingContext instanceof FlowletLoggingContext) {
      tagName = FlowletLoggingContext.TAG_FLOW_ID;
    } else if (loggingContext instanceof WorkflowLoggingContext) {
      tagName = WorkflowLoggingContext.TAG_WORKFLOW_ID;
    } else if (loggingContext instanceof MapReduceLoggingContext) {
      tagName = MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID;
    } else if (loggingContext instanceof SparkLoggingContext) {
      tagName = SparkLoggingContext.TAG_SPARK_JOB_ID;
    } else if (loggingContext instanceof UserServiceLoggingContext) {
      tagName = UserServiceLoggingContext.TAG_USERSERVICE_ID;
    } else if (loggingContext instanceof WorkerLoggingContext) {
      tagName = WorkerLoggingContext.TAG_WORKER_ID;
    } else {
      throw new IllegalArgumentException(String.format("Invalid logging context: %s", loggingContext));
    }

    final String entityId = loggingContext.getSystemTagsMap().get(tagName).getValue();
    return new LoggingContext.SystemTag() {
      @Override
      public String getName() {
        return tagName;
      }

      @Override
      public String getValue() {
        return entityId;
      }
    };
  }

  public static Map<String, String> getMetricsTags(LoggingContext context) throws IllegalArgumentException {
    if (context instanceof ServiceLoggingContext) {
     return getMetricsTagsFromSystemContext((ServiceLoggingContext) context);
    } else {
      return getMetricsTagsFromLoggingContext(context);
    }
  }

  private static Map<String, String> getMetricsTagsFromLoggingContext(LoggingContext context) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    Map<String, LoggingContext.SystemTag> loggingTags = context.getSystemTagsMap();
    String namespace = getValueFromTag(loggingTags.get(NamespaceLoggingContext.TAG_NAMESPACE_ID));

    if (namespace == null || namespace.isEmpty()) {
      throw new IllegalArgumentException("Cannot find namespace in logging context");
    }
    builder.put(Constants.Metrics.Tag.NAMESPACE, namespace);

    String applicationId = getValueFromTag(loggingTags.get(ApplicationLoggingContext.TAG_APPLICATION_ID));
    // Must be an application
    if (applicationId == null) {
      throw new IllegalArgumentException("Missing application id");
    }

    builder.put(Constants.Metrics.Tag.APP, applicationId);
    LoggingContext.SystemTag entityId = getEntityId(context);
    String entityName = getMetricsTagNameFromLoggingContext(entityId);
    if (entityName != null) {
      builder.put(entityName, entityId.getValue());
    }
    return builder.build();
  }


  private static String getMetricsTagNameFromLoggingContext(LoggingContext.SystemTag tag) {
    return LOG_TAG_TO_METRICS_TAG_MAP.get(tag.getName());
  }

  private static String getValueFromTag(LoggingContext.SystemTag tag) {
    return tag == null ? null : tag.getValue();
  }

  private static Map<String, String> getMetricsTagsFromSystemContext(ServiceLoggingContext context) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put(Constants.Metrics.Tag.NAMESPACE, Id.Namespace.SYSTEM.getId());
    builder.put(Constants.Metrics.Tag.COMPONENT,
                context.getSystemTagsMap().get(ServiceLoggingContext.TAG_SERVICE_ID).getValue());
    return builder.build();
  }
}
