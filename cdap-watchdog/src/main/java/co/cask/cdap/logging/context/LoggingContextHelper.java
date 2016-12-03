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
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.logging.filter.AndFilter;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.filter.MdcExpression;
import co.cask.cdap.logging.filter.OrFilter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.Location;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Helper class for LoggingContext objects.
 */
public final class LoggingContextHelper {

  private static final String ACCOUNT_ID = ".accountId";

  private static final Map<String, String> LOG_TAG_TO_METRICS_TAG_MAP =
    ImmutableMap.<String, String>builder()
      .put(FlowletLoggingContext.TAG_FLOWLET_ID, Constants.Metrics.Tag.FLOWLET)
      .put(FlowletLoggingContext.TAG_FLOW_ID, Constants.Metrics.Tag.FLOW)
      .put(WorkflowLoggingContext.TAG_WORKFLOW_ID, Constants.Metrics.Tag.WORKFLOW)
      .put(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID, Constants.Metrics.Tag.MAPREDUCE)
      .put(SparkLoggingContext.TAG_SPARK_JOB_ID, Constants.Metrics.Tag.SPARK)
      .put(WorkflowProgramLoggingContext.TAG_WORKFLOW_MAP_REDUCE_ID, Constants.Metrics.Tag.MAPREDUCE)
      .put(WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID, Constants.Metrics.Tag.SPARK)
      .put(UserServiceLoggingContext.TAG_USER_SERVICE_ID, Constants.Metrics.Tag.SERVICE)
      .put(UserServiceLoggingContext.TAG_HANDLER_ID, Constants.Metrics.Tag.HANDLER)
      .put(WorkerLoggingContext.TAG_WORKER_ID, Constants.Metrics.Tag.WORKER)
      .put(ApplicationLoggingContext.TAG_INSTANCE_ID, Constants.Metrics.Tag.INSTANCE_ID)
    .build();

  private LoggingContextHelper() {}

  public static Location getNamespacedBaseDirLocation(final NamespacedLocationFactory namespacedLocationFactory,
                                                      final String logBaseDir, final NamespaceId namespaceId,
                                                      Impersonator impersonator) throws Exception {
    Preconditions.checkArgument(logBaseDir != null, "Log Base dir cannot be null");
    return impersonator.doAs(namespaceId, new Callable<Location>() {
      @Override
      public Location call() throws Exception {
        return namespacedLocationFactory.get(namespaceId.toId()).append(logBaseDir);
      }
    });
  }

  /**
   * Gives the {@link NamespaceId} for  the given logging context.
   *
   * @param loggingContext the {@link LoggingContext} whose namespace id needs to be found.
   * @return {@link NamespaceId} for the given logging context
   */
  public static NamespaceId getNamespaceId(LoggingContext loggingContext) {
    Preconditions.checkArgument(loggingContext.getSystemTagsMap().containsKey(NamespaceLoggingContext.TAG_NAMESPACE_ID),
                                String.format("Failed to identify the namespace in the logging context '%s' since " +
                                                "it does not contains a '%s'. LoggingContexts should have a " +
                                                "namespace.", loggingContext.getSystemTagsMap(),
                                              NamespaceLoggingContext.TAG_NAMESPACE_ID));
    return new NamespaceId(loggingContext.getSystemTagsMap()
                             .get(NamespaceLoggingContext.TAG_NAMESPACE_ID).getValue());
  }

  public static LoggingContext getLoggingContext(Map<String, String> tags) {
    // Tags are empty, cannot determine logging context.
    if (tags == null || tags.isEmpty()) {
      throw new IllegalArgumentException("Tags are empty, cannot determine logging context");
    }

    String namespaceId = getByNamespaceOrSystemID(tags);

    String applicationId = tags.get(ApplicationLoggingContext.TAG_APPLICATION_ID);

    String componentId = tags.get(ComponentLoggingContext.TAG_COMPONENT_ID);

    // No namespace id or either from application id or component id is not present
    if (namespaceId == null || (applicationId == null && componentId == null)) {
      throw new IllegalArgumentException("No namespace/application or system/component id present");
    }

    if (tags.containsKey(FlowletLoggingContext.TAG_FLOW_ID)) {
      if (!tags.containsKey(FlowletLoggingContext.TAG_FLOWLET_ID)) {
        return null;
      }
      return new FlowletLoggingContext(namespaceId, applicationId, tags.get(FlowletLoggingContext.TAG_FLOW_ID),
                                       tags.get(FlowletLoggingContext.TAG_FLOWLET_ID),
                                       tags.get(ApplicationLoggingContext.TAG_RUN_ID),
                                       tags.get(ApplicationLoggingContext.TAG_INSTANCE_ID));
    } else if (tags.containsKey(WorkflowLoggingContext.TAG_WORKFLOW_ID)) {
      if (tags.containsKey(WorkflowProgramLoggingContext.TAG_WORKFLOW_MAP_REDUCE_ID)) {
        return new WorkflowProgramLoggingContext(namespaceId, applicationId,
                                                 tags.get(WorkflowLoggingContext.TAG_WORKFLOW_ID),
                                                 tags.get(ApplicationLoggingContext.TAG_RUN_ID), ProgramType.MAPREDUCE,
                                                 tags.get(WorkflowProgramLoggingContext.TAG_WORKFLOW_MAP_REDUCE_ID),
                                                 tags.get(WorkflowProgramLoggingContext.TAG_WORKFLOW_PROGRAM_RUN_ID));
      }

      if (tags.containsKey(WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID)) {
        return new WorkflowProgramLoggingContext(namespaceId, applicationId,
                                                 tags.get(WorkflowLoggingContext.TAG_WORKFLOW_ID),
                                                 tags.get(ApplicationLoggingContext.TAG_RUN_ID), ProgramType.SPARK,
                                                 tags.get(WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID),
                                                 tags.get(WorkflowProgramLoggingContext.TAG_WORKFLOW_PROGRAM_RUN_ID));
      }

      return new WorkflowLoggingContext(namespaceId, applicationId,
                                        tags.get(WorkflowLoggingContext.TAG_WORKFLOW_ID),
                                        tags.get(ApplicationLoggingContext.TAG_RUN_ID));
    } else if (tags.containsKey(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID)) {
      return new MapReduceLoggingContext(namespaceId, applicationId,
                                         tags.get(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID),
                                         tags.get(ApplicationLoggingContext.TAG_RUN_ID));
    } else if (tags.containsKey(SparkLoggingContext.TAG_SPARK_JOB_ID)) {
        return new SparkLoggingContext(namespaceId, applicationId, tags.get(SparkLoggingContext.TAG_SPARK_JOB_ID),
                                       tags.get(ApplicationLoggingContext.TAG_RUN_ID));
    } else if (tags.containsKey(UserServiceLoggingContext.TAG_USER_SERVICE_ID)) {
      if (!tags.containsKey(UserServiceLoggingContext.TAG_HANDLER_ID)) {
        return null;
      }
      return new UserServiceLoggingContext(namespaceId, applicationId,
                                           tags.get(UserServiceLoggingContext.TAG_USER_SERVICE_ID),
                                           tags.get(UserServiceLoggingContext.TAG_HANDLER_ID),
                                           tags.get(ApplicationLoggingContext.TAG_RUN_ID),
                                           tags.get(ApplicationLoggingContext.TAG_INSTANCE_ID));
    } else if (tags.containsKey(ServiceLoggingContext.TAG_SERVICE_ID)) {
      return new ServiceLoggingContext(namespaceId, componentId,
                                       tags.get(ServiceLoggingContext.TAG_SERVICE_ID));
    } else if (tags.containsKey(WorkerLoggingContext.TAG_WORKER_ID)) {
      return new WorkerLoggingContext(namespaceId, applicationId, tags.get(WorkerLoggingContext.TAG_WORKER_ID),
                                      tags.get(ApplicationLoggingContext.TAG_RUN_ID),
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
                                                   entityId, runId);
        }
        return new MapReduceLoggingContext(namespaceId, applicationId, entityId, runId);
      case SPARK:
        if (systemArgs != null && systemArgs.containsKey("workflowRunId")) {
          String workflowRunId = systemArgs.get("workflowRunId");
          String workflowId = systemArgs.get("workflowName");
          return new WorkflowProgramLoggingContext(namespaceId, applicationId, workflowId, workflowRunId, programType,
                                                   entityId, runId);
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
      LoggingContext.SystemTag systemTag = getByNamespaceOrSystemID(loggingContext.getSystemTagsMap());
      if (systemTag == null) {
        throw new IllegalArgumentException("No namespace or system id present");
      }
      String systemId = systemTag.getValue();
      String componentId = loggingContext.getSystemTagsMap().get(ServiceLoggingContext.TAG_COMPONENT_ID).getValue();
      String tagName = ServiceLoggingContext.TAG_SERVICE_ID;
      String entityId = loggingContext.getSystemTagsMap().get(ServiceLoggingContext.TAG_SERVICE_ID).getValue();
      ImmutableList.Builder<Filter> filterBuilder = ImmutableList.builder();
      // In CDAP 3.5 we removed SystemLoggingContext which had tag .systemId and now we use .namespaceId but to
      // support backward compatibility have an or filter so that we can read old logs too. See CDAP-7482
      OrFilter namespaceFilter = new OrFilter(ImmutableList.of(new MdcExpression(
                                                                 NamespaceLoggingContext.TAG_NAMESPACE_ID, systemId),
                                                               new MdcExpression(ServiceLoggingContext.TAG_SYSTEM_ID,
                                                                                 systemId)));

      filterBuilder.add(namespaceFilter);
      filterBuilder.add(new MdcExpression(ServiceLoggingContext.TAG_COMPONENT_ID, componentId));
      filterBuilder.add(new MdcExpression(tagName, entityId));

      return new AndFilter(filterBuilder.build());

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
      LoggingContext.SystemTag runId = loggingContext.getSystemTagsMap().get(ApplicationLoggingContext.TAG_RUN_ID);
      if (runId != null && runId.getValue() != null) {
        filterBuilder.add(new MdcExpression(ApplicationLoggingContext.TAG_RUN_ID, runId.getValue()));
      }

      return new AndFilter(filterBuilder.build());
    }
  }

  public static String getProgramName(LoggingContext loggingContext) {
    final String tagname;
    if (loggingContext instanceof FlowletLoggingContext) {
      tagname = FlowletLoggingContext.TAG_FLOW_ID;
    } else if (loggingContext instanceof WorkflowLoggingContext) {
      tagname = WorkflowLoggingContext.TAG_WORKFLOW_ID;
    } else if (loggingContext instanceof MapReduceLoggingContext) {
      tagname = MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID;
    } else if (loggingContext instanceof SparkLoggingContext) {
      tagname = SparkLoggingContext.TAG_SPARK_JOB_ID;
    } else if (loggingContext instanceof UserServiceLoggingContext) {
      tagname = UserServiceLoggingContext.TAG_USER_SERVICE_ID;
    } else if (loggingContext instanceof WorkerLoggingContext) {
      tagname = WorkerLoggingContext.TAG_WORKER_ID;
    } else {
      throw new IllegalArgumentException(String.format("Invalid logging context: %s", loggingContext));
    }
    return loggingContext.getSystemTagsMap().get(tagname).getValue();
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
      tagName = UserServiceLoggingContext.TAG_USER_SERVICE_ID;
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

    if (Strings.isNullOrEmpty(namespace)) {
      throw new IllegalArgumentException("Cannot find namespace in logging context");
    }
    builder.put(Constants.Metrics.Tag.NAMESPACE, namespace);

    String applicationId = getValueFromTag(loggingTags.get(ApplicationLoggingContext.TAG_APPLICATION_ID));
    // Must be an application
    if (Strings.isNullOrEmpty(applicationId)) {
      throw new IllegalArgumentException("Missing application id");
    }
    builder.put(Constants.Metrics.Tag.APP, applicationId);

    // Logic to set the run id tag properly for all programs
    if (context instanceof WorkflowLoggingContext) {
      // For Workflow logs, we need to add a tag 'wfr' which corresponds to workflow run id.
      String workflowRunId = getValueFromTag(loggingTags.get(ApplicationLoggingContext.TAG_RUN_ID));
      if (!Strings.isNullOrEmpty(workflowRunId)) {
        builder.put(Constants.Metrics.Tag.WORKFLOW_RUN_ID, workflowRunId);
      }

      // For programs that are launched by Workflow, they use WorkflowProgramLoggingContext and they have two runids
      if (context instanceof WorkflowProgramLoggingContext) {
        String programRunId = getValueFromTag(loggingTags.get(
          WorkflowProgramLoggingContext.TAG_WORKFLOW_PROGRAM_RUN_ID));
        if (!Strings.isNullOrEmpty(programRunId)) {
          builder.put(Constants.Metrics.Tag.RUN_ID, programRunId);
        }
      }
    } else {
      // For all other programs, use the run id in ApplicationLoggingContext
      String runId = getValueFromTag(loggingTags.get(ApplicationLoggingContext.TAG_RUN_ID));
      if (!Strings.isNullOrEmpty(runId)) {
        builder.put(Constants.Metrics.Tag.RUN_ID, runId);
      }
    }

    // For all tags other than namespace, appid, and runid
    Collection<LoggingContext.SystemTag> systemTags = context.getSystemTags();
    for (LoggingContext.SystemTag systemTag : systemTags) {
      String entityName = LOG_TAG_TO_METRICS_TAG_MAP.get(systemTag.getName());
      if (entityName != null) {
        builder.put(entityName, systemTag.getValue());
      }
    }
    return builder.build();
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

  @Nullable
  private static <T> T getByNamespaceOrSystemID(Map<String, T> tags) {
    // Note: In CDAP 3.5 we removed  SystemLoggingContext which had tag .systemId so if NamespaceLoggingContext
    // .TAG_NAMESPACE_ID does not exist we use ServiceLoggingContext.TAG_SYSTEM_ID to support backward
    // compatibility for the logs which are in kafka and needs to be written to HDFS through log saver. See CDAP-7482
    return tags.containsKey(NamespaceLoggingContext.TAG_NAMESPACE_ID) ? tags.get
      (NamespaceLoggingContext.TAG_NAMESPACE_ID) : tags.get(ServiceLoggingContext.TAG_SYSTEM_ID);
  }
}
