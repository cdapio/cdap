/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.framework;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * An {@link Appender} that emits metrics about the logging events.
 */
public class MetricsLogAppender extends AppenderBase<ILoggingEvent> {
  private static final Map<String, String> EMPTY_MAP = new HashMap<>();
  private static final String SYSTEM_METRIC_PREFIX = "services.log";
  private static final String APP_METRIC_PREFIX = "app.log";
  private static final Map<String, String> LOG_TAG_TO_METRICS_TAG_MAP =
    ImmutableMap.<String, String>builder()
      .put(Constants.Logging.TAG_FLOWLET_ID, Constants.Metrics.Tag.FLOWLET)
      .put(Constants.Logging.TAG_FLOW_ID, Constants.Metrics.Tag.FLOW)
      .put(Constants.Logging.TAG_WORKFLOW_ID, Constants.Metrics.Tag.WORKFLOW)
      .put(Constants.Logging.TAG_MAP_REDUCE_JOB_ID, Constants.Metrics.Tag.MAPREDUCE)
      .put(Constants.Logging.TAG_SPARK_JOB_ID, Constants.Metrics.Tag.SPARK)
      .put(Constants.Logging.TAG_WORKFLOW_MAP_REDUCE_ID, Constants.Metrics.Tag.MAPREDUCE)
      .put(Constants.Logging.TAG_WORKFLOW_SPARK_ID, Constants.Metrics.Tag.SPARK)
      .put(Constants.Logging.TAG_USER_SERVICE_ID, Constants.Metrics.Tag.SERVICE)
      .put(Constants.Logging.TAG_HANDLER_ID, Constants.Metrics.Tag.HANDLER)
      .put(Constants.Logging.TAG_WORKER_ID, Constants.Metrics.Tag.WORKER)
      .put(Constants.Logging.TAG_INSTANCE_ID, Constants.Metrics.Tag.INSTANCE_ID)
      .build();

  private MetricsContext metricsContext;

  public MetricsLogAppender() {
    setName(getClass().getName());
  }

  @Override
  public void start() {
    super.start();
    // This shouldn't happen
    Preconditions.checkState(context instanceof AppenderContext,
                             "The context object is not an instance of %s", AppenderContext.class);
    this.metricsContext = ((AppenderContext) context).getMetricsContext();
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    // increment metrics for logging event
    Map<String, String> metricsTags = getMetricsTagsMap(eventObject.getMDCPropertyMap());
    if (!metricsTags.isEmpty()) {
      String metricName =
        getMetricName(metricsTags.get(Constants.Metrics.Tag.NAMESPACE),
                      eventObject.getLevel().toString().toLowerCase());
      // Don't increment metrics for logs from MetricsProcessor to avoid possibility of infinite loop
      if (!(metricsTags.containsKey(Constants.Metrics.Tag.COMPONENT) &&
        metricsTags.get(Constants.Metrics.Tag.COMPONENT).equals(Constants.Service.METRICS_PROCESSOR))) {
        // todo this is inefficient as childContext implementation creates new map should use metricsCollectionService
        MetricsContext childContext = metricsContext.childContext(metricsTags);
        childContext.increment(metricName, 1);
      }
    }
  }

  private String getMetricName(String namespace, String logLevel) {
    return namespace.equals(NamespaceId.SYSTEM.getNamespace()) ?
      String.format("%s.%s", SYSTEM_METRIC_PREFIX, logLevel) :
      String.format("%s.%s", APP_METRIC_PREFIX, logLevel);
  }

  // should improve the client logic around workflow runids to avoid this complexity
  private Map<String, String> getMetricsTagsMap(Map<String, String> propertyMap) {
    Map<String, String> metricsTags = new HashMap<>();

    if (!propertyMap.containsKey(Constants.Logging.TAG_NAMESPACE_ID)) {
      return EMPTY_MAP;
    }

    String namespaceId = propertyMap.get(Constants.Logging.TAG_NAMESPACE_ID);
    metricsTags.put(Constants.Metrics.Tag.NAMESPACE, namespaceId);

    if (NamespaceId.SYSTEM.getNamespace().equals(namespaceId)) {
      if (propertyMap.containsKey(Constants.Logging.TAG_SERVICE_ID)) {
        metricsTags.put(Constants.Metrics.Tag.COMPONENT, propertyMap.get(Constants.Logging.TAG_SERVICE_ID));
      }
    } else {
      // get application id
      if (propertyMap.containsKey(Constants.Logging.TAG_APPLICATION_ID)) {
        metricsTags.put(Constants.Metrics.Tag.APP, propertyMap.get(Constants.Logging.TAG_APPLICATION_ID));
        // if workflow
        if (propertyMap.containsKey(Constants.Logging.TAG_WORKFLOW_ID)) {
          if (propertyMap.containsKey(Constants.Logging.TAG_RUN_ID)) {
            // For Workflow logs, we need to add a tag 'wfr' which corresponds to workflow run id.
            metricsTags.put(Constants.Metrics.Tag.WORKFLOW_RUN_ID, propertyMap.get(Constants.Logging.TAG_RUN_ID));
          }
          // For programs that are launched by Workflow,
          // they use WorkflowProgramLoggingContext and they have two run-ids
          if (propertyMap.containsKey(Constants.Logging.TAG_WORKFLOW_PROGRAM_RUN_ID)) {
            metricsTags.put(Constants.Metrics.Tag.RUN_ID,
                            propertyMap.get(Constants.Logging.TAG_WORKFLOW_PROGRAM_RUN_ID));
          }
        } else {
          // For all other programs, use the run id in ApplicationLoggingContext
          if (propertyMap.containsKey(Constants.Logging.TAG_RUN_ID)) {
            // For Workflow logs, we need to add a tag 'wfr' which corresponds to workflow run id.
            metricsTags.put(Constants.Metrics.Tag.RUN_ID, propertyMap.get(Constants.Logging.TAG_RUN_ID));
          }
        }
        // For all tags other than namespace, appid, and runid
        for (Map.Entry<String, String> logTagToMetricTag : LOG_TAG_TO_METRICS_TAG_MAP.entrySet()) {
          if (propertyMap.containsKey(logTagToMetricTag.getKey())) {
            metricsTags.put(logTagToMetricTag.getValue(),
                            propertyMap.get(logTagToMetricTag.getKey()));
          }
        }
      }
    }
    return metricsTags;
  }
}
