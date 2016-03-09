/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.appender.kafka.KafkaTopic;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.kafka.KafkaLogEvent;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Plugin to process Log and generate metrics on Log level.
 */
public class LogMetricsPlugin extends AbstractKafkaLogProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(LogMetricsPlugin.class);
  private static final int ROW_KEY_PREFIX = 101;
  private static final String SYSTEM_METRIC_PREFIX = "services.log";
  private static final String APP_METRIC_PREFIX = "app.log";

  private final CheckpointManager checkpointManager;
  private final MetricsCollectionService metricsCollectionService;

  private final Map<Integer, Checkpoint> partitionCheckpoints = Maps.newConcurrentMap();
  private ListeningScheduledExecutorService scheduledExecutor;
  private CheckPointWriter checkPointWriter;

  @Inject
  public LogMetricsPlugin (MetricsCollectionService metricsCollectionService,
                           CheckpointManagerFactory checkpointManagerFactory) {
    this.metricsCollectionService = metricsCollectionService;
    this.checkpointManager = checkpointManagerFactory.create(KafkaTopic.getTopic(), ROW_KEY_PREFIX);
  }

  @Override
  public void init(Set<Integer> partitions) {
    super.init(partitions, checkpointManager);

    scheduledExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("log-saver-metrics-plugin")));

    partitionCheckpoints.clear();
    try {
      Map<Integer, Checkpoint> partitionMap = checkpointManager.getCheckpoint(partitions);
      for (Map.Entry<Integer, Checkpoint> partition : partitionMap.entrySet()) {
        partitionCheckpoints.put(partition.getKey(), partition.getValue());
      }
    } catch (Exception e) {
      LOG.error("Caught exception while reading checkpoint", e);
      throw Throwables.propagate(e);
    }

    checkPointWriter = new CheckPointWriter(checkpointManager, partitionCheckpoints);
    scheduledExecutor.scheduleWithFixedDelay(checkPointWriter, 100, 500, TimeUnit.MILLISECONDS);
  }

  @Override
  public void doProcess(KafkaLogEvent event) {

    LoggingContext context = event.getLoggingContext();
    Map<String, String> tags = LoggingContextHelper.getMetricsTags(context);
    MetricsContext collector = metricsCollectionService.getContext(tags);

    String metricName = getMetricName(tags.get(Constants.Metrics.Tag.NAMESPACE),
                                      event.getLogEvent().getLevel().toString().toLowerCase());

    if  (!(tags.containsKey(Constants.Metrics.Tag.COMPONENT) &&
           tags.get(Constants.Metrics.Tag.COMPONENT).equals(Constants.Service.METRICS_PROCESSOR))) {
      collector.increment(metricName, 1);
    }

    partitionCheckpoints.put(event.getPartition(),
                             new Checkpoint(event.getNextOffset(), event.getLogEvent().getTimeStamp()));

  }

  private String getMetricName(String namespace, String logLevel) {
    return namespace.equals(Id.Namespace.SYSTEM.getId()) ?
           String.format("%s.%s", SYSTEM_METRIC_PREFIX, logLevel) :
           String.format("%s.%s", APP_METRIC_PREFIX, logLevel);
  }

  @Override
  public void stop() {
    try {
      if (scheduledExecutor != null) {
        scheduledExecutor.shutdown();
        scheduledExecutor.awaitTermination(5, TimeUnit.MINUTES);
      }
      if (checkPointWriter != null) {
        checkPointWriter.flush();
      }
    } catch (Throwable th) {
      LOG.error("Caught exception while closing Log metrics plugin {}", th.getMessage(), th);
    }
  }

  @Override
  public Checkpoint getCheckpoint(int partition) {
    try {
      return checkpointManager.getCheckpoint(partition);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @VisibleForTesting
  CheckpointManager getCheckPointManager() {
    return this.checkpointManager;
  }
}
