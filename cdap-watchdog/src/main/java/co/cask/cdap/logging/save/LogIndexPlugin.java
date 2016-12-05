/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.kafka.KafkaLogEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Plugin to index Log with Solr.
 */
public class LogIndexPlugin extends AbstractKafkaLogProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(LogMetricsPlugin.class);
  private static final int ROW_KEY_PREFIX = 102;

  private final CheckpointManager checkpointManager;
  private final CConfiguration cConfig;

  private SolrClient solrClient;

  private final Map<Integer, Checkpoint> partitionCheckpoints = Maps.newConcurrentMap();
  private ListeningScheduledExecutorService scheduledExecutor;
  private CheckPointWriter checkPointWriter;
  private int partition;

  LogIndexPlugin(CheckpointManagerFactory checkpointManagerFactory, CConfiguration cConfig) {
    this.cConfig = cConfig;
    this.checkpointManager = checkpointManagerFactory.create(cConfig.get(Constants.Logging.KAFKA_TOPIC),
                                                             ROW_KEY_PREFIX);
  }

  @Override
  public void init(int partition) throws Exception {
    long checkpointIntervalMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_CHECKPOINT_INTERVAL_MS,
                                                LoggingConfiguration.DEFAULT_LOG_SAVER_CHECKPOINT_INTERVAL_MS);
    Preconditions.checkArgument(checkpointIntervalMs > 0,
                                "Checkpoint interval is invalid: %s", checkpointIntervalMs);

    Checkpoint checkpoint = checkpointManager.getCheckpoint(partition);
    super.init(checkpoint);
    this.partition = partition;

    String solrUrl = cConfig.get(LoggingConfiguration.SOLR_URL);
    if (solrUrl == null) {
      throw new IllegalStateException("Solr URL not provided in configuration");
    }
    LOG.info("Initializing Solr client");
    solrClient = new HttpSolrClient(solrUrl);

    scheduledExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("log-index-plugin")));

    partitionCheckpoints.clear();
    partitionCheckpoints.put(partition, checkpoint);

    checkPointWriter = new CheckPointWriter(checkpointManager, partitionCheckpoints);
    scheduledExecutor.scheduleWithFixedDelay(checkPointWriter, checkpointIntervalMs, checkpointIntervalMs,
                                             TimeUnit.MILLISECONDS);
  }

  public void doProcess(Iterator<KafkaLogEvent> events) {
    KafkaLogEvent event = null;
    while (events.hasNext()) {
      event = events.next();
      LoggingContext context = event.getLoggingContext();
      SolrInputDocument document = new SolrInputDocument();
      event.getLogEvent();
      for (Map.Entry<String, Object> entry : getIndexFields(event, context).entrySet()) {
        document.addField(entry.getKey(), entry.getValue());
      }
      try {
        solrClient.add(document);
      } catch (Exception e) {
        LOG.error("Got exception for event {}", event, e);
      }
    }

    // Commit only if at least one event was added
    if (event != null) {
      try {
        solrClient.commit();
        partitionCheckpoints.put(event.getPartition(),
                                 new Checkpoint(event.getNextOffset(), event.getLogEvent().getTimeStamp()));
      } catch (Exception e) {
        LOG.error("Got exception while committing solr index", e);
      }
    }
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
      LOG.info("Closing Solr client");
      solrClient.close();
    } catch (Throwable th) {
      LOG.error("Caught exception while closing Log metrics plugin {}", th.getMessage(), th);
    }
  }

  @Override
  public Checkpoint getCheckpoint() {
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

  private Map<String, Object> getIndexFields(KafkaLogEvent kafkaLogEvent, LoggingContext context) {
    Map<String, String> tags = LoggingContextHelper.getMetricsTags(context);
    ILoggingEvent event = kafkaLogEvent.getLogEvent();
    LOG.info("Adding exception to Solr: {}", ThrowableProxyUtil.asString(event.getThrowableProxy()));
    return ImmutableMap.<String, Object>builder()
      .put("timestamp", event.getTimeStamp())
      .put("message", event.getFormattedMessage())
      .put("level", event.getLevel())
      .put("namespace", tags.get(Constants.Metrics.Tag.NAMESPACE))
      .put("application", tags.get(Constants.Metrics.Tag.APP))
      .put("program", LoggingContextHelper.getProgramName(context))
      .put("run", tags.get(Constants.Metrics.Tag.RUN_ID))
      .put("exception", ThrowableProxyUtil.asString(event.getThrowableProxy()))
      .build();
  }
}
