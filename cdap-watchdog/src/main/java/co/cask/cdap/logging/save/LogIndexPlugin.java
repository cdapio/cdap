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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.kafka.KafkaLogEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
  private static final int ROW_KEY_PREFIX = 101;

  private final CheckpointManager checkpointManager;
  private final CConfiguration cConfig;

  private static final String SOLR_CORE_URL = "http://146.148.93.232:8983/solr/gettingstarted";
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

    solrClient = new HttpSolrClient.Builder(SOLR_CORE_URL).build();

    scheduledExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("log-index-plugin")));

    partitionCheckpoints.clear();
    partitionCheckpoints.put(partition, checkpoint);

    checkPointWriter = new CheckPointWriter(checkpointManager, partitionCheckpoints);
    scheduledExecutor.scheduleWithFixedDelay(checkPointWriter, checkpointIntervalMs, checkpointIntervalMs,
                                             TimeUnit.MILLISECONDS);
  }

  public void doProcess(Iterator<KafkaLogEvent> events) {

    while (events.hasNext()) {
      KafkaLogEvent event = events.next();
      LoggingContext context = event.getLoggingContext();
      SolrInputDocument document = new SolrInputDocument();
      event.getLogEvent();
      for (String field : getIndexFields(event, context)) {
        document.addField("q", field);
      }
      try {
        solrClient.add(document);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }

      partitionCheckpoints.put(event.getPartition(),
                               new Checkpoint(event.getNextOffset(), event.getLogEvent().getTimeStamp()));
    }
    try {
      solrClient.commit();
    } catch (Exception e) {
      LOG.error(e.getMessage());
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

  private String[] getIndexFields(KafkaLogEvent kafkaLogEvent, LoggingContext context) {
    Map<String, String> tags = LoggingContextHelper.getMetricsTags(context);
    ILoggingEvent event = kafkaLogEvent.getLogEvent();
    return new String[] {
      "timestamp:" + event.getTimeStamp(),
      "message:'" + event.getFormattedMessage(),
      "level:" + event.getLevel(),
      "namespace:" + tags.get(Constants.Metrics.Tag.NAMESPACE)
    };
  }
}
