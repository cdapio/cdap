/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.metadata.lineage.LineageDataset;
import co.cask.cdap.data2.metadata.writer.DataAccessLineage;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Service responsible for consuming metadata messages from TMS and persist it to metadata store.
 * This is a wrapping service to host multiple {@link AbstractMessagingSubscriberService}s for lineage, usage
 * and metadata subscriptions.
 */
public class MetadataSubscriberService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataSubscriberService.class);

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final RetryStrategy retryStrategy;
  private final MetricsContext baseMetricsContext;
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private final List<Service> subscriberServices;
  private DatasetId lineageDatasetId = LineageDataset.LINEAGE_DATASET_ID;

  @Inject
  MetadataSubscriberService(CConfiguration cConf, MessagingService messagingService,
                            DatasetFramework datasetFramework, TransactionSystemClient txClient,
                            MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.metadata.");
    this.baseMetricsContext = metricsCollectionService.getContext(ImmutableMap.of(
      Constants.Metrics.Tag.COMPONENT, Constants.Service.MASTER_SERVICES,
      Constants.Metrics.Tag.INSTANCE_ID, "0",
      Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace()));

    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, ImmutableMap.of(), null, null, messagingContext)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );

    // Adding all metadata related subscribers
    this.subscriberServices = Collections.singletonList(new DataAccessLineageSubscriber());
  }

  /**
   * Sets the {@link DatasetId} for the {@link LineageDataset}. This method is only for testing.
   */
  @VisibleForTesting
  public void setLineageDatasetId(DatasetId lineageDatasetId) {
    this.lineageDatasetId = lineageDatasetId;
  }

  @Override
  protected void startUp() throws Exception {
    // Start all subscriber services. All of them has no-op in start, so they shouldn't fail.
    Futures.successfulAsList(subscriberServices.stream().map(s -> {
      LOG.debug("Starting metadata subscriber {}", s.getClass().getSimpleName());
      return s.start();
    }).collect(Collectors.toList())).get();

    LOG.debug("All metadata subscribers started");
  }

  @Override
  protected void shutDown() throws Exception {
    // This never throw
    Futures.successfulAsList(subscriberServices.stream().map(s -> {
      LOG.debug("Stopping metadata subscriber {}", s.getClass().getSimpleName());
      return s.stop();
    }).collect(Collectors.toList())).get();

    for (Service service : subscriberServices) {
      // The service must have been stopped, and calling stop again will just return immediate with the
      // future that carries the stop state.
      try {
        service.stop().get();
      } catch (ExecutionException e) {
        LOG.warn("Exception raised when stopping service {}", service.getClass().getSimpleName(), e.getCause());
      }
    }

    LOG.debug("All metadata subscribers stopped");
  }

  /**
   * A message subscriber for consuming lineage messages from the {@link Constants.Lineage#TOPIC} configuration
   * and writes to {@link LineageDataset}.
   */
  private final class DataAccessLineageSubscriber extends AbstractMessagingSubscriberService<DataAccessLineage> {

    DataAccessLineageSubscriber() {
      super(NamespaceId.SYSTEM.topic(cConf.get(Constants.Lineage.TOPIC)),
            false, cConf.getInt(Constants.Lineage.EVENT_FETCH_SIZE),
            cConf.getLong(Constants.Lineage.EVENT_POLL_DELAY_MILLIS), retryStrategy,
            baseMetricsContext.childContext(ImmutableMap.of(
              Constants.Metrics.Tag.TOPIC, cConf.get(Constants.Lineage.TOPIC),
              Constants.Metrics.Tag.CONSUMER, "lineage.writer"
            )));
    }

    @Override
    protected MessagingContext getMessagingContext() {
      return messagingContext;
    }

    @Override
    protected Transactional getTransactional() {
      return transactional;
    }

    @Nullable
    @Override
    protected String loadMessageId(DatasetContext datasetContext) throws Exception {
      LineageDataset lineageDataset = LineageDataset.getLineageDataset(datasetContext,
                                                                       datasetFramework, lineageDatasetId);
      return lineageDataset.loadMessageId(getTopicId());
    }

    @Override
    protected void storeMessageId(DatasetContext datasetContext, String messageId) throws Exception {
      LineageDataset lineageDataset = LineageDataset.getLineageDataset(datasetContext,
                                                                       datasetFramework, lineageDatasetId);
      lineageDataset.storeMessageId(getTopicId(), messageId);
    }

    @Override
    protected DataAccessLineage decodeMessage(Message message) throws Exception {
      return GSON.fromJson(message.getPayloadAsString(), DataAccessLineage.class);
    }

    @Override
    protected void processMessages(DatasetContext datasetContext,
                                   Iterator<ImmutablePair<String, DataAccessLineage>> messages) throws Exception {
      LineageDataset lineageDataset = LineageDataset.getLineageDataset(datasetContext,
                                                                       datasetFramework, lineageDatasetId);
      while (messages.hasNext()) {
        DataAccessLineage lineage = messages.next().getSecond();
        if (lineage.getDatasetId() != null) {
          lineageDataset.addAccess(lineage.getProgramRunId(), lineage.getDatasetId(),
                                   lineage.getAccessType(), lineage.getAccessTime(), lineage.getComponentId());
        } else if (lineage.getStreamId() != null) {
          lineageDataset.addAccess(lineage.getProgramRunId(), lineage.getStreamId(),
                                   lineage.getAccessType(), lineage.getAccessTime(), lineage.getComponentId());
        } else {
          // This shouldn't happen
          LOG.warn("Missing dataset id from the lineage access information. Ignoring the message {}", lineage);
        }
      }
    }
  }
}
