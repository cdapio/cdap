/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.messaging.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.TimeProvider;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.spi.MessageFetchRequest;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.MessagingServiceUtils;
import io.cdap.cdap.messaging.MessagingUtils;
import io.cdap.cdap.messaging.spi.RollbackDetail;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.tephra.TxConstants;
import org.apache.tephra.util.TxUtils;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The core implementation of {@link MessagingService}. It interacts with {@link MessageTable} and
 * {@link PayloadTable} directly to provide the messaging functionality.
 */
@ThreadSafe
public class CoreMessagingService extends AbstractIdleService implements MessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(CoreMessagingService.class);

  private final CConfiguration cConf;
  private final TableFactory tableFactory;
  private final LoadingCache<TopicId, TopicMetadata> topicCache;
  private final LoadingCache<TopicId, ConcurrentMessageWriter> messageTableWriterCache;
  private final LoadingCache<TopicId, ConcurrentMessageWriter> payloadTableWriterCache;
  private final TimeProvider timeProvider;
  private final MetricsCollectionService metricsCollectionService;
  private final long txMaxLifeTimeInMillis;

  @Inject
  protected CoreMessagingService(
      CConfiguration cConf,
      TableFactory tableFactory,
      MetricsCollectionService metricsCollectionService) {
    this(cConf, tableFactory, TimeProvider.SYSTEM_TIME, metricsCollectionService);
  }

  @VisibleForTesting
  CoreMessagingService(
      CConfiguration cConf,
      TableFactory tableFactory,
      TimeProvider timeProvider,
      MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.tableFactory = tableFactory;
    this.topicCache = createTopicCache();
    this.messageTableWriterCache = createTableWriterCache(true, cConf);
    this.payloadTableWriterCache = createTableWriterCache(false, cConf);
    this.timeProvider = timeProvider;

    // Due to circular dependency in our class hierarchy (which is bad), we cannot use
    // metricsCollectionService
    // to construct metricsContext in here. The circular dependency is

    // metrics collection ->
    //   metrics store ->
    //    lineage dataset framework ->
    //      audit publisher ->
    //        messaging service ->
    //          "metrics collection"
    this.metricsCollectionService = metricsCollectionService;
    this.txMaxLifeTimeInMillis =
        TimeUnit.SECONDS.toMillis(
            cConf.getLong(
                TxConstants.Manager.CFG_TX_MAX_LIFETIME,
                TxConstants.Manager.DEFAULT_TX_MAX_LIFETIME));
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata)
      throws TopicAlreadyExistsException, IOException {
    try (MetadataTable metadataTable = createMetadataTable()) {
      Map<String, String> properties = createDefaultProperties();
      properties.putAll(topicMetadata.getProperties());
      metadataTable.createTopic(
          new DefaultTopicMetadata(topicMetadata.getTopicId(), properties, true));
    }
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException {
    try (MetadataTable metadataTable = createMetadataTable()) {
      Map<String, String> properties = createDefaultProperties();
      properties.putAll(topicMetadata.getProperties());
      metadataTable.updateTopic(
          new DefaultTopicMetadata(topicMetadata.getTopicId(), properties, true));
      topicCache.invalidate(topicMetadata.getTopicId());
    }
  }

  @Override
  public void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    try (MetadataTable metadataTable = createMetadataTable()) {
      metadataTable.deleteTopic(topicId);
      topicCache.invalidate(topicId);
      messageTableWriterCache.invalidate(topicId);
      payloadTableWriterCache.invalidate(topicId);
    }
  }

  @Override
  public Map<String, String> getTopicMetadataProperties(TopicId topicId)
      throws TopicNotFoundException, IOException {
    return getTopic(topicId).getProperties();
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    try (MetadataTable metadataTable = createMetadataTable()) {
      return metadataTable.listTopics(namespaceId);
    }
  }

  @Nullable
  @Override
  public RollbackDetail publish(StoreRequest request) throws TopicNotFoundException, IOException {
    try {
      TopicMetadata metadata = topicCache.get(request.getTopicId());
      if (request.isTransactional()) {
        ensureValidTxLifetime(request.getTransactionWritePointer());
      }
      return messageTableWriterCache.get(request.getTopicId()).persist(request, metadata);
    } catch (ExecutionException e) {
      Throwable cause = Objects.firstNonNull(e.getCause(), e);
      Throwables.propagateIfPossible(cause, TopicNotFoundException.class, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void storePayload(StoreRequest request) throws TopicNotFoundException, IOException {
    try {
      TopicMetadata metadata = topicCache.get(request.getTopicId());
      payloadTableWriterCache.get(request.getTopicId()).persist(request, metadata);
    } catch (ExecutionException e) {
      Throwable cause = Objects.firstNonNull(e.getCause(), e);
      Throwables.propagateIfPossible(cause, TopicNotFoundException.class, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void rollback(TopicId topicId, RollbackDetail rollbackDetail)
      throws TopicNotFoundException, IOException {
    TopicMetadata metadata = new DefaultTopicMetadata(topicId, getTopicMetadataProperties(topicId));
    Exception failure = null;
    try (MessageTable messageTable = createMessageTable(metadata)) {
      messageTable.rollback(metadata, rollbackDetail);
    } catch (Exception e) {
      failure = e;
    }

    // Throw if there is any failure in rollback.
    if (failure != null) {
      Throwables.propagateIfPossible(failure, TopicNotFoundException.class, IOException.class);
      throw Throwables.propagate(failure);
    }
  }

  @Override
  protected void startUp() throws Exception {
    tableFactory.init();
    Queue<TopicId> asyncCreationTopics = new LinkedList<>();

    Set<TopicId> systemTopics = MessagingServiceUtils.getSystemTopics(cConf, true);
    for (TopicId topic : systemTopics) {
      createSystemTopic(topic, asyncCreationTopics);
    }

    if (!asyncCreationTopics.isEmpty()) {
      startAsyncTopicCreation(asyncCreationTopics, 5, TimeUnit.SECONDS);
    }

    LOG.info("Core Messaging Service started");
  }

  /**
   * Creates the given topic if it is not yet created. Adds a topic to the {@code
   * creationFailureTopics} if creation fails.
   */
  private void createSystemTopic(TopicId topicId, Queue<TopicId> creationFailureTopics) {
    try {
      createTopicIfNotExists(topicId);
    } catch (Exception e) {
      // If failed, add it to a list so that the retry will happen asynchronously
      LOG.warn("Topic {} creation failed with exception {}. Will retry.", topicId, e.getMessage());
      LOG.debug("Topic {} creation failure stacktrace", topicId, e);
      creationFailureTopics.add(topicId);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    messageTableWriterCache.invalidateAll();
    payloadTableWriterCache.invalidateAll();
    Closeables.closeQuietly(tableFactory);
    LOG.info("Core Messaging Service stopped");
  }

  private void ensureValidTxLifetime(long transactionWritePointer) throws IOException {
    long txTimestamp = TxUtils.getTimestamp(transactionWritePointer);
    boolean validLifetime = (txTimestamp + txMaxLifeTimeInMillis) > System.currentTimeMillis();
    if (!validLifetime) {
      throw new IOException(
          String.format(
              "Transaction %s has exceeded max lifetime %s ms",
              transactionWritePointer, txMaxLifeTimeInMillis));
    }
  }

  /**
   * Starts a thread to create the give list of topics. The thread will keep trying the creation
   * until all of the given topics are created.
   */
  private void startAsyncTopicCreation(
      final Queue<TopicId> topics, final long delay, final TimeUnit unit) {
    final ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor(
            Threads.createDaemonThreadFactory("async-topic-creation"));

    executor.schedule(
        new Runnable() {
          @Override
          public void run() {
            Iterator<TopicId> iterator = topics.iterator();
            while (iterator.hasNext()) {
              TopicId topicId = iterator.next();
              try {
                createTopicIfNotExists(topicId);
                // Successfully created, remove it from the async list
                iterator.remove();
              } catch (Exception e) {
                LOG.warn(
                    "Topic {} creation failed with exception {}. Will retry.",
                    topicId,
                    e.getMessage());
                LOG.debug("Topic {} creation failure stacktrace", topicId, e);
              }
            }

            if (!topics.isEmpty()) {
              executor.schedule(this, delay, unit);
            }
          }
        },
        delay,
        unit);
  }

  /** Creates the given topic if it is not yet created. */
  private void createTopicIfNotExists(TopicId topicId) throws IOException {
    try {
      createTopic(new DefaultTopicMetadata(topicId));
      LOG.debug("System topic created: {}", topicId);
    } catch (TopicAlreadyExistsException e) {
      // OK for the topic already created. Just log a debug as it happens on every restart.
      LOG.debug("System topic already exists: {}", topicId);
    }
  }

  /** Creates a loading cache for {@link TopicMetadata}. */
  private LoadingCache<TopicId, TopicMetadata> createTopicCache() {
    return CacheBuilder.newBuilder()
        .build(
            new CacheLoader<TopicId, TopicMetadata>() {
              @Override
              public TopicMetadata load(TopicId topicId) throws Exception {
                try (MetadataTable metadataTable = createMetadataTable()) {
                  return metadataTable.getMetadata(topicId);
                }
              }
            });
  }

  /**
   * Creates a {@link LoadingCache} for {@link ConcurrentMessageWriter} for writing to {@link
   * MessageTable} or {@link PayloadTable}.
   *
   * @param messageTable {@code true} for building a cache for the {@link MessageTable}; {@code
   *     false} for the {@link PayloadTable}
   * @param cConf the system configuration
   * @return a {@link LoadingCache} for
   */
  private LoadingCache<TopicId, ConcurrentMessageWriter> createTableWriterCache(
      final boolean messageTable, final CConfiguration cConf) {
    long expireSecs = cConf.getLong(Constants.MessagingSystem.TABLE_CACHE_EXPIRATION_SECONDS);

    return CacheBuilder.newBuilder()
        .expireAfterAccess(expireSecs, TimeUnit.SECONDS)
        .removalListener(
            new RemovalListener<TopicId, ConcurrentMessageWriter>() {
              @Override
              public void onRemoval(
                  RemovalNotification<TopicId, ConcurrentMessageWriter> notification) {
                ConcurrentMessageWriter writer = notification.getValue();
                if (writer != null) {
                  try {
                    writer.close();
                  } catch (IOException e) {
                    LOG.warn(
                        "Exception raised when closing message writer for topic {}",
                        notification.getKey(),
                        e);
                  }
                }
              }
            })
        .build(
            new CacheLoader<TopicId, ConcurrentMessageWriter>() {
              @Override
              public ConcurrentMessageWriter load(TopicId topicId) throws Exception {
                TopicMetadata metadata =
                    new DefaultTopicMetadata(topicId, getTopicMetadataProperties(topicId));
                StoreRequestWriter<?> messagesWriter =
                    messageTable
                        ? new MessageTableStoreRequestWriter(
                            createMessageTable(metadata), timeProvider)
                        : new PayloadTableStoreRequestWriter(
                            createPayloadTable(metadata), timeProvider);

                MetricsContext metricsContext =
                    metricsCollectionService.getContext(
                        ImmutableMap.of(
                            Constants.Metrics.Tag.COMPONENT,
                            Constants.Service.MESSAGING_SERVICE,
                            Constants.Metrics.Tag.INSTANCE_ID,
                            cConf.get(Constants.MessagingSystem.CONTAINER_INSTANCE_ID, "0"),
                            Constants.Metrics.Tag.NAMESPACE,
                            topicId.getNamespace(),
                            Constants.Metrics.Tag.TABLE,
                            messageTable ? "message" : "payload"));

                return new ConcurrentMessageWriter(messagesWriter, metricsContext);
              }
            });
  }

  /** Creates a new instance of {@link MetadataTable}. */
  private MetadataTable createMetadataTable() throws IOException {
    return tableFactory.createMetadataTable();
  }

  private MessageTable createMessageTable(@SuppressWarnings("unused") TopicMetadata topicMetadata)
      throws IOException {
    return tableFactory.createMessageTable(topicMetadata);
  }

  private PayloadTable createPayloadTable(@SuppressWarnings("unused") TopicMetadata topicMetadata)
      throws IOException {
    return tableFactory.createPayloadTable(topicMetadata);
  }

  /** Creates default topic properties based on {@link CConfiguration}. */
  private Map<String, String> createDefaultProperties() {
    Map<String, String> properties = new HashMap<>();

    // Default properties
    properties.put(
        DefaultTopicMetadata.TTL_KEY,
        cConf.get(Constants.MessagingSystem.TOPIC_DEFAULT_TTL_SECONDS));
    properties.put(
        DefaultTopicMetadata.GENERATION_KEY, MessagingUtils.Constants.DEFAULT_GENERATION);
    return properties;
  }

  private TopicMetadata getTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    try {
      return topicCache.get(topicId);
    } catch (ExecutionException e) {
      Throwable cause = Objects.firstNonNull(e.getCause(), e);
      Throwables.propagateIfPossible(cause, TopicNotFoundException.class, IOException.class);
      throw Throwables.propagate(e.getCause());
    }
  }

  @Override
  public CloseableIterator<RawMessage> fetch(MessageFetchRequest messageFetchRequest)
      throws TopicNotFoundException, IOException {
    TopicMetadata metadata = getTopic(messageFetchRequest.getTopicId());

    MessageTable messageTable = createMessageTable(metadata);
    try {
      return new MessageCloseableIterator(
          metadata, messageTable, () -> createPayloadTable(metadata), messageFetchRequest);
    } catch (Throwable t) {
      closeQuietly(messageTable);
      throw t;
    }
  }

  /**
   * Creates a {@link MessageId} from another message id by copying the publish timestamp and
   * sequence id.
   */
  private MessageId createMessageTableMessageId(MessageId messageId) {
    // Create a new MessageId with write timestamp and payload seqId = 0
    byte[] rawId = new byte[MessageId.RAW_ID_SIZE];
    MessageId.putRawId(
        messageId.getPublishTimestamp(), messageId.getSequenceId(), 0L, (short) 0, rawId, 0);
    return new MessageId(rawId);
  }

  /**
   * Creates a raw message id from the given {@link MessageTable.Entry} and {@link
   * PayloadTable.Entry}.
   *
   * @param messageEntry entry in the message table representing a message
   * @param payloadEntry an optional entry in the payload table if the message payload is stored in
   *     the Payload Table
   * @return a byte array representing the raw message id.
   */
  private byte[] createMessageId(
      MessageTable.Entry messageEntry, @Nullable PayloadTable.Entry payloadEntry) {
    long writeTimestamp = payloadEntry == null ? 0L : payloadEntry.getPayloadWriteTimestamp();
    short payloadSeqId = payloadEntry == null ? 0 : payloadEntry.getPayloadSequenceId();
    return createMessageId(messageEntry, writeTimestamp, payloadSeqId);
  }

  /**
   * Creates a raw message id from the given {@link MessageTable.Entry} and the payload write
   * timestamp and sequence id.
   *
   * @param messageEntry entry in the message table representing a message
   * @param payloadWriteTimestamp the timestamp that the entry was written to the payload table.
   * @param payloadSeqId the sequence id generated when the entry was written to the payload table.
   * @return a byte array representing the raw message id.
   */
  private byte[] createMessageId(
      MessageTable.Entry messageEntry, long payloadWriteTimestamp, short payloadSeqId) {
    byte[] rawId = new byte[MessageId.RAW_ID_SIZE];
    MessageId.putRawId(
        messageEntry.getPublishTimestamp(),
        messageEntry.getSequenceId(),
        payloadWriteTimestamp,
        payloadSeqId,
        rawId,
        0);
    return rawId;
  }

  /**
   * Calls the {@link AutoCloseable#close()} on the given {@link AutoCloseable} without throwing
   * exception. If there is exception raised, it will be logged but never thrown out.
   */
  private void closeQuietly(@Nullable AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Throwable t) {
      LOG.warn("Exception raised when closing Closeable {}", closeable, t);
    }
  }

  /**
   * A {@link CloseableIterator} of {@link RawMessage} implementation that contains the core message
   * fetching logic by combine scanning on both {@link MessageTable} and {@link PayloadTable}.
   */
  private final class MessageCloseableIterator implements CloseableIterator<RawMessage> {

    private final CloseableIterator<MessageTable.Entry> messageIterator;

    private final TableProvider<PayloadTable> payloadTableProvider;

    private final TopicMetadata topicMetadata;
    private final TopicId topicId;
    private final MessageTable messageTable;
    private RawMessage nextMessage;
    private MessageTable.Entry messageEntry;
    private CloseableIterator<PayloadTable.Entry> payloadIterator;
    private MessageId startOffset;
    private boolean inclusive;
    private int messageLimit;

    private PayloadTable payloadTable;

    MessageCloseableIterator(
        TopicMetadata topicMetadata,
        MessageTable messageTable,
        TableProvider<PayloadTable> payloadTableProvider,
        MessageFetchRequest messageFetchRequest)
        throws IOException {
      this.topicMetadata = topicMetadata;
      this.topicId = topicMetadata.getTopicId();
      this.messageTable = messageTable;
      this.payloadTableProvider = payloadTableProvider;
      this.inclusive = messageFetchRequest.isIncludeStart();
      this.messageLimit = messageFetchRequest.getLimit();

      long ttl = topicMetadata.getTTL();
      startOffset =
          messageFetchRequest.getStartOffset() == null
              ? null
              : new MessageId(messageFetchRequest.getStartOffset());
      Long startTime = messageFetchRequest.getStartTime();

      // Lower bound of messages that are still valid
      long smallestPublishTime = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(ttl);

      CloseableIterator<MessageTable.Entry> messageIterator;
      // If there is no startOffset or if the publish time in the startOffset is smaller then TTL,
      // do the scanning based on time. The smallest start time should be the currentTime - TTL.
      if (startOffset == null || startOffset.getPublishTimestamp() < smallestPublishTime) {
        long fetchStartTime =
            Math.max(smallestPublishTime, startTime == null ? smallestPublishTime : startTime);
        messageIterator =
            messageTable.fetch(
                topicMetadata, fetchStartTime, messageLimit, messageFetchRequest.getTransaction());
      } else {
        // Start scanning based on the start message id
        if (startOffset.getPayloadWriteTimestamp() != 0L) {
          // This message ID refer to payload table. Scan the message table with the reference
          // message ID inclusively.
          messageIterator =
              messageTable.fetch(
                  topicMetadata,
                  createMessageTableMessageId(startOffset),
                  true,
                  messageLimit,
                  messageFetchRequest.getTransaction());
        } else {
          messageIterator =
              messageTable.fetch(
                  topicMetadata,
                  startOffset,
                  messageFetchRequest.isIncludeStart(),
                  messageLimit,
                  messageFetchRequest.getTransaction());
        }
      }
      this.messageIterator = messageIterator;
    }

    @Override
    public boolean hasNext() {
      if (messageLimit <= 0) {
        return false;
      }

      // Find the next message
      while (nextMessage == null) {
        // If there is a payload iterator and is not empty, read the next message from the it
        if (payloadIterator != null && payloadIterator.hasNext()) {
          PayloadTable.Entry payloadEntry = payloadIterator.next();
          // messageEntry is guaranteed to be non-null if payloadIterator is non-null
          nextMessage =
              new RawMessage.Builder()
                  .setId(createMessageId(messageEntry, payloadEntry))
                  .setPayload(payloadEntry.getPayload())
                  .build();
          break;
        }

        // If there is no payload iterator or it has been exhausted, read the next message from the
        // message iterator
        if (messageIterator.hasNext()) {
          messageEntry = messageIterator.next();
          if (messageEntry.isPayloadReference()) {
            // If the message entry is a reference to payload table, create the payload iterator
            try {
              if (payloadTable == null) {
                payloadTable = payloadTableProvider.get();
              }

              closeQuietly(payloadIterator);

              MessageId payloadStartOffset =
                  startOffset == null
                      ? new MessageId(createMessageId(messageEntry, null))
                      : new MessageId(
                          createMessageId(
                              messageEntry,
                              startOffset.getPayloadWriteTimestamp(),
                              startOffset.getPayloadSequenceId()));

              // If startOffset is not used, always fetch with inclusive.
              payloadIterator =
                  payloadTable.fetch(
                      topicMetadata,
                      messageEntry.getTransactionWritePointer(),
                      payloadStartOffset,
                      startOffset == null || inclusive,
                      messageLimit);
              // The start offset is only used for the first payloadIterator being constructed.
              startOffset = null;
            } catch (IOException e) {
              throw Throwables.propagate(e);
            }
          } else {
            // Otherwise, the message entry is the next message
            nextMessage =
                new RawMessage.Builder()
                    .setId(createMessageId(messageEntry, null))
                    .setPayload(messageEntry.getPayload())
                    .build();
          }
        } else {
          // If there is no more message from the message iterator as well, then no more message to
          // fetch
          break;
        }
      }
      // After the first message, all the sub-sequence table.fetch call should always include all
      // message.
      inclusive = true;
      return nextMessage != null;
    }

    @Override
    public RawMessage next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more message from " + topicId);
      }
      RawMessage message = nextMessage;
      nextMessage = null;
      messageLimit--;
      return message;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
    }

    @Override
    public void close() {
      closeQuietly(payloadIterator);
      closeQuietly(messageIterator);
      closeQuietly(payloadTable);
      closeQuietly(messageTable);
    }
  }
}
