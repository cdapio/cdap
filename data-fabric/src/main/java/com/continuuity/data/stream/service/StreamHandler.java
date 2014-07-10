/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.stream.StreamCoordinator;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data.stream.StreamPropertyListener;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumer;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.hash.Hashing;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST call to stream endpoints.
 *
 * TODO: Currently stream "dataset" is implementing old dataset API, hence not supporting multi-tenancy.
 */
@Path(Constants.Gateway.GATEWAY_VERSION + "/streams")
public final class StreamHandler extends AuthenticatedHttpHandler {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);
  private final CConfiguration cConf;
  private final StreamAdmin streamAdmin;
  private final ConcurrentStreamWriter streamWriter;

  // TODO: Need to make the decision of whether this should be inside StreamAdmin or not.
  // Currently is here to align with the existing Reactor organization that dataset admin is not aware of MDS
  private final StreamMetaStore streamMetaStore;

  // A timed cache from consumer id (group id) to stream consumer.
  private final LoadingCache<ConsumerCacheKey, StreamDequeuer> dequeuerCache;

  @Inject
  public StreamHandler(CConfiguration cConf, Authenticator authenticator,
                       StreamCoordinator streamCoordinator, StreamAdmin streamAdmin, StreamMetaStore streamMetaStore,
                       StreamConsumerFactory streamConsumerFactory, StreamFileWriterFactory writerFactory,
                       TransactionExecutorFactory executorFactory, MetricsCollectionService metricsCollectionService) {
    super(authenticator);
    this.cConf = cConf;
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;
    this.dequeuerCache = createDequeuerCache(cConf, streamConsumerFactory, executorFactory, streamCoordinator);

    MetricsCollector collector = metricsCollectionService.getCollector(MetricsScope.REACTOR, getMetricsContext(), "0");
    this.streamWriter = new ConcurrentStreamWriter(streamCoordinator, streamAdmin, streamMetaStore, writerFactory,
                                                   cConf.getInt(Constants.Stream.WORKER_THREADS, 10), collector);
  }

  @Override
  public void destroy(HandlerContext context) {
    Closeables.closeQuietly(streamWriter);
  }

  @GET
  @Path("/{stream}/info")
  public void getInfo(HttpRequest request, HttpResponder responder,
                   @PathParam("stream") String stream) throws Exception {
    String accountID = getAuthenticatedAccountId(request);

    if (streamMetaStore.streamExists(accountID, stream)) {
      responder.sendJson(HttpResponseStatus.OK, streamAdmin.getConfig(stream));
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @PUT
  @Path("/{stream}")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("stream") String stream) throws Exception {

    String accountID = getAuthenticatedAccountId(request);

    // Verify stream name
    if (!isValidName(stream)) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "Stream name can only contains alphanumeric, '-' and '_' characters only.");
      return;
    }

    // TODO: Modify the REST API to support custom configurations.
    streamAdmin.create(stream);
    streamMetaStore.addStream(accountID, stream);

    // TODO: For create successful, 201 Created should be returned instead of 200.
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}")
  public void enqueue(HttpRequest request, HttpResponder responder,
                      @PathParam("stream") String stream) throws Exception {

    String accountId = getAuthenticatedAccountId(request);

    try {
      if (streamWriter.enqueue(accountId, stream, getHeaders(request, stream), request.getContent().toByteBuffer())) {
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
    }
  }

  @POST
  @Path("/{stream}/dequeue")
  public void dequeue(HttpRequest request, HttpResponder responder,
                      @PathParam("stream") String stream) throws Exception {
    String accountId = getAuthenticatedAccountId(request);

    // Get the consumer Id
    String consumerId = request.getHeader(Constants.Stream.Headers.CONSUMER_ID);
    long groupId;
    try {
      groupId = Long.parseLong(consumerId);
    } catch (Exception e) {
      LOG.trace("Invalid consumerId: {}", consumerId, e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid or missing consumer id");
      return;
    }

    // See if the consumer id is valid
    StreamDequeuer dequeuer;
    try {
      dequeuer = dequeuerCache.get(new ConsumerCacheKey(accountId, stream, groupId));
    } catch (Exception e) {
      LOG.trace("Failed to get consumer", e);
      responder.sendError(HttpResponseStatus.NOT_FOUND, "Stream or consumer does not exists.");
      return;
    }

    // Dequeue event
    StreamEvent event = dequeuer.fetch();
    if (event == null) {
      responder.sendStatus(HttpResponseStatus.NO_CONTENT);
      return;
    }

    // Construct response headers
    ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
    for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
      builder.put(stream + "." + entry.getKey(), entry.getValue());
    }

    responder.sendBytes(HttpResponseStatus.OK, event.getBody(), builder.build());
  }

  @POST
  @Path("/{stream}/consumer-id")
  public void newConsumer(HttpRequest request, HttpResponder responder,
                          @PathParam("stream") String stream) throws Exception {
    String accountId = getAuthenticatedAccountId(request);

    if (!streamMetaStore.streamExists(accountId, stream)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
      return;
    }

    long groupId = Hashing.md5().newHasher()
      .putString(stream)
      .putString(accountId)
      .putLong(System.nanoTime())
      .hash().asLong();

    streamAdmin.configureInstances(QueueName.fromStream(stream), groupId,
                                   cConf.getInt(Constants.Stream.CONTAINER_INSTANCES, 1));

    String consumerId = Long.toString(groupId);
    responder.sendByteArray(HttpResponseStatus.OK, consumerId.getBytes(Charsets.UTF_8),
                            ImmutableMultimap.of(Constants.Stream.Headers.CONSUMER_ID, consumerId));
  }

  @POST
  @Path("/{stream}/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                       @PathParam("stream") String stream) throws Exception {
    String accountId = getAuthenticatedAccountId(request);

    if (!streamMetaStore.streamExists(accountId, stream)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
      return;
    }

    try {
      streamAdmin.truncate(stream);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IOException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
    }
  }

  @PUT
  @Path("/{stream}/config")
  public void setConfig(HttpRequest request, HttpResponder responder,
                        @PathParam("stream") String stream) throws Exception {

    String accountId = getAuthenticatedAccountId(request);

    if (!streamMetaStore.streamExists(accountId, stream)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
      return;
    }

    try {
      StreamConfig config = streamAdmin.getConfig(stream);
      try {
        config = getConfigUpdate(request, config);
      } catch (Throwable t) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid stream configuration");
        return;
      }

      streamAdmin.updateConfig(config);
      responder.sendStatus(HttpResponseStatus.OK);

    } catch (IOException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
    }
  }

  private String getMetricsContext() {
    return Constants.Gateway.METRICS_CONTEXT + "." + cConf.getInt(Constants.Stream.CONTAINER_INSTANCE_ID, 0);
  }


  private StreamConfig getConfigUpdate(HttpRequest request, StreamConfig config) {
    JsonObject json = GSON.fromJson(request.getContent().toString(Charsets.UTF_8), JsonObject.class);

    // Only pickup changes in TTL
    if (json.has("ttl")) {
      JsonElement ttl = json.get("ttl");
      if (ttl.isJsonPrimitive()) {
        // TTL in the REST API is in seconds. Convert it to ms for the config.
        return new StreamConfig(config.getName(), config.getPartitionDuration(), config.getIndexInterval(),
                                TimeUnit.SECONDS.toMillis(ttl.getAsLong()), config.getLocation());
      }
    }
    return config;
  }

  /**
   * Creates a loading cache for stream consumers. Used by the dequeue REST API.
   */
  private LoadingCache<ConsumerCacheKey, StreamDequeuer> createDequeuerCache(
    final CConfiguration cConf, final StreamConsumerFactory consumerFactory,
    final TransactionExecutorFactory executorFactory, final StreamCoordinator streamCoordinator) {

    return CacheBuilder
      .newBuilder()
      .expireAfterAccess(Constants.Stream.CONSUMER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      .removalListener(new RemovalListener<ConsumerCacheKey, StreamDequeuer>() {
        @Override
        public void onRemoval(RemovalNotification<ConsumerCacheKey, StreamDequeuer> notification) {
          try {
            StreamDequeuer dequeuer = notification.getValue();
            if (dequeuer != null) {
              dequeuer.close();
            }
          } catch (IOException e) {
            LOG.error("Failed to close stream consumer for {}", notification.getKey(), e);
          }
        }
      }).build(new CacheLoader<ConsumerCacheKey, StreamDequeuer>() {
        @Override
        public StreamDequeuer load(ConsumerCacheKey key) throws Exception {
          if (!streamMetaStore.streamExists(key.getAccountId(), key.getStreamName())) {
            throw new IllegalStateException("Stream does not exists");
          }

          // TODO: Deal with dynamic resize of stream handler instances
          int groupSize = cConf.getInt(Constants.Stream.CONTAINER_INSTANCES, 1);
          ConsumerConfig config = new ConsumerConfig(key.getGroupId(),
                                                     cConf.getInt(Constants.Stream.CONTAINER_INSTANCE_ID, 0),
                                                     groupSize, DequeueStrategy.FIFO, null);

          StreamConsumer consumer = consumerFactory.create(QueueName.fromStream(key.getStreamName()),
                                                           Constants.Stream.HANDLER_CONSUMER_NS, config);
          Cancellable cancellable = streamCoordinator.addListener(key.getStreamName(),
                                                                  createStreamPropertyListener(key));
          return new StreamDequeuer(consumer, executorFactory, cancellable);
        }
      });
  }

  /**
   * Creates a {@link StreamPropertyListener} that would reload cached consumer if any stream properties changed.
   */
  private StreamPropertyListener createStreamPropertyListener(final ConsumerCacheKey key) throws IOException {

    return new StreamPropertyListener() {
      @Override
      public void generationChanged(String streamName, int generation) {
        dequeuerCache.refresh(key);
      }

      @Override
      public void generationDeleted(String streamName) {
        dequeuerCache.refresh(key);
      }

      @Override
      public void ttlChanged(String streamName, long ttl) {
        dequeuerCache.refresh(key);
      }

      @Override
      public void ttlDeleted(String streamName) {
        dequeuerCache.refresh(key);
      }
    };
  }

  private boolean isValidName(String streamName) {
    // TODO: This is copied from StreamVerification in app-fabric as this handler is in data-fabric module.
    return CharMatcher.inRange('A', 'Z')
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.is('-'))
      .or(CharMatcher.is('_'))
      .or(CharMatcher.inRange('0', '9')).matchesAllOf(streamName);
  }

  private Map<String, String> getHeaders(HttpRequest request, String stream) {
    // build a new event from the request, start with the headers
    ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
    // set some built-in headers
    headers.put(Constants.Gateway.HEADER_FROM_COLLECTOR, Constants.Gateway.STREAM_HANDLER_NAME);
    headers.put(Constants.Gateway.HEADER_DESTINATION_STREAM, stream);
    // and transfer all other headers that are to be preserved
    String prefix = stream + ".";
    for (Map.Entry<String, String> header : request.getHeaders()) {
      if (header.getKey().startsWith(prefix)) {
        headers.put(header.getKey().substring(prefix.length()), header.getValue());
      }
    }
    return headers.build();
  }

  private static final class ConsumerCacheKey {
    private final String accountId;
    private final String streamName;
    private final long groupId;

    private ConsumerCacheKey(String accountId, String streamName, long groupId) {
      this.accountId = accountId;
      this.streamName = streamName;
      this.groupId = groupId;
    }

    public String getAccountId() {
      return accountId;
    }

    public String getStreamName() {
      return streamName;
    }

    public long getGroupId() {
      return groupId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ConsumerCacheKey that = (ConsumerCacheKey) o;
      return Objects.equal(streamName, that.streamName) && groupId == that.groupId;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(streamName, groupId);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("stream", streamName)
        .add("groupId", groupId)
        .toString();
    }
  }

  /**
   * A Stream consumer that will commit automatically right after dequeue.
   */
  private static final class StreamDequeuer implements Closeable {

    private final StreamConsumer consumer;
    private final TransactionExecutor txExecutor;
    private final Cancellable cancellable;

    private StreamDequeuer(StreamConsumer consumer,
                           TransactionExecutorFactory executorFactory, Cancellable cancellable) {
      this.consumer = consumer;
      this.txExecutor = executorFactory.createExecutor(ImmutableList.<TransactionAware>of(consumer));
      this.cancellable = cancellable;
    }

    /**
     * @return an event fetched from the stream or {@code null} if no event.
     */
    StreamEvent fetch() throws Exception {
      return txExecutor.execute(new Callable<StreamEvent>() {

        @Override
        public StreamEvent call() throws Exception {
          synchronized (StreamDequeuer.this) {
            DequeueResult<StreamEvent> poll = consumer.poll(1, 0, TimeUnit.SECONDS);
            return poll.isEmpty() ? null : poll.iterator().next();
          }
        }
      });
    }

    @Override
    public void close() throws IOException {
      try {
        consumer.close();
      } finally {
        cancellable.cancel();
      }
    }
  }
}
