package com.continuuity.gateway.v2.handlers.v2.stream;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.GatewayMetrics;
import com.continuuity.gateway.GatewayMetricsHelperWrapper;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.StreamCache;
import com.continuuity.internal.app.verification.StreamVerification;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.streamevent.StreamEventCodec;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.FutureCallback;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NoData;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * Handler that handles stream event requests. This supports
 * POST requests to send an event to a stream, and GET requests to inspect
 * or retrieve events from the stream.
 */
@Path("/v2")
public class StreamHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);
  private static final String NAME = Constants.Gateway.STREAM_HANDLER_NAME;

  private final StreamCache streamCache;
  private final MetadataService metadataService;
  private final CMetrics cMetrics;
  private final GatewayMetrics gatewayMetrics;
  private final CachedStreamEventCollector streamEventCollector;
  private final GatewayAuthenticator authenticator;

  private final LoadingCache<ConsumerKey, ConsumerHolder> queueConsumerCache;

  @Inject
  public StreamHandler(final TransactionSystemClient txClient, StreamCache streamCache,
                       MetadataService metadataService, CMetrics cMetrics, GatewayMetrics gatewayMetrics,
                       final QueueClientFactory queueClientFactory, GatewayAuthenticator authenticator,
                       CachedStreamEventCollector cachedStreamEventCollector) {
    this.streamCache = streamCache;
    this.metadataService = metadataService;
    this.cMetrics = cMetrics;
    this.gatewayMetrics = gatewayMetrics;
    this.authenticator = authenticator;

    this.streamEventCollector = cachedStreamEventCollector;

    this.queueConsumerCache = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .removalListener(
        new RemovalListener<ConsumerKey, ConsumerHolder>() {
          @Override
          public void onRemoval(RemovalNotification<ConsumerKey, ConsumerHolder> notification) {
            ConsumerHolder holder = notification.getValue();
            if (holder == null) {
              return;
            }

            try {
              holder.close();
            } catch (IOException e) {
              LOG.error("Exception while closing consumer {}", holder, e);
            }
          }
        }
      )
      .build(
        new CacheLoader<ConsumerKey, ConsumerHolder>() {
          @Override
          public ConsumerHolder load(ConsumerKey key) throws Exception {
            return new ConsumerHolder(key, txClient, queueClientFactory);
          }
        });
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting StreamHandler.");
    streamEventCollector.startAndWait();
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping StreamHandler.");
    streamEventCollector.stopAndWait();
  }

  @PUT
  @Path("/streams/{stream-id}")
  public void create(HttpRequest request, HttpResponder responder, @PathParam("stream-id") String destination) {
    GatewayMetricsHelperWrapper helper = new GatewayMetricsHelperWrapper(
      new MetricsHelper(this.getClass(), cMetrics, Constants.Gateway.GATEWAY_PREFIX + NAME), gatewayMetrics);
    helper.setMethod("create");
    helper.setScope(destination);

    String accountId = authenticate(request, responder, helper);
    if (accountId == null) {
      return;
    }

    if (!isId(destination)) {
      LOG.trace("Stream id '{}' is not a printable ascii character string", destination);
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      helper.finish(BadRequest);
      return;
    }

    Account account = new Account(accountId);
    Stream stream = new Stream(destination);
    stream.setName(destination);

    //Check if a stream with the same id exists
    try {
      Stream existingStream = metadataService.getStream(account, stream);
      if (!existingStream.isExists()) {
        metadataService.createStream(account, stream);
      }
    } catch (Exception e) {
      LOG.trace("Error during creation of stream id '{}'", destination, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    responder.sendStatus(HttpResponseStatus.OK);
    helper.finish(Success);
  }

  @POST
  @Path("/streams/{stream-id}")
  public void enqueue(HttpRequest request, final HttpResponder responder,
                      @PathParam("stream-id") final String destination) {
    final GatewayMetricsHelperWrapper helper = new GatewayMetricsHelperWrapper(
      new MetricsHelper(this.getClass(), cMetrics, Constants.Gateway.GATEWAY_PREFIX + NAME), gatewayMetrics);
    helper.setMethod("enqueue");
    helper.setScope(destination);

    final String accountId = authenticate(request, responder, helper);
    if (accountId == null) {
      return;
    }

    try {
      // validate the existence of the stream
      if (!streamCache.validateStream(accountId, destination)) {
        helper.finish(NotFound);
        LOG.trace("Received a request for non-existent stream {}", destination);
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      // build a new event from the request, start with the headers
      Map<String, String> headers = Maps.newHashMap();
      // set some built-in headers
      headers.put(Constants.Gateway.HEADER_FROM_COLLECTOR, NAME);
      headers.put(Constants.Gateway.HEADER_DESTINATION_STREAM, destination);
      // and transfer all other headers that are to be preserved
      String prefix = destination + ".";
      for (String header : request.getHeaderNames()) {
        String preservedHeader = isPreservedHeader(prefix, header);
        if (preservedHeader != null) {
          headers.put(preservedHeader, request.getHeader(header));
        }
      }
      // read the body of the request
      ByteBuffer body = request.getContent().toByteBuffer();
      // and create a stream event
      StreamEvent event = new DefaultStreamEvent(headers, body);

      // let the streamEventCollector process the event.
      // in case of exception, respond with internal error
      streamEventCollector.collect(event, accountId,
                                   new FutureCallback<Void>() {
                                     @Override
                                     public void onSuccess(Void result) {
                                       helper.finish(Success);
                                       responder.sendStatus(HttpResponseStatus.OK);
                                     }

                                     @Override
                                     public void onFailure(Throwable t) {
                                       LOG.error("Exception when trying to enqueue stream event", t);
                                       helper.finish(Error);
                                       responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);

                                       // refresh cache for this stream - apparently it has disappeared
                                       try {
                                         streamCache.refreshStream(accountId, destination);
                                       } catch (OperationException e) {
                                         LOG.error("Exception thrown during refresh of stream {} with accountId {}",
                                                   destination, accountId, e);
                                       }
                                     }
                                   }
      );
    } catch (Exception e) {
      LOG.error("Exception while enqueing stream {} events", destination, e);
      helper.finish(Error);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/streams/{streamId}/dequeue")
  public void dequeue(HttpRequest request, HttpResponder responder,
                      @PathParam("streamId") String destination) {
    GatewayMetricsHelperWrapper helper = new GatewayMetricsHelperWrapper(
      new MetricsHelper(this.getClass(), cMetrics, Constants.Gateway.GATEWAY_PREFIX + NAME), gatewayMetrics);

    String accountId = authenticate(request, responder, helper);
    if (accountId == null) {
      return;
    }

    helper.setMethod("dequeue");
    helper.setScope(destination);

    // there must be a header with the consumer id in the request
    String idHeader = request.getHeader(Constants.Gateway.HEADER_STREAM_CONSUMER);
    Long groupId = null;
    if (idHeader == null) {
      LOG.trace("Received a dequeue request for stream {} without header {}",
                destination, Constants.Gateway.HEADER_STREAM_CONSUMER);
    } else {
      try {
        groupId = Long.valueOf(idHeader);
      } catch (NumberFormatException e) {
        LOG.trace("Received a dequeue request with a invalid header {} for stream {}",
                  destination, Constants.Gateway.HEADER_STREAM_CONSUMER, e);
      }
    }

    if (null == groupId) {
      helper.finish(BadRequest);
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }

    // valid group id, dequeue and return
    QueueName queueName = QueueName.fromStream(accountId, destination);
    ConsumerHolder consumerHolder;
    try {
      // validate the existence of the stream
      if (!streamCache.validateStream(accountId, destination)) {
        helper.finish(NotFound);
        LOG.trace("Received a request for non-existent stream {}", destination);
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      consumerHolder = queueConsumerCache.get(new ConsumerKey(queueName, groupId));
    } catch (Exception e) {
      LOG.error("Caught exception during creation of consumer for stream {}", destination, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    // perform dequeue
    DequeueResult result;
    try {
      result = consumerHolder.dequeue();
    } catch (Throwable e) {
      helper.finish(Error);
      LOG.error("Error dequeueing from stream {} with consumer {}", queueName, consumerHolder, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      try {
        // refresh the cache for this stream, it may have been deleted
        streamCache.refreshStream(accountId, destination);
      } catch (OperationException e1) {
        LOG.error("Exception while refreshing stream {} with account {}", destination, accountId, e1);
      }
      return;
    }

    if (result.isEmpty()) {
      responder.sendStatus(HttpResponseStatus.NO_CONTENT);
      helper.finish(NoData);
      return;
    }

    // try to deserialize into an event
    Map<String, String> headers;
    byte[] body;
    try {
      StreamEventCodec deserializer = new StreamEventCodec();
      StreamEvent event = deserializer.decodePayload(result.iterator().next());
      headers = event.getHeaders();
      body = Bytes.toBytes(event.getBody());
    } catch (Exception e) {
      LOG.error("Exception when deserializing data from stream {} into an event: ", queueName, e);
      helper.finish(Error);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    // prefix each header with the destination to distinguish them from
    // HTTP headers
    ImmutableMultimap.Builder<String, String> prefixedHeadersBuilder = ImmutableMultimap.builder();
    for (Map.Entry<String, String> header : headers.entrySet()) {
      prefixedHeadersBuilder.put(destination + "." + header.getKey(), header.getValue());
    }
    // now the headers and body are ready to be sent back
    responder.sendByteArray(HttpResponseStatus.OK, body, prefixedHeadersBuilder.build());
    helper.finish(Success);
  }

  @GET
  @Path("/streams/{streamId}/consumerid")
  public void newConsumer(HttpRequest request, HttpResponder responder,
                          @PathParam("streamId") String destination) {
    GatewayMetricsHelperWrapper helper = new GatewayMetricsHelperWrapper(
      new MetricsHelper(this.getClass(), cMetrics, NAME), gatewayMetrics);

    String accountId = authenticate(request, responder, helper);
    if (accountId == null) {
      return;
    }

    helper.setMethod("getNewId");
    helper.setScope(destination);

    try {
      // validate the existence of the stream
      if (!streamCache.validateStream(accountId, destination)) {
        helper.finish(NotFound);
        LOG.trace("Received a request for non-existent stream {}", destination);
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
    } catch (OperationException e) {
      LOG.error("Exception during validation of stream {}", destination, e);
    }

    // TODO: ENG-3203. The new queue implementation does not have getGroupId method yet.
    // Hence temporarily generating groupID using stream name, accountId and nano time.
    HashCode hashCode = Hashing.md5().newHasher()
      .putInt(destination.length())
      .putString(destination)
      .putInt(accountId.length())
      .putString(accountId)
      .putInt(Long.SIZE)
      .putLong(System.nanoTime())
      .hash();

    long groupId = hashCode.asLong();
    byte [] body = Bytes.toBytes(groupId);

    ImmutableMultimap<String, String> headers = ImmutableMultimap.of(
      Constants.Gateway.HEADER_STREAM_CONSUMER, Long.toString(groupId));
    responder.sendByteArray(HttpResponseStatus.OK, body, headers);

    helper.finish(Success);
  }

  @GET
  @Path("/streams/{streamId}/info")
  public void getInfo(HttpRequest request, HttpResponder responder,
                      @PathParam("streamId") String destination) {
    GatewayMetricsHelperWrapper helper = new GatewayMetricsHelperWrapper(
      new MetricsHelper(this.getClass(), cMetrics, NAME), gatewayMetrics);

    String accountId = authenticate(request, responder, helper);
    if (accountId == null) {
      return;
    }

    helper.setMethod("getQueueInfo");
    helper.setScope(destination);

    // TODO: ENG-3203. The new queue implementation does not have getQueueInfo method yet.
    // Hence for now just checking whether stream exists or not.
    try {
      // validate the existence of the stream
      if (!streamCache.validateStream(accountId, destination)) {
        helper.finish(NotFound);
        LOG.trace("Received a request for non-existent stream {}", destination);
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      //Check if a stream exists
      Stream existingStream = metadataService.getStream(new Account(accountId), new Stream(destination));
      boolean existsInMDS = existingStream.isExists();

      if (existsInMDS) {
        responder.sendJson(HttpResponseStatus.OK, ImmutableMap.of());
        helper.finish(Success);
      } else {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        helper.finish(NotFound);
      }
      streamCache.refreshStream(accountId, destination);
    } catch (Exception e) {
      LOG.error("Exception while fetching metadata for stream {}", destination);
      helper.finish(Error);
    }
  }

  private String authenticate(HttpRequest request, HttpResponder responder, GatewayMetricsHelperWrapper helper) {
    // if authentication is enabled, verify an authentication token has been
    // passed and then verify the token is valid
    if (!authenticator.authenticateRequest(request)) {
      LOG.trace("Received an unauthorized request");
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
      helper.finish(BadRequest);
      return null;
    }

    String accountId = authenticator.getAccountId(request);
    if (accountId == null || accountId.isEmpty()) {
      LOG.trace("No valid account information found");
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      helper.finish(NotFound);
    }
    return accountId;
  }

  /**
   * Determines whether an HTTP header should be preserved in the persisted
   * event, and if so returns the (possibly transformed) header name. We pass
   * through all headers that start with the name of destination stream, but
   * we strip off the stream name.
   *
   * @param destinationPrefix The name of the destination stream with . appended
   * @param name              The nameof the header to check
   * @return the name to use for the header if it is preserved, null otherwise.
   */
  private String isPreservedHeader(String destinationPrefix, String name) {
    if (name.startsWith(destinationPrefix)) {
      return name.substring(destinationPrefix.length());
    }
    return null;
  }

  private boolean isId(String id) {
    StreamSpecification spec = new StreamSpecification.Builder().setName(id).create();
    StreamVerification verifier = new StreamVerification();
    VerifyResult result = verifier.verify(spec);
    return (result.getStatus() == VerifyResult.Status.SUCCESS);
  }

}
