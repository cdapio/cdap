package com.continuuity.gateway.v2.handlers.v2.stream;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.DataType;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.StreamCache;
import com.continuuity.gateway.util.ThriftHelper;
import com.continuuity.gateway.v2.handlers.v2.AuthenticatedHttpHandler;
import com.continuuity.internal.app.verification.StreamVerification;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.streamevent.StreamEventCodec;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
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
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.thrift.protocol.TProtocol;
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

/**
 * Handler that handles stream event requests. This supports
 * POST requests to send an event to a stream, and GET requests to inspect
 * or retrieve events from the stream.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class StreamHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);
  private static final String NAME = Constants.Gateway.STREAM_HANDLER_NAME;

  private final StreamCache streamCache;
  private final CachedStreamEventCollector streamEventCollector;
  private final GatewayAuthenticator authenticator;
  private final DiscoveryServiceClient discoveryClient;
  private final StreamAdmin streamAdmin;
  private EndpointStrategy endpointStrategy;

  private final LoadingCache<ConsumerKey, ConsumerHolder> queueConsumerCache;

  @Inject
  public StreamHandler(final TransactionSystemClient txClient, StreamCache streamCache,
                       final QueueClientFactory queueClientFactory, GatewayAuthenticator authenticator,
                       CachedStreamEventCollector cachedStreamEventCollector, DiscoveryServiceClient discoveryClient,
                       StreamAdmin streamAdmin) {
    super(authenticator);
    this.streamCache = streamCache;
    this.authenticator = authenticator;
    this.discoveryClient = discoveryClient;
    this.streamEventCollector = cachedStreamEventCollector;
    this.streamAdmin = streamAdmin;

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
    endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC)), 1L, TimeUnit.SECONDS);
    this.streamCache.init(endpointStrategy);
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
    String accountId = authenticate(request, responder);
    if (accountId == null) {
      return;
    }

    if (!isId(destination)) {
      LOG.trace("Stream id '{}' is not a printable ascii character string", destination);
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }

    // Check if a stream with the same id exists
    try {
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        ProgramId id = new ProgramId(accountId, "", "");
        String existing = client.getDataEntity(id, DataType.STREAM, destination);
        if (existing == null || existing.isEmpty()) {
          client.createStream(id, new Gson().toJson(new StreamSpecification.Builder().setName(destination).create()));
        }
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
    } catch (Throwable e) {
      LOG.error("Error during creation of stream id '{}'", destination, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/streams/{stream-id}")
  public void enqueue(HttpRequest request, final HttpResponder responder,
                      @PathParam("stream-id") final String destination) {
    final String accountId = authenticate(request, responder);
    if (accountId == null) {
      return;
    }

    try {
      // validate the existence of the stream
      if (!streamCache.validateStream(accountId, destination)) {
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
                                       responder.sendStatus(HttpResponseStatus.OK);
                                     }

                                     @Override
                                     public void onFailure(Throwable t) {
                                       LOG.error("Exception when trying to enqueue stream event", t);
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
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/streams/{stream-id}/dequeue")
  public void dequeue(HttpRequest request, HttpResponder responder,
                      @PathParam("stream-id") String destination) {
    String accountId = authenticate(request, responder);
    if (accountId == null) {
      return;
    }

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
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid groupId");
      return;
    }

    // valid group id, dequeue and return
    QueueName queueName = QueueName.fromStream(destination);
    ConsumerHolder consumerHolder;
    try {
      // validate the existence of the stream
      if (!streamCache.validateStream(accountId, destination)) {
        LOG.trace("Received a request for non-existent stream {}", destination);
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      consumerHolder = queueConsumerCache.get(new ConsumerKey(queueName, groupId));
    } catch (Throwable e) {
      LOG.error("Caught exception during creation of consumer for stream {}", destination, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    // perform dequeue
    DequeueResult result;
    try {
      result = consumerHolder.dequeue();
    } catch (Throwable e) {
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
    } catch (Throwable e) {
      LOG.error("Exception when deserializing data from stream {} into an event: ", queueName, e);
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
  }

  @POST
  @Path("/streams/{stream-id}/consumer-id")
  public void newConsumer(HttpRequest request, HttpResponder responder,
                          @PathParam("stream-id") String destination) {
    String accountId = authenticate(request, responder);
    if (accountId == null) {
      return;
    }

    try {
      // validate the existence of the stream
      if (!streamCache.validateStream(accountId, destination)) {
        LOG.trace("Received a request for non-existent stream {}", destination);
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
    } catch (Throwable e) {
      LOG.error("Exception during validation of stream {}", destination, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
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
  }

  @POST
  @Path("/streams/{stream-id}/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                          @PathParam("stream-id") String stream) {
    String accountId = authenticate(request, responder);
    if (accountId == null) {
      return;
    }

    try {
      // validate the existence of the stream
      if (!streamCache.validateStream(accountId, stream)) {
        LOG.trace("Received a request for non-existent stream {}", stream);
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
    } catch (Throwable e) {
      LOG.error("Exception during validation of stream {}", stream, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    try {
      QueueName streamName = QueueName.fromStream(stream);
      streamAdmin.truncate(streamName.toURI().toString());
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable e) {
      LOG.error("Exception while truncating stream {}", stream, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/streams/{stream-id}/info")
  public void getInfo(HttpRequest request, HttpResponder responder,
                      @PathParam("stream-id") String destination) {
    String accountId = authenticate(request, responder);
    if (accountId == null) {
      return;
    }
    // TODO: ENG-3203. The new queue implementation does not have getQueueInfo method yet.
    // Hence for now just checking whether stream exists or not.
    try {
      // validate the existence of the stream, but stream cache may be out of date, so refresh it first
      streamCache.refreshStream(accountId, destination);
      if (!streamCache.validateStream(accountId, destination)) {
        LOG.trace("Received a request for non-existent stream {}", destination);
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendJson(HttpResponseStatus.OK, ImmutableMap.of());
      }
    } catch (Throwable e) {
      LOG.error("Exception while fetching metadata for stream {}", destination);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private String authenticate(HttpRequest request, HttpResponder responder) {
    // if authentication is enabled, verify an authentication token has been
    // passed and then verify the token is valid
    if (!authenticator.authenticateRequest(request)) {
      LOG.trace("Received an unauthorized request");
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
      return null;
    }

    String accountId = authenticator.getAccountId(request);
    if (accountId == null || accountId.isEmpty()) {
      LOG.trace("No valid account information found");
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
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
    VerifyResult result = verifier.verify(null, spec); // safe to pass in null for this verifier
    return (result.getStatus() == VerifyResult.Status.SUCCESS);
  }

}
