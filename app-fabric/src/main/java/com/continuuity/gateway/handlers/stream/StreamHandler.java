package com.continuuity.gateway.handlers.stream;

import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.app.verification.StreamVerification;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that handles stream event requests. This supports
 * POST requests to send an event to a stream, and GET requests to inspect
 * or retrieve events from the stream.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class StreamHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);
  private static final String NAME = Constants.Gateway.STREAM_HANDLER_NAME;

  private final Authenticator authenticator;
  private Provider<CachedStreamEventCollector> cachedStreamEventCollectorProvider;
  private final DiscoveryServiceClient discoveryClient;
  private EndpointStrategy endpointStrategy;
  private CachedStreamEventCollector streamEventCollector;

  @Inject
  public StreamHandler(final TransactionSystemClient txClient,
                       final QueueClientFactory queueClientFactory, Authenticator authenticator,
                       Provider<CachedStreamEventCollector> cachedStreamEventCollectorProvider,
                       DiscoveryServiceClient discoveryClient,
                       StreamAdmin streamAdmin) {
    super(authenticator);
    this.authenticator = authenticator;
    this.cachedStreamEventCollectorProvider = cachedStreamEventCollectorProvider;
    this.discoveryClient = discoveryClient;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting StreamHandler.");
    endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.STREAM_HANDLER)), 2L, TimeUnit.SECONDS);
    streamEventCollector = cachedStreamEventCollectorProvider.get();
    streamEventCollector.startAndWait();
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping StreamHandler.");
    streamEventCollector.stopAndWait();
  }

  @PUT
  @Path("/streams/{stream-id}")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("stream-id") String destination) throws Exception {
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }

    URL url = new URL(String.format("http://%s:%d%s/streams/%s",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    Constants.Gateway.GATEWAY_VERSION,
                                    destination));
    LOG.info("Forward to {}", url);

    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setRequestMethod("PUT");
    responder.sendStatus(HttpResponseStatus.valueOf(urlConn.getResponseCode()));
  }

  @POST
  @Path("/streams/{stream-id}")
  public void enqueue(HttpRequest request, final HttpResponder responder,
                      @PathParam("stream-id") final String destination) throws Exception {
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }

    URL url = new URL(String.format("http://%s:%d%s/streams/%s",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    Constants.Gateway.GATEWAY_VERSION,
                                    destination));
    LOG.info("Forward to {}", url);

    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    ByteStreams.copy(new ChannelBufferInputStream(request.getContent()), urlConn.getOutputStream());
    responder.sendStatus(HttpResponseStatus.valueOf(urlConn.getResponseCode()));
  }

  @POST
  @Path("/streams/{stream-id}/dequeue")
  public void dequeue(HttpRequest request, HttpResponder responder,
                      @PathParam("stream-id") String destination) throws Exception {
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }

    URL url = new URL(String.format("http://%s:%d%s/streams/%s/dequeue",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    Constants.Gateway.GATEWAY_VERSION,
                                    destination));
    LOG.info("Forward to {}", url);

    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    ByteStreams.copy(new ChannelBufferInputStream(request.getContent()), urlConn.getOutputStream());
    responder.sendStatus(HttpResponseStatus.valueOf(urlConn.getResponseCode()));
  }

  @POST
  @Path("/streams/{stream-id}/consumer-id")
  public void newConsumer(HttpRequest request, HttpResponder responder,
                          @PathParam("stream-id") String destination) throws Exception {
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }

    URL url = new URL(String.format("http://%s:%d%s/streams/%s/consumer-id",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    Constants.Gateway.GATEWAY_VERSION,
                                    destination));
    LOG.info("Forward to {}", url);

    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    ByteStreams.copy(new ChannelBufferInputStream(request.getContent()), urlConn.getOutputStream());
    responder.sendStatus(HttpResponseStatus.valueOf(urlConn.getResponseCode()));
  }

  @POST
  @Path("/streams/{stream-id}/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                          @PathParam("stream-id") String stream) throws Exception {
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }

    URL url = new URL(String.format("http://%s:%d%s/streams/%s/truncate",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    Constants.Gateway.GATEWAY_VERSION,
                                    stream));
    LOG.info("Forward to {}", url);

    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    ByteStreams.copy(new ChannelBufferInputStream(request.getContent()), urlConn.getOutputStream());
    responder.sendStatus(HttpResponseStatus.valueOf(urlConn.getResponseCode()));
  }

  @GET
  @Path("/streams/{stream-id}/info")
  public void getInfo(HttpRequest request, HttpResponder responder,
                      @PathParam("stream-id") String destination) throws Exception {
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }

    URL url = new URL(String.format("http://%s:%d%s/streams/%s/info",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    Constants.Gateway.GATEWAY_VERSION,
                                    destination));
    LOG.info("Forward to {}", url);

    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    responder.sendStatus(HttpResponseStatus.valueOf(urlConn.getResponseCode()));
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
