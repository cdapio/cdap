package com.continuuity.gateway.v2.handlers;

import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.GatewayMetricsHelperWrapper;
import com.continuuity.gateway.util.StreamCache;
import com.continuuity.gateway.v2.CachedStreamEventConsumer;
import com.continuuity.internal.app.verification.StreamVerification;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * Handler that handles stream event requests.
 */
@Path("/stream")
public class StreamHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);

  private final StreamCache streamCache;
  private final MetadataService metadataService;
  private final GatewayMetricsHelperWrapper helper;
  private final CachedStreamEventConsumer consumer;

  @Inject
  public StreamHandler(StreamCache streamCache, MetadataService metadataService, CMetrics cMetrics,
                       CachedStreamEventConsumer consumer) {
    this.streamCache = streamCache;
    this.metadataService = metadataService;
    this.helper = new GatewayMetricsHelperWrapper(new MetricsHelper(
      this.getClass(), cMetrics, this.collector.getMetricsQualifier()), collector.getGatewayMetrics());
    this.consumer = consumer;
  }

  @POST
  @Path("/{streamId}")
  public void create(HttpRequest request, HttpResponder responder, @PathParam("streamId") String streamId) {
    helper.setMethod("create");

    //our user interfaces do not support stream name
    //we are using id for stream name until it is supported in UI's
    if (!isId(streamId)) {
      String message = String.format("Stream id '%s' is not a printable ascii character string", streamId);
      LOG.info(message);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, message);
      helper.finish(BadRequest);
      return;
    }

    Account account = new Account(accountId);
    Stream stream = new Stream(streamId);
    stream.setName(streamId); //

    //Check if a stream with the same id exists
    try {
      Stream existingStream = metadataService.getStream(account, stream);
      if (!existingStream.isExists()) {
        metadataService.createStream(account, stream);
      }
    } catch (Exception e) {
      String message = String.format("Error during creation of stream id '%s'", streamId);
      LOG.error(message, e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, message);
    }

    responder.sendString(HttpResponseStatus.OK, String.format("Stream id '%s' created", streamId));
    helper.finish(Success);
  }

  @PUT
  @Path("/{streamId}")
  public void enqueue(HttpRequest request, final HttpResponder responder,
                      @PathParam("streamId") final String destination) {
    helper.setMethod("enqueue");

    // validate the existence of the stream except for create stream operation
    if (!streamCache.validateStream(accountId, destination)) {
      helper.finish(NotFound);
      LOG.trace("Received a request for non-existent stream {}", destination);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    // build a new event from the request, start with the headers
    Map<String, String> headers = Maps.newHashMap();
    // set some built-in headers
    headers.put(Constants.HEADER_FROM_COLLECTOR, getClass().getCanonicalName());
    headers.put(Constants.HEADER_DESTINATION_STREAM, destination);
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

    LOG.trace("Sending event to consumer: " + event);
    // let the consumer process the event.
    // in case of exception, respond with internal error
    ListenableFuture<Void> future = consumer.consume(event, accountId);
    Futures.addCallback(future,
                        new FutureCallback<Void>() {
                          @Override
                          public void onSuccess(Void result) {
                            helper.finish(Success);
                            responder.sendStatus(HttpResponseStatus.OK);
                          }

                          @Override
                          public void onFailure(Throwable t) {
                            helper.finish(Error);
                            responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);

                            // refresh cache for this stream - apparently it has disappeared
                            streamCache.refreshStream(accountId, destination);
                          }
                        });
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
