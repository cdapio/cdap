/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
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

  private final StreamAdmin streamAdmin;
  private final ConcurrentStreamWriter streamWriter;

  @Inject
  public StreamHandler(Authenticator authenticator, StreamAdmin streamAdmin,
                       StreamFileWriterFactory writerFactory, CConfiguration cConf,
                       MetricsCollectionService metricsCollectionService) {
    super(authenticator);
    this.streamAdmin = streamAdmin;

    MetricsCollector collector = metricsCollectionService.getCollector(MetricsScope.REACTOR,
                                                                       Constants.Gateway.METRICS_CONTEXT, "0");
    this.streamWriter = new ConcurrentStreamWriter(streamAdmin, writerFactory,
                                                   cConf.getInt(Constants.Stream.WORKER_THREADS, 10), collector);
  }

  @Override
  public void destroy(HandlerContext context) {
    Closeables.closeQuietly(streamWriter);
  }

  @GET
  @Path("/{stream}/info")
  public void info(HttpRequest request, HttpResponder responder,
                   @PathParam("stream") String stream) throws Exception {
    // Just call to verify, but not using the accountId returned.
    getAuthenticatedAccountId(request);

    if (streamAdmin.exists(stream)) {
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @PUT
  @Path("/{stream}")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("stream") String stream) throws Exception {

    getAuthenticatedAccountId(request);

    // TODO: Modify the REST API to support custom configurations.
    streamAdmin.create(stream);

    // TODO: For create successful, 201 Created should be returned instead of 200.
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}")
  public void enqueue(HttpRequest request, HttpResponder responder,
                      @PathParam("stream") String stream) throws Exception {

    getAuthenticatedAccountId(request);

    if (streamWriter.enqueue(stream, getHeaders(request, stream), request.getContent().toByteBuffer())) {
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/{stream}/dequeue")
  public void dequeue(HttpRequest request, HttpResponder responder, @PathParam("stream") String stream) {
    getAuthenticatedAccountId(request);

    // TODO
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}/consumer")
  public void newConsumer(HttpRequest request, HttpResponder responder, @PathParam("stream") String stream) {
    getAuthenticatedAccountId(request);
    // TODO
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                       @PathParam("stream") String stream) throws Exception {
    getAuthenticatedAccountId(request);

    // TODO: This is not thread and multi-instances safe yet
    // Need to communicates with other instances with a barrier for closing current file for the given stream
    if (streamAdmin.exists(stream)) {
      streamAdmin.truncate(stream);
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
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
        headers.put(header);
      }
    }
    return headers.build();
  }
}
