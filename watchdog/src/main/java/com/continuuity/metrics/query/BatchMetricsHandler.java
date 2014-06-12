/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.ServerException;
import com.continuuity.data2.OperationException;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpResponder;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.List;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Class for handling batch requests for metrics data.
 */
@Path(Constants.Gateway.GATEWAY_VERSION + "/metrics")
public final class BatchMetricsHandler extends BaseMetricsHandler {

  private static final Logger LOG = LoggerFactory.getLogger(BatchMetricsHandler.class);
  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final Gson GSON = new Gson();

  private final MetricsRequestExecutor requestExecutor;

  @Inject
  public BatchMetricsHandler(Authenticator authenticator, final MetricsTableFactory metricsTableFactory) {
    super(authenticator);
    this.requestExecutor = new MetricsRequestExecutor(metricsTableFactory);
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    LOG.info("Starting BatchMetricsHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    super.destroy(context);
    LOG.info("Stopping BatchMetricsHandler");
  }

  @POST
  public void handleBatch(HttpRequest request, HttpResponder responder) throws IOException {
    if (!CONTENT_TYPE_JSON.equals(request.getHeader(HttpHeaders.Names.CONTENT_TYPE))) {
      responder.sendError(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, "Only " + CONTENT_TYPE_JSON + " is supported.");
      return;
    }

    JsonArray output = new JsonArray();

    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
    String currPath = "";
    try {
      // decode requests
      List<URI> uris = GSON.fromJson(reader, new TypeToken<List<URI>>() { }.getType());
      LOG.trace("Requests: {}", uris);
      for (URI uri : uris) {
        currPath = uri.toString();
        // if the request is invalid, this will throw an exception and return a 400 indicating the request
        // that is invalid and why.
        MetricsRequest metricsRequest = parseAndValidate(request, uri);

        JsonObject json = new JsonObject();
        json.addProperty("path", metricsRequest.getRequestURI().toString());
        json.add("result", requestExecutor.executeQuery(metricsRequest));
        json.add("error", JsonNull.INSTANCE);

        output.add(json);
      }
      responder.sendJson(HttpResponseStatus.OK, output);
    } catch (MetricsPathException e) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid path '" + currPath + "': " + e.getMessage());
    } catch (OperationException e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    } catch (ServerException e) {
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    } finally {
      reader.close();
    }
  }
}
