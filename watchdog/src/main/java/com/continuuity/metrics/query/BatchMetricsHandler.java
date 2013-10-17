/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.queue.QueueName;
import com.continuuity.metrics.data.AggregatesScanResult;
import com.continuuity.metrics.data.AggregatesScanner;
import com.continuuity.metrics.data.AggregatesTable;
import com.continuuity.metrics.data.Interpolator;
import com.continuuity.metrics.data.Interpolators;
import com.continuuity.metrics.data.MetricsScanQuery;
import com.continuuity.metrics.data.MetricsScanQueryBuilder;
import com.continuuity.metrics.data.MetricsScanResult;
import com.continuuity.metrics.data.MetricsScanner;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.data.TimeSeriesTable;
import com.continuuity.metrics.data.TimeValue;
import com.continuuity.metrics.data.TimeValueAggregator;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Class for handling batch requests for metrics data.
 */
@Path("/metrics")
public final class BatchMetricsHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(BatchMetricsHandler.class);
  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final Gson GSON = new Gson();

  // Function to map URI into MetricRequest by parsing the URI.
  private static final Function<URI, MetricsRequest> URI_TO_METRIC_REQUEST = new Function<URI, MetricsRequest>() {
    @Override
    public MetricsRequest apply(URI input) {
      try {
        return MetricsRequestParser.parse(input);
      } catch (IllegalArgumentException e) {
        LOG.error("Failed to parse request: {}", input, e);
        throw Throwables.propagate(e);
      }
    }
  };

  private final MetricsRequestExecutor requestExecutor;

  @Inject
  public BatchMetricsHandler(final MetricsTableFactory metricsTableFactory) {
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
  public void handleBatch(HttpRequest request, HttpResponder responder) throws IOException, OperationException {
    if (!CONTENT_TYPE_JSON.equals(request.getHeader(HttpHeaders.Names.CONTENT_TYPE))) {
      responder.sendError(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, "Only " + CONTENT_TYPE_JSON + " is supported.");
      return;
    }

    List<MetricsRequest> metricsRequests;
    try {
      metricsRequests = decodeRequests(request.getContent());
    } catch (Throwable t) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid request: " + t.getMessage());
      return;
    }

    // Pretty ugly logic now. Need to refactor
    JsonArray output = new JsonArray();
    for (MetricsRequest metricsRequest : metricsRequests) {

      JsonObject json = new JsonObject();
      json.addProperty("path", metricsRequest.getRequestURI().toString());
      json.add("result", requestExecutor.executeQuery(metricsRequest));
      json.add("error", JsonNull.INSTANCE);

      output.add(json);
    }

    responder.sendJson(HttpResponseStatus.OK, output);
  }

  /**
   * Decodes the batch request.
   *
   * @return a List of String containing all requests from the batch.
   */
  private List<MetricsRequest> decodeRequests(ChannelBuffer content) throws IOException {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      List<URI> uris = GSON.fromJson(reader, new TypeToken<List<URI>>() {}.getType());
      LOG.trace("Requests: {}", uris);
      return ImmutableList.copyOf(Iterables.transform(uris, URI_TO_METRIC_REQUEST));
    } finally {
      reader.close();
    }
  }
}
