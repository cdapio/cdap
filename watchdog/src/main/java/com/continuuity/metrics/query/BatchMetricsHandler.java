/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.metrics.data.MetricsTable;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.List;

/**
 * Class for handling batch requests for time series data.
 */
@Path("/metrics")
public final class BatchMetricsHandler extends AbstractHttpHandler {

  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final Gson GSON = new Gson();

  // Function to map URI into MetricRequest by parsing the URI.
  private static final Function<URI, MetricsRequest> URI_TO_METRIC_REQUEST = new Function<URI, MetricsRequest>() {
    @Override
    public MetricsRequest apply(URI input) {
      return MetricsRequestParser.parse(input);
    }
  };

  // It's a cache from metric table resolution to MetricsTable
  private final LoadingCache<Integer, MetricsTable> metricsTableCache;

  @Inject
  public BatchMetricsHandler(final MetricsTableFactory metricsTableFactory) {
    metricsTableCache = CacheBuilder.newBuilder().build(new CacheLoader<Integer, MetricsTable>() {
      @Override
      public MetricsTable load(Integer key) throws Exception {
        return metricsTableFactory.create(key);
      }
    });
  }

  @POST
  public void handleBatch(HttpRequest request, HttpResponder responder) throws IOException {
    if (!CONTENT_TYPE_JSON.equals(request.getHeader(HttpHeaders.Names.CONTENT_TYPE))) {
      responder.sendError(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, "Only " + CONTENT_TYPE_JSON + " is supported.");
      return;
    }

    List<MetricsRequest> metricsRequests;
    try {
      metricsRequests = decodeRequests(request.getContent());
    } catch (JsonSyntaxException e) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid json request: " + e.getMessage());
      return;
    }

    // Naive approach, just fire one scan per request
    for (MetricsRequest metricsRequest : metricsRequests) {

    }
    responder.sendJson(HttpResponseStatus.OK, "");
  }

  /**
   * Decodes the batch request
   *
   * @return a List of String containing all requests from the batch.
   */
  private List<MetricsRequest> decodeRequests(ChannelBuffer content) throws IOException {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      return ImmutableList.copyOf(
        Iterables.transform(
          GSON.<List<URI>>fromJson(reader, new TypeToken<List<URI>>() {}.getType()),
          URI_TO_METRIC_REQUEST
        )
      );
    } finally {
      reader.close();
    }
  }
}
