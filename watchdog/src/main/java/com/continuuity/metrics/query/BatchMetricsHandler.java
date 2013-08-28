/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.queue.QueueName;
import com.continuuity.metrics.data.AggregatesScanResult;
import com.continuuity.metrics.data.AggregatesScanner;
import com.continuuity.metrics.data.AggregatesTable;
import com.continuuity.metrics.data.MetricsScanQuery;
import com.continuuity.metrics.data.MetricsScanQueryBuilder;
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
import com.google.common.collect.Lists;
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

/**
 * Class for handling batch requests for metrics data of the {@link MetricsScope#REACTOR} scope.
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

  // It's a cache from metric table resolution to MetricsTable
  private final Map<MetricsScope, LoadingCache<Integer, TimeSeriesTable>> metricsTableCaches;
  private final Map<MetricsScope, AggregatesTable> aggregatesTables;

  @Inject
  public BatchMetricsHandler(final MetricsTableFactory metricsTableFactory) {
    this.metricsTableCaches = Maps.newHashMap();
    this.aggregatesTables = Maps.newHashMap();
    for (final MetricsScope scope : MetricsScope.values()) {
      LoadingCache<Integer, TimeSeriesTable> cache =
        CacheBuilder.newBuilder().build(new CacheLoader<Integer, TimeSeriesTable>() {
          @Override
          public TimeSeriesTable load(Integer key) throws Exception {
            return metricsTableFactory.createTimeSeries(scope.name(), key);
          }
        });
      this.metricsTableCaches.put(scope, cache);
      this.aggregatesTables.put(scope, metricsTableFactory.createAggregates(scope.name()));
    }
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
    } catch (Throwable t) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid request: " + t.getMessage());
      return;
    }

    // Pretty ugly logic now. Need to refactor
    JsonArray output = new JsonArray();
    for (MetricsRequest metricsRequest : metricsRequests) {
      Object resultObj = null;
      if (metricsRequest.getType() == MetricsRequest.Type.TIME_SERIES) {
        TimeSeriesResponse.Builder builder = TimeSeriesResponse.builder(metricsRequest.getStartTime(),
                                                                        metricsRequest.getEndTime());
        // Special metrics handle that requires computation from multiple time series.
        if ("process.busyness".equals(metricsRequest.getMetricPrefix())) {
          computeProcessBusyness(metricsRequest, builder);
        } else {
          MetricsScanQuery scanQuery = new MetricsScanQueryBuilder()
            .setContext(metricsRequest.getContextPrefix())
            .setMetric(metricsRequest.getMetricPrefix())
            .setTag(metricsRequest.getTagPrefix())
            .build(metricsRequest.getStartTime(), metricsRequest.getEndTime());

          PeekingIterator<TimeValue> timeValueItor = Iterators.peekingIterator(
            queryTimeSeries(metricsRequest.getScope(), scanQuery));

          for (int i = 0; i < metricsRequest.getCount(); i++) {
            long resultTime = metricsRequest.getStartTime() + i;

            if (timeValueItor.hasNext() && timeValueItor.peek().getTime() == resultTime) {
              builder.addData(resultTime, timeValueItor.next().getValue());
              continue;
            }
            builder.addData(resultTime, 0);
          }
        }
        resultObj = builder.build();

      } else if (metricsRequest.getType() == MetricsRequest.Type.AGGREGATE) {
        // Special metrics handle that requires computation from multiple aggregates results.
        if ("process.events.pending".equals(metricsRequest.getMetricPrefix())) {
          resultObj = computeQueueLength(metricsRequest);
        } else {
          resultObj = getAggregates(metricsRequest);
        }
      }

      JsonObject json = new JsonObject();
      json.addProperty("path", metricsRequest.getRequestURI().toString());
      json.add("result", GSON.toJsonTree(resultObj));
      json.add("error", JsonNull.INSTANCE);

      output.add(json);
    }

    responder.sendJson(HttpResponseStatus.OK, output);
  }

  private void computeProcessBusyness(MetricsRequest metricsRequest, TimeSeriesResponse.Builder builder) {
    MetricsScanQuery scanQuery = new MetricsScanQueryBuilder()
      .setContext(metricsRequest.getContextPrefix())
      .setMetric("process.tuples.read")
      .build(metricsRequest.getStartTime(), metricsRequest.getEndTime());
    MetricsScope scope = metricsRequest.getScope();

    PeekingIterator<TimeValue> tuplesReadItor = Iterators.peekingIterator(queryTimeSeries(scope, scanQuery));

    scanQuery = new MetricsScanQueryBuilder()
      .setContext(metricsRequest.getContextPrefix())
      .setMetric("process.events.processed")
      .build(metricsRequest.getStartTime(), metricsRequest.getEndTime());

    PeekingIterator<TimeValue> eventsProcessedItor = Iterators.peekingIterator(queryTimeSeries(scope, scanQuery));

    for (int i = 0; i < metricsRequest.getCount(); i++) {
      long resultTime = metricsRequest.getStartTime() + i;
      int tupleRead = 0;
      int eventProcessed = 0;
      if (tuplesReadItor.hasNext() && tuplesReadItor.peek().getTime() == resultTime) {
        tupleRead = tuplesReadItor.next().getValue();
      }
      if (eventsProcessedItor.hasNext() && eventsProcessedItor.peek().getTime() == resultTime) {
        eventProcessed = eventsProcessedItor.next().getValue();
      }
      if (eventProcessed != 0) {
        int busyness = (int) ((float) tupleRead / eventProcessed * 100);
        builder.addData(resultTime, busyness > 100 ? 100 : busyness);
      } else {
        builder.addData(resultTime, 0);
      }
    }
  }

  private Object computeQueueLength(MetricsRequest metricsRequest) {
    MetricsScope scope = metricsRequest.getScope();
    AggregatesTable aggregatesTable = aggregatesTables.get(scope);
    // First scan the ack to get an aggregate and also names of queues.
    AggregatesScanner scanner = aggregatesTable.scan(metricsRequest.getContextPrefix(),
                                                     "q.ack",
                                                     metricsRequest.getRunId(),
                                                     metricsRequest.getTagPrefix());
    long ack = 0;
    Set<QueueName> queueNames = Sets.newHashSet();
    while (scanner.hasNext()) {
      AggregatesScanResult scanResult = scanner.next();
      ack += scanResult.getValue();
      queueNames.add(QueueName.from(URI.create(scanResult.getMetric().substring("q.ack.".length()))));
    }

    // For each queue, get the enqueue aggregate
    long enqueue = 0;
    for (QueueName queueName : queueNames) {
      if (queueName.isStream()) {
        // It's a stream, use stream context
        enqueue += sumAll(aggregatesTable.scan("-.stream", "q.enqueue." + queueName.toString()));
      } else {
        // Construct query context from the queue name and the request context
        // This is hacky. Need a refactor of how metrics, queue and opex interact
        String contextPrefix = metricsRequest.getContextPrefix();
        String appId = contextPrefix.substring(0, contextPrefix.indexOf('.'));
        String flowId = queueName.toURI().getHost();
        String flowletId = Splitter.on('/').omitEmptyStrings().split(queueName.toURI().getPath()).iterator().next();
        // The paths would be /flowId/flowletId/queueSimpleName
        enqueue += sumAll(aggregatesTable.scan(String.format("%s.f.%s.%s", appId, flowId, flowletId),
                                               "q.enqueue." + queueName.toString()));
      }
    }

    long len = enqueue - ack;
    return new AggregateResponse(len >= 0 ? len : 0);
  }

  private Iterator<TimeValue> queryTimeSeries(MetricsScope scope, MetricsScanQuery scanQuery) {
    List<Iterable<TimeValue>> timeValues = Lists.newArrayList();
    MetricsScanner scanner = metricsTableCaches.get(scope).getUnchecked(1).scan(scanQuery);
    while (scanner.hasNext()) {
      timeValues.add(scanner.next());
    }

    return new TimeValueAggregator(timeValues).iterator();
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

  private AggregateResponse getAggregates(MetricsRequest request) {
    AggregatesTable aggregatesTable = aggregatesTables.get(request.getScope());
    AggregatesScanner scanner = aggregatesTable.scan(request.getContextPrefix(), request.getMetricPrefix(),
                                                     request.getRunId(), request.getTagPrefix());
    return new AggregateResponse(sumAll(scanner));
  }

  private long sumAll(AggregatesScanner scanner) {
    long value = 0;
    while (scanner.hasNext()) {
      value += scanner.next().getValue();
    }
    return value;
  }
}
