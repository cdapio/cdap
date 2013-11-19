package com.continuuity.metrics.query;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data2.OperationException;
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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Executes metrics requests, returning a json object representing the result of the request.
 */
public class MetricsRequestExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsRequestExecutor.class);
  private static final Gson GSON = new Gson();

  // It's a cache from metric table resolution to MetricsTable
  private final Map<MetricsScope, LoadingCache<Integer, TimeSeriesTable>> metricsTableCaches;
  private final Map<MetricsScope, AggregatesTable> aggregatesTables;

  public MetricsRequestExecutor(final MetricsTableFactory metricsTableFactory) {
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

  public JsonElement executeQuery(MetricsRequest metricsRequest) throws IOException, OperationException {

    // Pretty ugly logic now. Need to refactor
    Object resultObj = null;
    if (metricsRequest.getType() == MetricsRequest.Type.TIME_SERIES) {
      TimeSeriesResponse.Builder builder = TimeSeriesResponse.builder(metricsRequest.getStartTime(),
                                                                      metricsRequest.getEndTime());
      // Special metrics handle that requires computation from multiple time series.
      if ("process.busyness".equals(metricsRequest.getMetricPrefix())) {
        computeProcessBusyness(metricsRequest, builder);
      } else {
        MetricsScanQuery scanQuery = createScanQuery(metricsRequest);

        PeekingIterator<TimeValue> timeValueItor = Iterators.peekingIterator(queryTimeSeries(metricsRequest.getScope(),
                                                                                     scanQuery,
                                                                                     metricsRequest.getInterpolator()));

        // if this is an interpolated timeseries, we might have extended the "start" in order to interpolate.
        // so fast forward the iterator until we we're inside the actual query time window.
        while (timeValueItor.hasNext() && (timeValueItor.peek().getTime() < metricsRequest.getStartTime())) {
          timeValueItor.next();
        }

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

    return GSON.toJsonTree(resultObj);
  }

  private void computeProcessBusyness(MetricsRequest metricsRequest, TimeSeriesResponse.Builder builder)
    throws OperationException {
    MetricsScanQuery scanQuery = new MetricsScanQueryBuilder()
      .setContext(metricsRequest.getContextPrefix())
      .setMetric("process.tuples.read")
      .build(metricsRequest.getStartTime(), metricsRequest.getEndTime());
    MetricsScope scope = metricsRequest.getScope();

    PeekingIterator<TimeValue> tuplesReadItor =
      Iterators.peekingIterator(queryTimeSeries(scope, scanQuery, metricsRequest.getInterpolator()));

    scanQuery = new MetricsScanQueryBuilder()
      .setContext(metricsRequest.getContextPrefix())
      .setMetric("process.events.processed")
      .build(metricsRequest.getStartTime(), metricsRequest.getEndTime());

    PeekingIterator<TimeValue> eventsProcessedItor =
      Iterators.peekingIterator(queryTimeSeries(scope, scanQuery, metricsRequest.getInterpolator()));

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
    AggregatesTable aggregatesTable = aggregatesTables.get(metricsRequest.getScope());

    // process.events.processed will have a tag like "input.queue://PurchaseFlow/reader/queue" which indicates
    // where the processed event came from.  So first get the aggregate count for events processed and all the
    // queues they came from. Next, for all those queues, get the aggregate count for events they wrote,
    // and subtract the two to get queue length.
    AggregatesScanner scanner = aggregatesTable.scan(metricsRequest.getContextPrefix(),
                                                     "process.events.processed",
                                                     metricsRequest.getRunId(),
                                                     "input");

    long processed = 0;
    Set<String> streamNames = Sets.newHashSet();
    Set<ImmutablePair<String, String>> queueNameContexts = Sets.newHashSet();
    while (scanner.hasNext()) {
      AggregatesScanResult scanResult = scanner.next();
      processed += scanResult.getValue();
      // tag is of the form input.[queueURI].  ex: input.queue://PurchaseFlow/reader/queue
      String tag = scanResult.getTag();
      // strip the preceding "input." from the tag.
      QueueName queueName = QueueName.from(URI.create(tag.substring(6, tag.length())));
      if (queueName.isStream()) {
        streamNames.add(queueName.getSimpleName());
      } else if (queueName.isQueue()) {
        String context = String.format("%s.f.%s.%s",
                                       queueName.getFirstComponent(), // the app
                                       queueName.getSecondComponent(), // the flow
                                       queueName.getThirdComponent()); // the flowlet
        queueNameContexts.add(new ImmutablePair<String, String>(queueName.getSimpleName(), context));
      } else {
        LOG.warn("unknown type of queue name {} ", queueName.toString());
      }
    }

    // For each queue, get the enqueue aggregate
    long enqueue = 0;
    for (ImmutablePair<String, String> pair : queueNameContexts) {
      // The paths would be /flowId/flowletId/queueSimpleName
      enqueue += sumAll(aggregatesTable.scan(pair.getSecond(), "process.events.out", "0", pair.getFirst()));
    }
    for (String streamName : streamNames) {
      enqueue += sumAll(aggregatesTable.scan(Constants.Gateway.METRICS_CONTEXT, "collect.events", "0", streamName));
    }

    long len = enqueue - processed;
    return new AggregateResponse(len >= 0 ? len : 0);
  }

  private Iterator<TimeValue> queryTimeSeries(MetricsScope scope, MetricsScanQuery scanQuery,
                                              Interpolator interpolator) throws OperationException {
    Map<TimeseriesId, Iterable<TimeValue>> timeValues = Maps.newHashMap();
    MetricsScanner scanner = metricsTableCaches.get(scope).getUnchecked(1).scan(scanQuery);
    while (scanner.hasNext()) {
      MetricsScanResult res = scanner.next();
      // if we get multiple scan results for the same logical timeseries, concatenate them together.
      // Needed if we need to interpolate across scan results.  Using the fact that the is a scan
      // over an ordered table, so the earlier timeseries is guaranteed to come first.
      TimeseriesId timeseriesId = new TimeseriesId(res.getContext(), res.getMetric(), res.getTag(), res.getRunId());
      if (!timeValues.containsKey(timeseriesId)) {
        timeValues.put(timeseriesId, res);
      } else {
        timeValues.put(timeseriesId, Iterables.concat(timeValues.get(timeseriesId), res));
      }
    }

    return new TimeValueAggregator(timeValues.values(), interpolator).iterator();
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

  private MetricsScanQuery createScanQuery(MetricsRequest request) {
    long start = request.getStartTime();
    long end = request.getEndTime();

    // if we're interpolating, expand the time window a little to allow interpolation at the start and end.
    // Before returning the results, we'll make sure to only return what the client requested.
    Interpolator interpolator = request.getInterpolator();
    if (interpolator != null) {
      // try and expand the window by the max allowed gap for interpolation, but cap it so we dont have
      // super big windows.  The worry being that somebody sets the max allowed gap to Long.MAX_VALUE
      // to tell us to always interpolate.
      long expandCap = Math.max(Interpolators.DEFAULT_MAX_ALLOWED_GAP, (end - start) / 4);
      start -= Math.min(interpolator.getMaxAllowedGap(), expandCap);
      end += Math.min(interpolator.getMaxAllowedGap(), expandCap);
      // no use going past the current time
      end = Math.min(end, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
    }

    return new MetricsScanQueryBuilder()
      .setContext(request.getContextPrefix())
      .setMetric(request.getMetricPrefix())
      .setTag(request.getTagPrefix())
      .build(start, end);
  }
}
