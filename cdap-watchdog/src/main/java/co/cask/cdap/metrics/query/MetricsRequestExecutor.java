/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.metrics.query;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.metrics.data.AggregatesScanResult;
import co.cask.cdap.metrics.data.AggregatesScanner;
import co.cask.cdap.metrics.data.AggregatesTable;
import co.cask.cdap.metrics.data.Interpolator;
import co.cask.cdap.metrics.data.Interpolators;
import co.cask.cdap.metrics.data.MetricsScanQuery;
import co.cask.cdap.metrics.data.MetricsScanQueryBuilder;
import co.cask.cdap.metrics.data.MetricsScanResult;
import co.cask.cdap.metrics.data.MetricsScanner;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import co.cask.cdap.metrics.data.TimeSeriesTables;
import co.cask.cdap.metrics.data.TimeValue;
import co.cask.cdap.metrics.data.TimeValueAggregator;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
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

  private final Supplier<AggregatesTable> aggregatesTables;
  private final TimeSeriesTables timeSeriesTables;

  public MetricsRequestExecutor(final MetricsTableFactory metricsTableFactory) {
    this.timeSeriesTables = new TimeSeriesTables(metricsTableFactory);
    this.aggregatesTables = Suppliers.memoize(new Supplier<AggregatesTable>() {
      @Override
      public AggregatesTable get() {
        return metricsTableFactory.createAggregates();
      }
    });
  }

  public JsonElement executeQuery(MetricsRequest metricsRequest) throws IOException, OperationException {

    // Pretty ugly logic now. Need to refactor
    Object resultObj = null;
    if (metricsRequest.getType() == MetricsRequest.Type.TIME_SERIES) {
      TimeSeriesResponse.Builder builder = TimeSeriesResponse.builder(metricsRequest.getStartTime(),
                                                                      metricsRequest.getEndTime());
      // Special metrics handle that requires computation from multiple time series.
      if ("system.process.busyness".equals(metricsRequest.getMetricPrefix())) {
        computeProcessBusyness(metricsRequest, builder);
      } else {
        MetricsScanQuery scanQuery = createScanQuery(metricsRequest);

        PeekingIterator<TimeValue> timeValueItor = Iterators.peekingIterator(
          queryTimeSeries(scanQuery,
                          metricsRequest.getInterpolator(),
                          metricsRequest.getTimeSeriesResolution()));

        // if this is an interpolated timeseries, we might have extended the "start" in order to interpolate.
        // so fast forward the iterator until we we're inside the actual query time window.
        if (metricsRequest.getInterpolator() != null) {
          while (timeValueItor.hasNext() &&
            ((timeValueItor.peek().getTime() +
              metricsRequest.getTimeSeriesResolution()) <= metricsRequest.getStartTime())) {
            timeValueItor.next();
          }
        }
        long resolution = metricsRequest.getTimeSeriesResolution();
        long resultTimeStamp = (metricsRequest.getStartTime() / resolution) * resolution;

        for (int i = 0; i < metricsRequest.getCount(); i++) {
          if (timeValueItor.hasNext() && timeValueItor.peek().getTime() == resultTimeStamp) {
            builder.addData(resultTimeStamp, timeValueItor.next().getValue());
          } else {
            // If the scan result doesn't have value for a timestamp, we add 0 to the result-returned for that timestamp
            builder.addData(resultTimeStamp, 0);
          }
          resultTimeStamp += metricsRequest.getTimeSeriesResolution();
        }
      }
      resultObj = builder.build();

    } else if (metricsRequest.getType() == MetricsRequest.Type.AGGREGATE) {
      // Special metrics handle that requires computation from multiple aggregates results.
      if ("system.process.events.pending".equals(metricsRequest.getMetricPrefix())) {
        resultObj = computeQueueLength(metricsRequest);
      } else {
        resultObj = getAggregates(metricsRequest);
      }
    }

    return GSON.toJsonTree(resultObj);
  }

  private void computeProcessBusyness(MetricsRequest metricsRequest, TimeSeriesResponse.Builder builder)
    throws OperationException {
    int resolution = metricsRequest.getTimeSeriesResolution();
    long start = metricsRequest.getStartTime() / resolution * resolution;
    long end = (metricsRequest.getEndTime() / resolution) * resolution;
    MetricsScanQuery scanQuery = new MetricsScanQueryBuilder()
      .setContext(metricsRequest.getContextPrefix())
      .setMetric("system.process.tuples.read")
      .build(start, end);

    PeekingIterator<TimeValue> tuplesReadItor =
      Iterators.peekingIterator(queryTimeSeries(scanQuery, metricsRequest.getInterpolator(),
                                                metricsRequest.getTimeSeriesResolution()));

    scanQuery = new MetricsScanQueryBuilder()
      .setContext(metricsRequest.getContextPrefix())
      .setMetric("system.process.events.processed")
      .build(metricsRequest.getStartTime(), metricsRequest.getEndTime());

    PeekingIterator<TimeValue> eventsProcessedItor =
      Iterators.peekingIterator(queryTimeSeries(scanQuery, metricsRequest.getInterpolator(),
                                                metricsRequest.getTimeSeriesResolution()));
    long resultTimeStamp = start;

    for (int i = 0; i < metricsRequest.getCount(); i++) {
      long tupleRead = 0;
      long eventProcessed = 0;
      if (tuplesReadItor.hasNext() && tuplesReadItor.peek().getTime() == resultTimeStamp) {
        tupleRead = tuplesReadItor.next().getValue();
      }
      if (eventsProcessedItor.hasNext() && eventsProcessedItor.peek().getTime() == resultTimeStamp) {
        eventProcessed = eventsProcessedItor.next().getValue();
      }
      if (eventProcessed != 0) {
        int busyness = (int) ((float) tupleRead / eventProcessed * 100);
        builder.addData(resultTimeStamp, busyness > 100 ? 100 : busyness);
      } else {
        // If the scan result doesn't have value for a timestamp, we add 0 to the returned result for that timestamp.
        builder.addData(resultTimeStamp, 0);
      }
      resultTimeStamp += metricsRequest.getTimeSeriesResolution();
    }
  }

  private Object computeQueueLength(MetricsRequest metricsRequest) {
    AggregatesTable aggregatesTable = aggregatesTables.get();

    // process.events.processed will have a tag like "input.queue://PurchaseFlow/reader/queue" which indicates
    // where the processed event came from.  So first get the aggregate count for events processed and all the
    // queues they came from. Next, for all those queues, get the aggregate count for events they wrote,
    // and subtract the two to get queue length.
    AggregatesScanner scanner = aggregatesTable.scan(metricsRequest.getContextPrefix(),
                                                     "system.process.events.processed",
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
        String context = String.format("%s.%s.f.%s.%s",
                                       queueName.getFirstComponent(), // the namespace
                                       queueName.getSecondComponent(), // the app
                                       queueName.getThirdComponent(), // the flow
                                       queueName.getFourthComponent()); // the flowlet
        queueNameContexts.add(new ImmutablePair<String, String>(queueName.getSimpleName(), context));
      } else {
        LOG.warn("unknown type of queue name {} ", queueName.toString());
      }
    }

    // For each queue, get the enqueue aggregate
    long enqueue = 0;
    for (ImmutablePair<String, String> pair : queueNameContexts) {
      // The paths would be /flowId/flowletId/queueSimpleName
      enqueue += sumAll(aggregatesTable.scan(pair.getSecond(), "system.process.events.out", "0", pair.getFirst()));
    }
    for (String streamName : streamNames) {
      String ctx = String.format("%s.%s.%s",
                                 Constants.SYSTEM_NAMESPACE,
                                 Constants.Gateway.METRICS_CONTEXT,
                                 Constants.Gateway.STREAM_HANDLER_NAME);
      enqueue += sumAll(aggregatesTable.scan(ctx, "system.collect.events", "0", streamName));
    }

    long len = enqueue - processed;
    return new AggregateResponse(len >= 0 ? len : 0);
  }

  private Iterator<TimeValue> queryTimeSeries(MetricsScanQuery scanQuery, Interpolator interpolator, int resolution)
    throws OperationException {

    Map<TimeseriesId, Iterable<TimeValue>> timeValues = Maps.newHashMap();
    MetricsScanner scanner = timeSeriesTables.scan(resolution, scanQuery);
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
    AggregatesTable aggregatesTable = aggregatesTables.get();
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
    int resolution = request.getTimeSeriesResolution();
    long start = request.getStartTime() / resolution * resolution;
    long end = (request.getEndTime() / resolution) * resolution;

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
      .setRunId(request.getRunId())
      .build(start, end);
  }
}
