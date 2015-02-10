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

import co.cask.cdap.metrics.data.TimeValue;
import co.cask.cdap.metrics.store.MetricStore;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.cdap.metrics.store.cube.TimeSeries;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Executes metrics requests, returning a json object representing the result of the request.
 */
// todo: remove it when v2/metrics APIs are gone
public class MetricStoreRequestExecutor {
  private static final Gson GSON = new Gson();

  private final MetricStore metricStore;

  public MetricStoreRequestExecutor(final MetricStore metricStore) {
    this.metricStore = metricStore;
  }

  public JsonElement executeQuery(CubeQuery query) throws Exception {
    // Pretty ugly logic now. Need to refactor
    Object resultObj;
    if (query.getResolution() != Integer.MAX_VALUE) {
      TimeSeriesResponse.Builder builder = TimeSeriesResponse.builder(query.getStartTs(),
                                                                      query.getEndTs());
      // Special metrics handle that requires computation from multiple time series.
      if ("system.process.busyness".equals(query.getMeasureName())) {
        computeProcessBusyness(query, builder);
      } else {
        PeekingIterator<TimeValue> timeValueItor = Iterators.peekingIterator(queryTimeSeries(query));

        long resultTimeStamp = (query.getStartTs() / query.getResolution()) * query.getResolution();

        for (int i = 0; i < query.getLimit(); i++) {
          if (timeValueItor.hasNext() && timeValueItor.peek().getTime() == resultTimeStamp) {
            builder.addData(resultTimeStamp, timeValueItor.next().getValue());
          } else {
            // If the scan result doesn't have value for a timestamp, we add 0 to the result-returned for that timestamp
            builder.addData(resultTimeStamp, 0);
          }
          resultTimeStamp += query.getResolution();
        }
      }
      resultObj = builder.build();

    } else {
      // Special metrics handle that requires computation from multiple aggregates results.
      if ("system.process.events.pending".equals(query.getMeasureName())) {
        resultObj = computeQueueLength(query);
      } else {
        resultObj = getAggregates(query);
      }
    }

    return GSON.toJsonTree(resultObj);
  }

  private void computeProcessBusyness(CubeQuery query, TimeSeriesResponse.Builder builder) throws Exception {
    PeekingIterator<TimeValue> tuplesReadItor =
      Iterators.peekingIterator(queryTimeSeries(new CubeQuery(query, "system.process.tuples.read")));

    PeekingIterator<TimeValue> eventsProcessedItor =
      Iterators.peekingIterator(queryTimeSeries(new CubeQuery(query, "system.process.events.processed")));

    long resultTimeStamp = query.getStartTs();

    for (int i = 0; i < query.getLimit(); i++) {
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
      resultTimeStamp += query.getResolution();
    }
  }

  private Object computeQueueLength(CubeQuery query) {
    // todo
    return null;
  }

  private Iterator<TimeValue> queryTimeSeries(CubeQuery query) throws Exception {

    Collection<TimeSeries> result = metricStore.query(query);
    if (result.size() == 0) {
      return new ArrayList<TimeValue>().iterator();
    }

    // since there's no group by condition, it'll return single time series always
    TimeSeries timeSeries = result.iterator().next();

    return Iterables.transform(timeSeries.getTimeValues(),
                        new Function<co.cask.cdap.metrics.store.timeseries.TimeValue, TimeValue>() {
      @Override
      public TimeValue apply(co.cask.cdap.metrics.store.timeseries.TimeValue input) {
        return new TimeValue(input.getTimestamp(), input.getValue());
      }
    }).iterator();
  }

  private AggregateResponse getAggregates(CubeQuery query) throws Exception {
    // query must have resolution set to Integer.MAX_VALUE (i.e. "totals")

    Collection<TimeSeries> result = metricStore.query(query);
    if (result.size() == 0) {
      return new AggregateResponse(0);
    }

    // since there's no group by condition, it'll return single time series always
    TimeSeries timeSeries = result.iterator().next();

    if (timeSeries.getTimeValues().isEmpty()) {
      return new AggregateResponse(0);
    }

    // since it is totals, it will have only one TimeValue or none

    return new AggregateResponse(timeSeries.getTimeValues().get(0).getValue());
  }
}
