/*
 * Copyright 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.cube;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.AggregationOption;
import io.cdap.cdap.api.dataset.lib.cube.Cube;
import io.cdap.cdap.api.dataset.lib.cube.CubeDeleteQuery;
import io.cdap.cdap.api.dataset.lib.cube.CubeExploreQuery;
import io.cdap.cdap.api.dataset.lib.cube.CubeFact;
import io.cdap.cdap.api.dataset.lib.cube.CubeQuery;
import io.cdap.cdap.api.dataset.lib.cube.DimensionValue;
import io.cdap.cdap.api.dataset.lib.cube.TimeSeries;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.dataset.metrics.MeteredDataset;
import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data2.dataset2.lib.timeseries.Fact;
import io.cdap.cdap.data2.dataset2.lib.timeseries.FactScan;
import io.cdap.cdap.data2.dataset2.lib.timeseries.FactScanResult;
import io.cdap.cdap.data2.dataset2.lib.timeseries.FactScanner;
import io.cdap.cdap.data2.dataset2.lib.timeseries.FactTable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link io.cdap.cdap.api.dataset.lib.cube.Cube}.
 */
public class DefaultCube implements Cube, MeteredDataset {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultCube.class);
  // Log the metrics processing progress no more than once per minute.
  private static final Logger PROGRESS_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(60000));

  private static final DimensionValueComparator DIMENSION_VALUE_COMPARATOR = new DimensionValueComparator();
  // hard-limit on max records to scan
  private static final int MAX_RECORDS_TO_SCAN = 100 * 1000;
  private static final EnumSet<AggregationOption> PARTITION_AGG_OPTIONS = EnumSet.of(AggregationOption.LATEST,
                                                                                     AggregationOption.SUM);

  private final Map<Integer, FactTable> resolutionToFactTable;
  private final Map<String, ? extends Aggregation> aggregations;
  private final Map<String, AggregationAlias> aggregationAliasMap;
  private final ExecutorService executorService;
  private final int writeParallelism;

  @Nullable
  private MetricsCollector metrics;

  /**
   * Created a cube with no parallel computations / writes to same resultion
   */
  public DefaultCube(int[] resolutions, FactTableSupplier factTableSupplier,
                     Map<String, ? extends Aggregation> aggregations,
                     Map<String, AggregationAlias> aggregationAliasMap) {
    this(resolutions, factTableSupplier, aggregations, aggregationAliasMap, 1);
  }

  /**
   * Creates a cube that can do up to writePrallelism parallel computations when writing data to each resolution
   * table
   */
  public DefaultCube(int[] resolutions, FactTableSupplier factTableSupplier,
                     Map<String, ? extends Aggregation> aggregations,
                     Map<String, AggregationAlias> aggregationAliasMap,
                     int writeParallelism) {
    this.aggregations = aggregations;
    this.resolutionToFactTable = Maps.newHashMap();
    for (int resolution : resolutions) {
      resolutionToFactTable.put(resolution, factTableSupplier.get(resolution, 3600));
    }
    this.aggregationAliasMap = aggregationAliasMap;
    this.writeParallelism = writeParallelism;
    ThreadPoolExecutor executor = new ThreadPoolExecutor(resolutions.length * this.writeParallelism,
                                                         resolutions.length * this.writeParallelism,
                                                         30, TimeUnit.SECONDS,
                                                         new LinkedBlockingQueue<>(),
                                                         Threads.createDaemonThreadFactory("metrics-table-%d"));
    executor.allowCoreThreadTimeOut(true);
    this.executorService = executor;
  }

  @Override
  public void add(CubeFact fact) {
    add(ImmutableList.of(fact));
  }

  @Override
  public void add(Collection<? extends CubeFact> facts) {
    // We store key in the value to use it for key deduplication while creating Fact objects
    // This would help later to compare different facts
    Map<List<DimensionValue>, Map.Entry<List<DimensionValue>, List<Fact>>> toWrite = new HashMap<>();
    int dimValuesCount = 0;
    long minTimestamp = Long.MAX_VALUE;
    long maxTimestamp = 0;
    long sumTimestamp = 0;
    long numFacts = 0;
    long numMeasurements = 0;
    AtomicLong numUpdates = new AtomicLong();
    for (CubeFact fact : facts) {
      for (Map.Entry<String, ? extends Aggregation> aggEntry : aggregations.entrySet()) {
        Aggregation agg = aggEntry.getValue();
        AggregationAlias aggregationAlias = null;

        if (aggregationAliasMap.containsKey(aggEntry.getKey())) {
          aggregationAlias = aggregationAliasMap.get(aggEntry.getKey());
        }

        if (agg.accept(fact)) {
          List<DimensionValue> dimensionValues = Lists.newArrayList();
          for (String dimensionName : agg.getDimensionNames()) {
            String dimensionValueKey =
              aggregationAlias == null ? dimensionName : aggregationAlias.getAlias(dimensionName);
            dimensionValues.add(new DimensionValue(dimensionName, fact.getDimensionValues().get(dimensionValueKey)));
            dimValuesCount++;
          }
          Map.Entry<List<DimensionValue>, List<Fact>> listEntry =
            toWrite.computeIfAbsent(dimensionValues, v -> new AbstractMap.SimpleEntry<>(v, new ArrayList<>()));
          listEntry.getValue()
            .add(new Fact(fact.getTimestamp(), listEntry.getKey(), fact.getMeasurements()));
          minTimestamp = Math.min(minTimestamp, fact.getTimestamp());
          maxTimestamp = Math.max(maxTimestamp, fact.getTimestamp());
          sumTimestamp += fact.getTimestamp();
          numFacts++;
          numMeasurements += fact.getMeasurements().size();
        }
      }
    }

    Map<Integer, List<Future<?>>> futures = new HashMap<>();
    Consumer<List<Fact>> batchWriter = batch -> {
      for (Map.Entry<Integer, FactTable> table : resolutionToFactTable.entrySet()) {
        Future<?> future = executorService.submit(() -> numUpdates.addAndGet(table.getValue().add(batch)));
        futures.computeIfAbsent(table.getKey(), k -> new ArrayList<>()).add(future);
      }
    };

    List<Fact> writeBatch = new ArrayList<>();
    int batchSize = (int) Math.min(Integer.MAX_VALUE, numFacts / writeParallelism);
    for (Map.Entry<List<DimensionValue>, List<Fact>> listEntry: toWrite.values()) {
      List<Fact> metricFacts = listEntry.getValue();
      if (metricFacts.size() > batchSize) {
        batchWriter.accept(metricFacts);
      } else if (writeBatch.size() <= batchSize - metricFacts.size()) {
        writeBatch.addAll(metricFacts);
      } else {
        batchWriter.accept(writeBatch);
        writeBatch = new ArrayList<>();
        writeBatch.addAll(metricFacts);
      }
    }
    if (!writeBatch.isEmpty()) {
      batchWriter.accept(writeBatch);
    }


    boolean failed = false;
    Exception failedException = null;
    StringBuilder failedMessage = new StringBuilder("Failed to add metrics to ");
    for (Map.Entry<Integer, List<Future<?>>> futureList: futures.entrySet()) {
      try {
        for (Future<?> future: futureList.getValue()) {
          Uninterruptibles.getUninterruptibly(future);
        }
      } catch (ExecutionException e) {
        if (!failed) {
          failed = true;
          failedMessage.append(String.format("the %d resolution table", futureList.getKey()));
        } else {
          failedMessage.append(String.format(", the %d resolution table", futureList.getKey()));
        }
        if (failedException == null) {
          failedException = e;
        } else {
          failedException.addSuppressed(e);
        }
      }
    }

    if (failed) {
      throw new RuntimeException(failedMessage.append(".").toString(), failedException);
    }

    incrementMetric("cube.cubeFact.add.request.count", 1);
    incrementMetric("cube.cubeFact.added.count", facts.size());
    incrementMetric("cube.tsFact.created.count", numFacts);
    incrementMetric("cube.tsFact.created.dimValues.count", dimValuesCount);
    incrementMetric("cube.tsFact.added.count", numFacts * resolutionToFactTable.size());
    long avgTimestamp = sumTimestamp / numFacts;
    PROGRESS_LOG.debug(
      "Persisted {} updates for {} facts with {} measurements for {} dimension sets " +
        "from {} cube facts for timestamps {}..{} (avg {}, lag {}s)",
      numUpdates.get(), numFacts, numMeasurements, toWrite.size(), facts.size(), minTimestamp, maxTimestamp,
      avgTimestamp, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - avgTimestamp);
  }

  @Override
  public Collection<TimeSeries> query(CubeQuery query) {
    /*
      CubeQuery example: "dataset read ops for app per dataset". Or:

      SELECT count('read.ops')                                           << measure name and type
      FROM aggregation1.1min_resolution                                  << aggregation and resolution
      GROUP BY dataset,                                                  << groupByDimensions
      WHERE namespace='ns1' AND app='myApp' AND program='myFlow' AND     << dimensionValues
            ts>=1423370200 AND ts{@literal<}1423398198                   << startTs and endTs
      LIMIT 100                                                          << limit

      Execution:

      1) (optional, if aggregation to query in is not provided) find aggregation to supply results

      Here, we need aggregation that has following dimensions: 'namespace', 'app', 'program', 'dataset'.

      Ideally (to reduce the scan range), 'dataset' should be in the end, other dimensions as close to the beginning
      as possible, and minimal number of other "unspecified" dimensions.

      Let's say we found aggregation: 'namespace', 'app', 'program', 'instance', 'dataset'

      2) build a scan in the aggregation

      For scan we set "any" into the dimension values that aggregation has but query doesn't define value for:

      'namespace'='ns1', 'app'='myApp', 'program'='myFlow', 'instance'=*, 'dataset'=*

      Plus specified measure & aggregation?:

      'measureName'='read.ops'
      'measureType'='COUNTER'

      3) While scanning build a table: dimension values -> time -> value. Use measureType as values aggregate
         function if needed.
    */

    incrementMetric("cube.query.request.count", 1);

    if (!resolutionToFactTable.containsKey(query.getResolution())) {
      incrementMetric("cube.query.request.failure.count", 1);
      throw new IllegalArgumentException("There's no data aggregated for specified resolution to satisfy the query: " +
                                           query.toString());
    }

    // 1) find aggregation to query
    Aggregation agg;
    String aggName;
    if (query.getAggregation() != null) {
      aggName = query.getAggregation();
      agg = aggregations.get(query.getAggregation());
      if (agg == null) {
        incrementMetric("cube.query.request.failure.count", 1);
        throw new IllegalArgumentException(
          String.format("Specified aggregation %s is not found in cube aggregations: %s",
                        query.getAggregation(), aggregations.keySet().toString()));
      }
    } else {
      ImmutablePair<String, Aggregation> aggregation = findAggregation(query);
      if (aggregation == null) {
        incrementMetric("cube.query.request.failure.count", 1);
        throw new IllegalArgumentException("There's no data aggregated for specified dimensions " +
                                             "to satisfy the query: " + query.toString());
      }
      agg = aggregation.getSecond();
      aggName = aggregation.getFirst();
    }

    // tell how many queries end up querying specific pre-aggregated views and resolutions
    incrementMetric("cube.query.agg." + aggName + ".count", 1);
    incrementMetric("cube.query.res." + query.getResolution() + ".count", 1);

    // 2) build a scan for a query
    List<DimensionValue> dimensionValues = Lists.newArrayList();
    for (String dimensionName : agg.getDimensionNames()) {
      // if not defined in query, will be set as null, which means "any"
      dimensionValues.add(new DimensionValue(dimensionName, query.getDimensionValues().get(dimensionName)));
    }

    FactScan scan = new FactScan(query.getStartTs(), query.getEndTs(),
                                 query.getMeasurements().keySet(), dimensionValues);

    // 3) execute scan query
    FactTable table = resolutionToFactTable.get(query.getResolution());
    FactScanner scanner = table.scan(scan);
    Table<Map<String, String>, String, Map<Long, Long>> resultMap = getTimeSeries(query, scanner);

    incrementMetric("cube.query.request.success.count", 1);
    incrementMetric("cube.query.result.size", resultMap.size());

    Collection<TimeSeries> timeSeries = convertToQueryResult(query, resultMap);
    incrementMetric("cube.query.result.timeseries.count", timeSeries.size());

    return timeSeries;
  }

  @Override
  public void delete(CubeDeleteQuery query) {
    //this may be very inefficient and its better to use TTL, this is to only support existing old functionality.
    List<DimensionValue> dimensionValues = Lists.newArrayList();
    // find all the aggregations that match the dimensionValues in the query and
    // use the dimension values of the aggregation to delete entries in all the fact-tables.
    for (Aggregation agg : aggregations.values()) {
      if (query.getTagPredicate().test(agg.getDimensionNames())) {
        dimensionValues.clear();
        for (String dimensionName : agg.getDimensionNames()) {
          dimensionValues.add(new DimensionValue(dimensionName, query.getDimensionValues().get(dimensionName)));
        }
        FactTable factTable = resolutionToFactTable.get(query.getResolution());
        FactScan scan = new FactScan(query.getStartTs(), query.getEndTs(), query.getMeasureNames(), dimensionValues);
        factTable.delete(scan);
      }
    }
  }

  @Override
  public Collection<DimensionValue> findDimensionValues(CubeExploreQuery query) {
    LOG.trace("Searching for next-level context, query: {}", query);

    // In each aggregation that matches given dimensions, try to fill in value in a single null-valued given dimension.
    // NOTE: that we try to fill in first value that is non-null-valued in a stored record
    //       (see FactTable#findSingleDimensionValue)
    SortedSet<DimensionValue> result = Sets.newTreeSet(DIMENSION_VALUE_COMPARATOR);

    // todo: the passed query should have map instead
    LinkedHashMap<String, String> slice = Maps.newLinkedHashMap();
    for (DimensionValue dimensionValue : query.getDimensionValues()) {
      slice.put(dimensionValue.getName(), dimensionValue.getValue());
    }

    FactTable table = resolutionToFactTable.get(query.getResolution());

    for (Aggregation agg : aggregations.values()) {
      if (agg.getDimensionNames().containsAll(slice.keySet())) {
        result.addAll(table.findSingleDimensionValue(agg.getDimensionNames(), slice,
                                                     query.getStartTs(), query.getEndTs()));
      }
    }

    return result;
  }

  @Override
  public Collection<String> findMeasureNames(CubeExploreQuery query) {
    LOG.trace("Searching for measures, query: {}", query);

    // In each aggregation that matches given dimensions, try to find measure names
    SortedSet<String> result = Sets.newTreeSet();

    // todo: the passed query should have map instead
    LinkedHashMap<String, String> slice = Maps.newLinkedHashMap();
    for (DimensionValue dimensionValue : query.getDimensionValues()) {
      slice.put(dimensionValue.getName(), dimensionValue.getValue());
    }

    FactTable table = resolutionToFactTable.get(query.getResolution());

    for (Aggregation agg : aggregations.values()) {
      if (agg.getDimensionNames().containsAll(slice.keySet())) {
        result.addAll(table.findMeasureNames(agg.getDimensionNames(), slice, query.getStartTs(), query.getEndTs()));
      }
    }

    return result;
  }

  /**
   * Sets {@link MetricsCollector} for metrics reporting.
   * @param metrics {@link MetricsCollector} to set.
   */
  @Override
  public void setMetricsCollector(MetricsCollector metrics) {
    this.metrics = metrics;
    for (FactTable factTable : resolutionToFactTable.values()) {
      factTable.setMetricsCollector(metrics);
    }
  }

  private void incrementMetric(String metricName, long value) {
    if (metrics != null) {
      metrics.increment(metricName, value);
    }
  }

  @Nullable
  private ImmutablePair<String, Aggregation> findAggregation(CubeQuery query) {
    ImmutablePair<String, Aggregation> currentBest = null;

    for (Map.Entry<String, ? extends Aggregation> entry : aggregations.entrySet()) {
      Aggregation agg = entry.getValue();
      if (agg.getDimensionNames().containsAll(query.getGroupByDimensions()) &&
        agg.getDimensionNames().containsAll(query.getDimensionValues().keySet())) {

        // todo: choose aggregation smarter than just by number of dimensions :)
        if (currentBest == null ||
          currentBest.getSecond().getDimensionNames().size() > agg.getDimensionNames().size()) {
          currentBest = new ImmutablePair<>(entry.getKey(), agg);
        }
      }
    }

    return currentBest;
  }

  private Table<Map<String, String>, String, Map<Long, Long>> getTimeSeries(CubeQuery query, FactScanner scanner) {
    // {dimension values, measure} -> {time -> value}s
    Table<Map<String, String>, String, Map<Long, Long>> result = HashBasedTable.create();

    int count = 0;
    while (scanner.hasNext()) {
      FactScanResult next = scanner.next();
      incrementMetric("cube.query.scan.records.count", 1);

      boolean skip = false;
      // using tree map, as we are using it as a key for a map
      Map<String, String> seriesDimensions = Maps.newTreeMap();
      for (String dimensionName : query.getGroupByDimensions()) {
        // todo: use Map<String, String> instead of List<DimensionValue> into a String, String, everywhere
        for (DimensionValue dimensionValue : next.getDimensionValues()) {
          if (dimensionName.equals(dimensionValue.getName())) {
            if (dimensionValue.getValue() == null) {
              // Currently, we do NOT return null as grouped by value.
              // Depending on whether dimension is required or not the records with null value in it may or may not be
              // in aggregation. At this moment, the choosing of the aggregation for query doesn't look at this, so
              // potentially null may or may not be included in results, depending on the aggregation selected
              // querying. We don't want to produce inconsistent results varying due to different aggregations selected,
              // so don't return nulls in any of those cases.
              skip = true;
              continue;
            }
            seriesDimensions.put(dimensionName, dimensionValue.getValue());
            break;
          }
        }
      }

      if (skip) {
        incrementMetric("cube.query.scan.skipped.count", 1);
        continue;
      }

      for (TimeValue timeValue : next) {
        Map<Long, Long> timeValues = result.get(seriesDimensions, next.getMeasureName());
        if (timeValues == null) {
          result.put(seriesDimensions, next.getMeasureName(), Maps.<Long, Long>newHashMap());
        }

        AggregationFunction function = query.getMeasurements().get(next.getMeasureName());
        if (AggregationFunction.SUM == function) {
          Long value =  result.get(seriesDimensions, next.getMeasureName()).get(timeValue.getTimestamp());
          value = value == null ? 0 : value;
          value += timeValue.getValue();
          result.get(seriesDimensions, next.getMeasureName()).put(timeValue.getTimestamp(), value);
        } else if (AggregationFunction.MAX == function) {
          Long value = result.get(seriesDimensions, next.getMeasureName()).get(timeValue.getTimestamp());
          value = value != null && value > timeValue.getValue() ? value : timeValue.getValue();
          result.get(seriesDimensions, next.getMeasureName()).put(timeValue.getTimestamp(), value);
        } else if (AggregationFunction.MIN == function) {
          Long value =  result.get(seriesDimensions, next.getMeasureName()).get(timeValue.getTimestamp());
          value = value != null && value < timeValue.getValue() ? value : timeValue.getValue();
          result.get(seriesDimensions, next.getMeasureName()).put(timeValue.getTimestamp(), value);
        } else if (AggregationFunction.LATEST == function) {
          result.get(seriesDimensions, next.getMeasureName()).put(timeValue.getTimestamp(), timeValue.getValue());
        } else {
          // should never happen: developer error
          throw new RuntimeException("Unknown MeasureType: " + function);
        }
      }
      if (++count >= MAX_RECORDS_TO_SCAN) {
        break;
      }
    }
    return result;
  }

  private Collection<TimeSeries> convertToQueryResult(
    CubeQuery query, Table<Map<String, String>, String, Map<Long, Long>> resultTable) {
    List<TimeSeries> result = new ArrayList<>();
    // iterating each groupValue dimensions
    for (Map.Entry<Map<String, String>, Map<String, Map<Long, Long>>> row : resultTable.rowMap().entrySet()) {
      // iterating each measure
      for (Map.Entry<String, Map<Long, Long>> measureEntry : row.getValue().entrySet()) {
        // generating time series for a grouping and a measure
        int count = 0;
        List<TimeValue> timeValues = new ArrayList<>();
        for (Map.Entry<Long, Long> timeValue : measureEntry.getValue().entrySet()) {
          timeValues.add(new TimeValue(timeValue.getKey(), timeValue.getValue()));
        }
        Collections.sort(timeValues);
        List<TimeValue> resultTimeValues = new ArrayList<>();

        AggregationOption aggregationOption = query.getAggregationOption();
        // this should not happen in production, since the check has been made in the handler
        if (query.getLimit() <= 0) {
          throw new IllegalArgumentException("The query limit cannot be less than 0");
        }
        // only partition the data points if the data points are larger than the required limit and only do it for
        // option LATEST and SUM.
        if (query.getLimit() < timeValues.size() && PARTITION_AGG_OPTIONS.contains(aggregationOption)) {
          int partitionSize = timeValues.size() / query.getLimit();
          int remainder = timeValues.size() % query.getLimit();
          // ignore the first reminderth data points
          for (List<TimeValue> interval : Iterables.partition(timeValues.subList(remainder,
                                                                                 timeValues.size()), partitionSize)) {
            // for LATEST we only need to get the last data point in the interval
            if (aggregationOption.equals(AggregationOption.LATEST)) {
              resultTimeValues.add(interval.get(interval.size() - 1));
              continue;
            }
            // for SUM we want to sum up all the values in the interval
            if (aggregationOption.equals(AggregationOption.SUM)) {
              long sum = interval.stream().mapToLong(TimeValue::getValue).sum();
              resultTimeValues.add(new TimeValue(interval.get(interval.size() - 1).getTimestamp(), sum));
            }
          }
        } else {
          // TODO: CDAP-15565 remove the interpolation logic since it is never maintained and adds huge complexity
          PeekingIterator<TimeValue> timeValueItor = Iterators.peekingIterator(
            new TimeSeriesInterpolator(timeValues, query.getInterpolator(), query.getResolution()).iterator());
          while (timeValueItor.hasNext()) {
            TimeValue timeValue = timeValueItor.next();
            resultTimeValues.add(new TimeValue(timeValue.getTimestamp(), timeValue.getValue()));
            if (++count >= query.getLimit()) {
              break;
            }
          }
        }
        result.add(new TimeSeries(measureEntry.getKey(), row.getKey(), resultTimeValues));
      }
    }
    return result;
  }

  @Override
  public void write(Object ignored, CubeFact cubeFact) {
    add(cubeFact);
  }

  @Override
  public void close() throws IOException {
    try {
      for (FactTable factTable : resolutionToFactTable.values()) {
        factTable.close();
      }
    } finally {
      executorService.shutdown();
    }
  }

  private static final class DimensionValueComparator implements Comparator<DimensionValue> {
    @Override
    public int compare(DimensionValue t1, DimensionValue t2) {
      int cmp = t1.getName().compareTo(t2.getName());
      if (cmp != 0) {
        return cmp;
      }
      if (t1.getValue() == null) {
        if (t2.getValue() == null) {
          return 0;
        } else {
          return -1;
        }
      }
      if (t2.getValue() == null) {
        return 1;
      }
      return t1.getValue().compareTo(t2.getValue());
    }

  }
}
