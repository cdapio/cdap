/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.metrics.store;

import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.api.metrics.TimeValue;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.store.cube.Aggregation;
import co.cask.cdap.metrics.store.cube.Cube;
import co.cask.cdap.metrics.store.cube.CubeDeleteQuery;
import co.cask.cdap.metrics.store.cube.CubeExploreQuery;
import co.cask.cdap.metrics.store.cube.CubeFact;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.cdap.metrics.store.cube.DefaultAggregation;
import co.cask.cdap.metrics.store.cube.DefaultCube;
import co.cask.cdap.metrics.store.cube.FactTableSupplier;
import co.cask.cdap.metrics.store.cube.TimeSeries;
import co.cask.cdap.metrics.store.timeseries.FactTable;
import co.cask.cdap.metrics.store.timeseries.MeasureType;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.List;

/**
 * Default implementation of {@link MetricStore}.
 */
public class DefaultMetricStore implements MetricStore {
  public static final int TOTALS_RESOLUTION = Integer.MAX_VALUE;
  private final int resolutions[];
  private final Supplier<Cube> cube;

  @Inject
  public DefaultMetricStore(final MetricDatasetFactory dsFactory) {
    this(dsFactory, new int[] {1, 60, 3600, TOTALS_RESOLUTION});
  }

  // NOTE: should never be used apart from data migration during cdap upgrade
  public DefaultMetricStore(final MetricDatasetFactory dsFactory, final int resolutions[]) {
    this.resolutions = resolutions;
    final FactTableSupplier factTableSupplier = new FactTableSupplier() {
      @Override
      public FactTable get(int resolution, int ignoredRollTime) {
        // roll time will be taken from configuration todo: clean this up
        return dsFactory.get(resolution);
      }
    };
    this.cube = Suppliers.memoize(new Supplier<Cube>() {
      @Override
      public Cube get() {
        // 1 sec, 1 min, 1 hour and "all time totals"
        return new DefaultCube(resolutions, factTableSupplier, createAggregations());
      }
    });
  }

  private static List<Aggregation> createAggregations() {
    // NOTE: changing aggregations will require more work than just changing the below code. See CDAP-1466 for details.
    List<Aggregation> aggs = Lists.newLinkedList();

    // Namespaces:
    aggs.add(new DefaultAggregation(ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE)));

    // Applications:
    aggs.add(new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP, Constants.Metrics.Tag.DATASET),
      // i.e. for programs only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP)));

    // Programs:

    // Note that dataset tag goes before runId and such. This is a trade-off between efficiency of two query types:
    // * program metrics
    // * dataset metrics per program
    // It makes the former a bit slower, but bearable, as program usually doesn't access many datasets. While it speeds
    // up the latter significantly, otherwise (if dataset tag is after runId and such) queries like
    // "writes into dataset A per program" would be potentially scannig thru whole program history.

    // flow
    aggs.add(new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.FLOW, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID, Constants.Metrics.Tag.FLOWLET,
                       Constants.Metrics.Tag.INSTANCE_ID, Constants.Metrics.Tag.FLOWLET_QUEUE),
      // i.e. for flows only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.FLOW)));
    // mapreduce
    aggs.add(new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.MAPREDUCE, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID, Constants.Metrics.Tag.MR_TASK_TYPE,
                       Constants.Metrics.Tag.INSTANCE_ID),
      // i.e. for mapreduce only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.MAPREDUCE)));
    // service
    aggs.add(new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.SERVICE, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID, Constants.Metrics.Tag.SERVICE_RUNNABLE,
                       Constants.Metrics.Tag.INSTANCE_ID),
      // i.e. for service only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.SERVICE)));

    // procedure
    aggs.add(new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.PROCEDURE, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID, Constants.Metrics.Tag.INSTANCE_ID),
      // i.e. for procedure only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.PROCEDURE)));

    // workflow
    aggs.add(new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.WORKFLOW, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID),
      // i.e. for workflow only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.WORKFLOW)));

    // spark
    aggs.add(new DefaultAggregation(
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.SPARK, Constants.Metrics.Tag.DATASET,
                       Constants.Metrics.Tag.RUN_ID),
      // i.e. for spark only
      ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                       Constants.Metrics.Tag.SPARK)));

    // Streams:
    aggs.add(new DefaultAggregation(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.STREAM),
                                    // i.e. for streams only
                                    ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.STREAM)));

    // Datasets:
    aggs.add(new DefaultAggregation(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.DATASET),
                                    // i.e. for datasets only
                                    ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.DATASET)));

    // System components:
    aggs.add(new DefaultAggregation(ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.COMPONENT,
      Constants.Metrics.Tag.HANDLER, Constants.Metrics.Tag.METHOD),
                                    // i.e. for components only
                                    ImmutableList.of(
                                      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.COMPONENT)));


    return aggs;
  }

  @Override
  public void add(MetricValue metricValue) throws Exception {
    add(ImmutableList.of(metricValue));
  }

  @Override
  public void add(Collection<? extends MetricValue> metricValues) throws Exception {
    List<CubeFact> facts = Lists.newArrayListWithCapacity(metricValues.size());
    for (MetricValue metricValue : metricValues) {
      String scope = metricValue.getTags().get(Constants.Metrics.Tag.SCOPE);
      String measureName = (scope == null ? "system." : scope + ".") + metricValue.getName();

      CubeFact fact = new CubeFact(metricValue.getTags(),
                                   toMeasureType(metricValue.getType()), measureName,
                                   new TimeValue(metricValue.getTimestamp(), metricValue.getValue()));
      facts.add(fact);
    }
    cube.get().add(facts);
  }

  @Override
  public Collection<MetricTimeSeries> query(MetricDataQuery q) throws Exception {
    Collection<TimeSeries> cubeResult = cube.get().query(buildCubeQuery(q));
    List<MetricTimeSeries> result = Lists.newArrayList();
    for (TimeSeries timeSeries : cubeResult) {
      result.add(new MetricTimeSeries(timeSeries.getMeasureName(),
                                      timeSeries.getTagValues(),
                                      timeSeries.getTimeValues()));
    }
    return result;
  }

  private CubeQuery buildCubeQuery(MetricDataQuery q) {
    return new CubeQuery(q.getStartTs(), q.getEndTs(), q.getResolution(), q.getMetricName(),
                         toMeasureType(q.getMetricType()), q.getSliceByTags(), q.getGroupByTags());
  }

  @Override
  public void deleteBefore(long timestamp) throws Exception {
    // Delete all data before the timestamp. null for MeasureName indicates match any MeasureName.
    for (int resolution : resolutions) {
      // NOTE: we do not purge on TTL the "totals" currently, as there might be system components dependent on it
      if (TOTALS_RESOLUTION == resolution) {
        continue;
      }
      CubeDeleteQuery query = new CubeDeleteQuery(0, timestamp, resolution, null, Maps.<String, String>newHashMap());
      cube.get().delete(query);
    }
  }

  @Override
  public void delete(MetricDeleteQuery query) throws Exception {
    cube.get().delete(buildCubeDeleteQuery(query));
  }

  @Override
  public void deleteAll() throws Exception {
    // this will delete all aggregates metrics data
    delete(new MetricDeleteQuery(0, System.currentTimeMillis() / 1000, null,
                                             Maps.<String, String>newHashMap()));
    // this will delete all timeseries data
    deleteBefore(System.currentTimeMillis() / 1000);
  }

  private CubeDeleteQuery buildCubeDeleteQuery(MetricDeleteQuery query) {
    // note: delete query currently usually executed synchronously,
    //       so we only attempt to delete totals, to avoid timeout
    return new CubeDeleteQuery(query.getStartTs(), query.getEndTs(), TOTALS_RESOLUTION,
                               query.getMetricName(), query.getSliceByTags());
  }

  @Override
  public Collection<TagValue> findNextAvailableTags(MetricSearchQuery query) throws Exception {
    return cube.get().findNextAvailableTags(buildCubeSearchQuery(query));
  }

  private CubeExploreQuery buildCubeSearchQuery(MetricSearchQuery query) {
    return new CubeExploreQuery(query.getStartTs(), query.getEndTs(), query.getResolution(),
                                query.getLimit(), query.getTagValues());
  }

  @Override
  public Collection<String> findMetricNames(MetricSearchQuery query) throws Exception {
    return cube.get().findMeasureNames(buildCubeSearchQuery(query));
  }

  private MeasureType toMeasureType(MetricType type) {
    switch (type) {
      case COUNTER:
        return MeasureType.COUNTER;
      case GAUGE:
        return MeasureType.GAUGE;
      default:
        // should never happen
        throw new IllegalArgumentException("Unknown MetricType: " + type);
    }
  }
}
