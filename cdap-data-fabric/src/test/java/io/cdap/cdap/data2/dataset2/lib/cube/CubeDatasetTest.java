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

package io.cdap.cdap.data2.dataset2.lib.cube;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.Cube;
import io.cdap.cdap.api.dataset.lib.cube.CubeDeleteQuery;
import io.cdap.cdap.api.dataset.lib.cube.CubeExploreQuery;
import io.cdap.cdap.api.dataset.lib.cube.CubeFact;
import io.cdap.cdap.api.dataset.lib.cube.CubeQuery;
import io.cdap.cdap.api.dataset.lib.cube.DimensionValue;
import io.cdap.cdap.api.dataset.lib.cube.TimeSeries;
import io.cdap.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import io.cdap.cdap.proto.id.DatasetId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 *
 */
public class CubeDatasetTest extends AbstractCubeTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @Override
  protected Cube getCube(String name, int[] resolutions,
                         Map<String, ? extends Aggregation> aggregations) throws Exception {
    return new CubeTxnlWrapper(getCubeInternal(name, resolutions, aggregations));
  }

  @Override
  protected Cube getCube(String name, int[] resolutions, Map<String, ? extends Aggregation> aggregations,
                         int coarseLagFactor, int coarseRoundFactor) throws Exception {
    return new CubeTxnlWrapper(getCubeInternal(name, resolutions, aggregations, coarseLagFactor, coarseRoundFactor));
  }

  private Cube getCubeInternal(String name, int[] resolutions,
                               Map<String, ? extends Aggregation> aggregations
  ) throws Exception {
    return getCubeInternal(name, resolutions, aggregations, null, null);
  }

  private Cube getCubeInternal(String name, int[] resolutions,
                               Map<String, ? extends Aggregation> aggregations,
                               Integer coarseLagFactor,
                               Integer coarseRoundFactor
                               ) throws Exception {
    DatasetProperties props = configureProperties(resolutions, aggregations, coarseLagFactor, coarseRoundFactor);
    DatasetId id = DatasetFrameworkTestUtil.NAMESPACE_ID.dataset(name);
    if (dsFrameworkUtil.getInstance(id) == null) {
      dsFrameworkUtil.createInstance(Cube.class.getName(), id, props);
    }
    return dsFrameworkUtil.getInstance(id);
  }

  @Test
  public void testTxRetryOnFailure() throws Exception {
    // This test ensures that there's no non-transactional cache used in cube dataset. For that, it
    // 1) simulates transaction conflict for the first write to cube
    // 2) attempts to write again, writes successfully
    // 3) uses second cube instance to read the result
    //
    // In case there's a non-transactional cache used in cube, it would fill entity mappings in the first tx, and only
    // use them to write data. Hence, when reading - there will be no mapping in entity table to decode, as first tx
    // that wrote it is not visible (was aborted on conflict).

    Aggregation agg1 = new DefaultAggregation(ImmutableList.of("dim1", "dim2", "dim3"));
    int resolution = 1;
    Cube cube1 = getCubeInternal("concurrCube", new int[]{resolution}, ImmutableMap.of("agg1", agg1));
    Cube cube2 = getCubeInternal("concurrCube", new int[]{resolution}, ImmutableMap.of("agg1", agg1));

    Configuration txConf = HBaseConfiguration.create();
    TransactionManager txManager = new TransactionManager(txConf);
    txManager.startAndWait();
    try {
      TransactionSystemClient txClient = new InMemoryTxSystemClient(txManager);

      // 1) write and abort after commit to simlate conflict
      Transaction tx = txClient.startShort();
      ((TransactionAware) cube1).startTx(tx);

      writeInc(cube1, "metric1", 1, 1, "1", "1", "1");

      ((TransactionAware) cube1).commitTx();
      txClient.abort(tx);
      ((TransactionAware) cube1).rollbackTx();

      // 2) write successfully
      tx = txClient.startShort();
      ((TransactionAware) cube1).startTx(tx);

      writeInc(cube1, "metric1", 1, 1, "1", "1", "1");

      // let's pretend we had conflict and rollback it
      ((TransactionAware) cube1).commitTx();
      txClient.commitOrThrow(tx);
      ((TransactionAware) cube1).postTxCommit();

      // 3) read using different cube instance
      tx = txClient.startShort();
      ((TransactionAware) cube2).startTx(tx);

      verifyCountQuery(cube2, 0, 2, resolution, "metric1", AggregationFunction.SUM,
                       new HashMap<String, String>(), new ArrayList<String>(),
                       ImmutableList.of(
                         new TimeSeries("metric1", new HashMap<String, String>(), timeValues(1, 1))));

      // let's pretend we had conflict and rollback it
      ((TransactionAware) cube2).commitTx();
      txClient.commitOrThrow(tx);
      ((TransactionAware) cube2).postTxCommit();
    } finally {
      txManager.stopAndWait();
    }
  }

  private DatasetProperties configureProperties(int[] resolutions, Map<String, ? extends Aggregation> aggregations,
                                                Integer coarseLagFactor, Integer coarseRoundFactor) {
    DatasetProperties.Builder builder = DatasetProperties.builder();

    // add resolution property
    StringBuilder resolutionPropValue = new StringBuilder();
    for (int resolution : resolutions) {
      resolutionPropValue.append(",").append(resolution);
    }
    // .substring(1) for removing first comma
    builder.add(Cube.PROPERTY_RESOLUTIONS, resolutionPropValue.substring(1));

    // add aggregation props
    for (Map.Entry<String, ? extends Aggregation> entry : aggregations.entrySet()) {
      // NOTE: at this moment we support only DefaultAggregation, so all other tests in AbstractCubeTest must be skipped
      DefaultAggregation defAgg = (DefaultAggregation) entry.getValue();
      String aggPropertyPrefix = CubeDatasetDefinition.PROPERTY_AGGREGATION_PREFIX + (entry.getKey());
      if (!defAgg.getDimensionNames().isEmpty()) {
        builder.add(aggPropertyPrefix + ".dimensions", Joiner.on(",").join(defAgg.getDimensionNames()));
      }
      if (!defAgg.getRequiredDimensions().isEmpty()) {
        builder.add(aggPropertyPrefix + ".requiredDimensions", Joiner.on(",").join(defAgg.getRequiredDimensions()));
      }
    }

    if (coarseLagFactor != null) {
      builder.add(CubeDatasetDefinition.PROPERTY_COARSE_LAG_FACTOR, coarseLagFactor);
    }

    if (coarseRoundFactor != null) {
      builder.add(CubeDatasetDefinition.PROPERTY_COARSE_ROUND_FACTOR, coarseRoundFactor);
    }

    return builder.build();
  }

  private static final class CubeTxnlWrapper implements Cube {
    private final Cube delegate;
    private final TransactionExecutor txnl;

    private CubeTxnlWrapper(Cube delegate) {
      this.delegate = delegate;
      this.txnl = dsFrameworkUtil.newTransactionExecutor((TransactionAware) delegate);
    }

    @Override
    public void add(final CubeFact fact) {
      txnl.executeUnchecked(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          delegate.add(fact);
        }
      });
    }

    @Override
    public void add(final Collection<? extends CubeFact> facts) {
      txnl.executeUnchecked(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          delegate.add(facts);
        }
      });
    }

    @Override
    public Collection<TimeSeries> query(final CubeQuery query) {
      return txnl.executeUnchecked(new Callable<Collection<TimeSeries>>() {
        @Override
        public Collection<TimeSeries> call() {
          return delegate.query(query);
        }
      });
    }

    @Override
    public void delete(final CubeDeleteQuery query) {
      txnl.executeUnchecked(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          delegate.delete(query);
        }
      });
    }

    @Override
    public Collection<DimensionValue> findDimensionValues(final CubeExploreQuery query) {
      return txnl.executeUnchecked(new Callable<Collection<DimensionValue>>() {
        @Override
        public Collection<DimensionValue> call() {
          return delegate.findDimensionValues(query);
        }
      });
    }

    @Override
    public Collection<String> findMeasureNames(final CubeExploreQuery query) {
      return txnl.executeUnchecked(new Callable<Collection<String>>() {
        @Override
        public Collection<String> call() {
          return delegate.findMeasureNames(query);
        }
      });
    }

    @Override
    public void write(Object ignored, CubeFact cubeFact) {
      add(cubeFact);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
