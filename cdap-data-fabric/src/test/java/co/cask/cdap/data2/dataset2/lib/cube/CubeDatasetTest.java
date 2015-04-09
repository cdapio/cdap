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

package co.cask.cdap.data2.dataset2.lib.cube;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeDeleteQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeExploreQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.TagValue;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import com.google.common.base.Joiner;
import org.junit.ClassRule;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 *
 */
public class CubeDatasetTest extends AbstractCubeTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @Override
  protected Cube getCube(String name, int[] resolutions,
                         Collection<? extends Aggregation> aggregations) throws Exception {
    DatasetProperties props = configureProperties(resolutions, aggregations);
    Id.DatasetInstance id = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, name);
    dsFrameworkUtil.createInstance(Cube.class.getName(), id, props);
    Dataset cube = dsFrameworkUtil.getInstance(id);
    return new CubeTxnlWrapper((Cube) cube);
  }

  private DatasetProperties configureProperties(int[] resolutions, Collection<? extends Aggregation> aggregations) {
    DatasetProperties.Builder builder = DatasetProperties.builder();

    // add resolution property
    StringBuilder resolutionPropValue = new StringBuilder();
    for (int resolution : resolutions) {
      resolutionPropValue.append(",").append(resolution);
    }
    // .substring(1) for removing first comma
    builder.add(CubeDatasetDefinition.PROPERTY_RESOLUTIONS, resolutionPropValue.substring(1));

    // add aggregation props
    int aggName = 0;
    for (Aggregation agg : aggregations) {
      // NOTE: at this moment we support only DefaultAggregation, so all other tests in AbstractCubeTest must be skipped
      DefaultAggregation defAgg = (DefaultAggregation) agg;
      String aggPropertyPrefix = CubeDatasetDefinition.PROPERTY_AGGREGATION_PREFIX + (aggName++);
      builder.add(aggPropertyPrefix + ".tags", Joiner.on(",").join(defAgg.getTagNames()));
      builder.add(aggPropertyPrefix + ".requiredTags", Joiner.on(",").join(defAgg.getRequiredTags()));
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
    public void add(final CubeFact fact) throws Exception {
      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          delegate.add(fact);
        }
      });
    }

    @Override
    public void add(final Collection<? extends CubeFact> facts) throws Exception {
      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          delegate.add(facts);
        }
      });
    }

    @Override
    public Collection<TimeSeries> query(final CubeQuery query) throws Exception {
      return txnl.execute(new Callable<Collection<TimeSeries>>() {
        @Override
        public Collection<TimeSeries> call() throws Exception {
          return delegate.query(query);
        }
      });
    }

    @Override
    public void delete(final CubeDeleteQuery query) throws Exception {
      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          delegate.delete(query);
        }
      });
    }

    @Override
    public Collection<TagValue> findNextAvailableTags(final CubeExploreQuery query) throws Exception {
      return txnl.execute(new Callable<Collection<TagValue>>() {
        @Override
        public Collection<TagValue> call() throws Exception {
          return delegate.findNextAvailableTags(query);
        }
      });
    }

    @Override
    public Collection<String> findMeasureNames(final CubeExploreQuery query) throws Exception {
      return txnl.execute(new Callable<Collection<String>>() {
        @Override
        public Collection<String> call() throws Exception {
          return delegate.findMeasureNames(query);
        }
      });
    }
  }
}
