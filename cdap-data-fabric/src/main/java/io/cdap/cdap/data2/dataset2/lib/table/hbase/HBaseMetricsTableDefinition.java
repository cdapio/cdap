/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.IncompatibleUpdateException;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LogSampler;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.data2.dataset2.lib.table.AbstractTableDefinition;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTable;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HBase based implementation for {@link MetricsTable}.
 */
// todo: re-use HBase table based dataset instead of having separate classes hierarchies, see CDAP-1193
public class HBaseMetricsTableDefinition extends AbstractTableDefinition<MetricsTable, DatasetAdmin> {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseMetricsTableDefinition.class);
  private static final Gson GSON = new Gson();

  // A logger for logging the incompatible config change for number of splits.
  // We only log it once per message (hence per table).
  private static final Logger CONFIG_CHANGE_LOG = Loggers.sampling(LOG, LogSamplers.perMessage(() -> new LogSampler() {
    private final AtomicBoolean logged = new AtomicBoolean(false);

    @Override
    public boolean accept(String message, int logLevel) {
      return logged.compareAndSet(false, true);
    }
  }));

  @Inject
  private Configuration hConf;
  @Inject
  private Provider<HBaseTableUtil> hBaseTableUtilProvider;
  @Inject
  private LocationFactory locationFactory;

  private volatile HBaseTableUtil hBaseTableUtil;

  @Inject
  public HBaseMetricsTableDefinition(@Named(Constants.Dataset.TABLE_TYPE_NO_TX) String name) {
    super(name);
  }

  @VisibleForTesting
  HBaseMetricsTableDefinition(String name, Configuration hConf, Provider<HBaseTableUtil> hBaseTableUtilProvider,
                              LocationFactory locationFactory, CConfiguration cConf) {
    super(name);
    this.hConf = hConf;
    this.hBaseTableUtilProvider = hBaseTableUtilProvider;
    this.locationFactory = locationFactory;
    this.cConf = cConf;
  }

  @Override
  public DatasetSpecification configure(String name, DatasetProperties properties) {
    TableProperties.Builder props = TableProperties.builder();
    props.addAll(properties.getProperties());

    // Extra properties for HBase table
    // for efficient counters
    props.setReadlessIncrementSupport(true);

    // Disable auto split
    props.add(HBaseTableAdmin.SPLIT_POLICY,
              cConf.get(Constants.Metrics.METRICS_TABLE_HBASE_SPLIT_POLICY));
    // configuring pre-splits
    props.add(HBaseTableAdmin.PROPERTY_SPLITS,
              GSON.toJson(getMetricsTableSplits(cConf.getInt(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS))));
    props.add(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS,
              cConf.getInt(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS));

    return DatasetSpecification.builder(name, getName())
      .properties(props.build().getProperties())
      .property(Constants.Dataset.TABLE_TX_DISABLED, "true")
      .build();
  }

  @Override
  public MetricsTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                 Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    int datasetSplits = spec.getIntProperty(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS, 16);

    // Detect if there is a cdap-site change on the number of splits, which we don't support.
    // Log a warning if that's the case.
    if (cConf.getInt(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS) != datasetSplits) {
      CONFIG_CHANGE_LOG.warn(
        "Ignoring configuration {} with value {} from cdap-site.xml. " +
          "The system table {} already has a splits value {}, which can not be changed.",
        Constants.Metrics.METRICS_HBASE_TABLE_SPLITS, cConf.getInt(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS),
        spec.getName(), datasetSplits);
    }

    return new HBaseMetricsTable(datasetContext, spec, hConf, getHBaseTableUtil(), cConf);
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return new HBaseTableAdmin(datasetContext, spec, hConf, getHBaseTableUtil(), cConf, locationFactory);
  }

  @Override
  public DatasetSpecification reconfigure(String instanceName, DatasetProperties properties,
                                          DatasetSpecification currentSpec) throws IncompatibleUpdateException {
    throw new IncompatibleUpdateException("System Metrics Table properties can not be changed after creation.");
  }

  private static byte[][] getMetricsTableSplits(int splits) {
    RowKeyDistributorByHashPrefix rowKeyDistributor = new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(splits));
    return rowKeyDistributor.getSplitKeys(splits, splits);
  }

  /**
   * Returns the {@link HBaseTableUtil} to use.
   */
  private HBaseTableUtil getHBaseTableUtil() {
    HBaseTableUtil tableUtil = hBaseTableUtil;
    if (tableUtil != null) {
      return tableUtil;
    }

    synchronized (this) {
      tableUtil = hBaseTableUtil;
      if (tableUtil != null) {
        return tableUtil;
      }
      hBaseTableUtil = tableUtil = hBaseTableUtilProvider.get();
      return tableUtil;
    }
  }
}
