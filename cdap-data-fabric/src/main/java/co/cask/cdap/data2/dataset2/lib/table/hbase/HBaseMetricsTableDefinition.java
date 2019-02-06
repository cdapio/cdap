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

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSampler;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.data2.dataset2.lib.table.AbstractTableDefinition;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Provider;
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

  public HBaseMetricsTableDefinition(String name) {
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
    return DatasetSpecification.builder(name, getName())
      .properties(properties.getProperties())
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
