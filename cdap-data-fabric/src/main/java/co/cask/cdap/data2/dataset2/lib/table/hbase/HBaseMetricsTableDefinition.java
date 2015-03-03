/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Map;

/**
 * HBase based implementation for {@link MetricsTable}.
 */
// todo: re-use HBase table based dataset instead of having separate classes hierarchies, see CDAP-1193
public class HBaseMetricsTableDefinition extends AbstractDatasetDefinition<MetricsTable, DatasetAdmin> {
  @Inject
  private Configuration hConf;
  @Inject
  private HBaseTableUtil hBaseTableUtil;
  @Inject
  private LocationFactory locationFactory;
  @Inject
  private CConfiguration conf;

  public HBaseMetricsTableDefinition(String name) {
    super(name);
  }

  // for unit-test purposes only
  HBaseMetricsTableDefinition(String name, Configuration hConf, HBaseTableUtil hBaseTableUtil,
                                     LocationFactory locationFactory, CConfiguration conf) {
    super(name);
    this.hConf = hConf;
    this.hBaseTableUtil = hBaseTableUtil;
    this.locationFactory = locationFactory;
    this.conf = conf;
  }

  @Override
  public DatasetSpecification configure(String name, DatasetProperties properties) {
    return DatasetSpecification.builder(name, getName())
      .property(Constants.Dataset.TABLE_TX_DISABLED, "true")
      .properties(properties.getProperties())
      .build();
  }


  @Override
  public MetricsTable getDataset(DatasetSpecification spec, Map<String, String> arguments, ClassLoader classLoader)
    throws IOException {
    return new HBaseMetricsTable(spec.getName(), hConf);
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, ClassLoader classLoader,
                               DatasetSpecification spec) throws IOException {
    return new HBaseTableAdmin(spec, hConf, hBaseTableUtil, conf, locationFactory);
  }
}
