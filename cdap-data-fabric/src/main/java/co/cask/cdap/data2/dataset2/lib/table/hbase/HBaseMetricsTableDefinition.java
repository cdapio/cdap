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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * HBase based implementation for {@link MetricsTable}.
 */
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

  @Override
  public DatasetSpecification configure(String name, DatasetProperties properties) {
    return DatasetSpecification.builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public MetricsTable getDataset(DatasetSpecification spec, Map<String, String> arguments, ClassLoader classLoader)
    throws IOException {
    return new HBaseMetricsTable(spec.getName(), hConf);
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new HTableDatasetAdmin(getHTableDescriptor(spec), hConf, hBaseTableUtil);
  }

  private HTableDescriptor getHTableDescriptor(DatasetSpecification spec) {
    final String tableName = HBaseTableUtil.getHBaseTableName(spec.getName());

    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(HBaseMetricsTable.DATA_COLUMN_FAMILY);
    hBaseTableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);
    columnDescriptor.setMaxVersions(1);

    long ttlMillis = spec.getLongProperty(OrderedTable.PROPERTY_TTL, -1);
    if (ttlMillis > 0) {
      columnDescriptor.setTimeToLive((int) TimeUnit.MILLISECONDS.toSeconds(ttlMillis));
    }

    final HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    tableDescriptor.addFamily(columnDescriptor);
    return tableDescriptor;
  }
}
