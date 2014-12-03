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
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.tephra.TxConstants;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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
    // todo: CDAP-1458 HTableDatasetAdmin doesn't fit where there are coprocessors set for table, as it does no upgrade
    return new HTableDatasetAdmin(getHTableDescriptor(spec), hConf, hBaseTableUtil);
  }

  private HTableDescriptor getHTableDescriptor(DatasetSpecification spec) throws IOException {
    final String tableName = HBaseTableUtil.getHBaseTableName(spec.getName());

    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(HBaseMetricsTable.DATA_COLUMN_FAMILY);
    hBaseTableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);
    // to support read-less increments, we need to allow storing many versions: each increment is a put to the same cell
    columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
    // to make sure delta-increments get compacted on flush and major/minor compaction and redundant versions are
    // cleaned up. See IncrementHandler.CompactionBound.
    columnDescriptor.setValue("dataset.table.readless.increment.transactional", "false");

    long ttlMillis = spec.getLongProperty(Table.PROPERTY_TTL, -1);
    if (ttlMillis > 0) {
      // the IncrementHandler coprocessor reads this value and performs TTL
      columnDescriptor.setValue(TxConstants.PROPERTY_TTL, Long.toString(ttlMillis));
    }

    final HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    tableDescriptor.addFamily(columnDescriptor);
    AbstractHBaseDataSetAdmin.CoprocessorJar cpJar =
      HBaseOrderedTableAdmin.createCoprocessorJarInternal(conf, locationFactory, hBaseTableUtil, true);
    tableDescriptor.addCoprocessor(hBaseTableUtil.getIncrementHandlerClassForVersion().getName(),
                                   new Path(cpJar.getJarLocation().toURI()), Coprocessor.PRIORITY_USER, null);
    return tableDescriptor;
  }
}
