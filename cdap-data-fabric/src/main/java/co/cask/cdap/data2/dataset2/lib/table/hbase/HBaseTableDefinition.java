/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.AbstractTableDefinition;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Defines the HBase implementation of Table.
 */
public class HBaseTableDefinition extends AbstractTableDefinition<Table, HBaseTableAdmin> {

  @Inject
  private Configuration hConf;
  @Inject
  private HBaseTableUtil hBaseTableUtil;
  @Inject
  private LocationFactory locationFactory;

  public HBaseTableDefinition(String name) {
    super(name);
  }

  @Override
  public Table getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                          Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    return new HBaseTable(datasetContext, spec, cConf, hConf, hBaseTableUtil);
  }

  @Override
  public HBaseTableAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                                  ClassLoader classLoader) throws IOException {
    return new HBaseTableAdmin(datasetContext, spec, hConf, hBaseTableUtil, cConf, locationFactory);
  }
}
