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
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Admin for non-transactional dataset that uses plain HBase table.
 */
public class HTableDatasetAdmin implements DatasetAdmin {
  protected final byte[] tableName;
  protected final Configuration hConf;
  protected final HBaseTableUtil tableUtil;
  protected final HTableDescriptor tableDescriptor;

  private HBaseAdmin admin;

  public HTableDatasetAdmin(HTableDescriptor tableDescriptor, Configuration hConf, HBaseTableUtil tableUtil) {
    this.tableName = tableDescriptor.getName();
    this.hConf = hConf;
    this.tableUtil = tableUtil;
    this.tableDescriptor = tableDescriptor;
  }

  private HBaseAdmin getAdmin() throws IOException {
    if (admin == null) {
      admin = new HBaseAdmin(hConf);
    }
    return admin;
  }

  @Override
  public void upgrade() throws IOException {
    // do nothing
  }

  @Override
  public boolean exists() throws IOException {
    return getAdmin().tableExists(tableName);
  }

  @Override
  public void create() throws IOException {
    tableUtil.createTableIfNotExists(getAdmin(), tableName, tableDescriptor, null);
  }

  @Override
  public void truncate() throws IOException {
    HTableDescriptor tableDescriptor = getAdmin().getTableDescriptor(tableName);
    getAdmin().disableTable(tableName);
    getAdmin().deleteTable(tableName);
    getAdmin().createTable(tableDescriptor);
  }

  @Override
  public void drop() throws IOException {
    getAdmin().disableTable(tableName);
    getAdmin().deleteTable(tableName);
  }

  @Override
  public void close() throws IOException {
    if (admin != null) {
      admin.close();
    }
  }
}
