/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.List;

/**
 * A class that allows injecting testing code into HBaseTableUtil.
 */
class DelegatingHBaseTableUtil extends HBaseTableUtil {
  private final HBaseTableUtil delegate;

  DelegatingHBaseTableUtil(HBaseTableUtil delegate) {
    this.delegate = delegate;
  }

  @Override
  public HTable createHTable(Configuration conf, TableId tableId) throws IOException {
    return delegate.createHTable(conf, tableId);
  }

  @Override
  public TableId createHTableId(NamespaceId namespace, String tableName) throws IOException {
    return delegate.createHTableId(namespace, tableName);
  }

  @Override
  public HTableDescriptorBuilder buildHTableDescriptor(TableId tableId) {
    return delegate.buildHTableDescriptor(tableId);
  }

  @Override
  public HTableDescriptorBuilder buildHTableDescriptor(HTableDescriptor tableDescriptor) {
    return delegate.buildHTableDescriptor(tableDescriptor);
  }

  @Override
  public HTableDescriptor getHTableDescriptor(HBaseAdmin admin, TableId tableId) throws IOException {
    return delegate.getHTableDescriptor(admin, tableId);
  }

  @Override
  public boolean hasNamespace(HBaseAdmin admin, String namespace) throws IOException {
    return delegate.hasNamespace(admin, namespace);
  }

  @Override
  public boolean tableExists(HBaseAdmin admin, TableId tableId) throws IOException {
    return delegate.tableExists(admin, tableId);
  }

  @Override
  public void deleteTable(HBaseDDLExecutor ddlExecutor, TableId tableId) throws IOException {
    delegate.deleteTable(ddlExecutor, tableId);
  }

  @Override
  public void modifyTable(HBaseDDLExecutor ddlExecutor, HTableDescriptor tableDescriptor) throws IOException {
    delegate.modifyTable(ddlExecutor, tableDescriptor);
  }

  @Override
  public List<HRegionInfo> getTableRegions(HBaseAdmin admin, TableId tableId) throws IOException {
    return delegate.getTableRegions(admin, tableId);
  }

  @Override
  public List<TableId> listTablesInNamespace(HBaseAdmin admin, String namespaceId) throws IOException {
    return delegate.listTablesInNamespace(admin, namespaceId);
  }

  @Override
  public List<TableId> listTables(HBaseAdmin admin) throws IOException {
    return delegate.listTables(admin);
  }

  @Override
  public void setCompression(HColumnDescriptor columnDescriptor, CompressionType type) {
    delegate.setCompression(columnDescriptor, type);
  }

  @Override
  public void setBloomFilter(HColumnDescriptor columnDescriptor, BloomType type) {
    delegate.setBloomFilter(columnDescriptor, type);
  }

  @Override
  public CompressionType getCompression(HColumnDescriptor columnDescriptor) {
    return delegate.getCompression(columnDescriptor);
  }

  @Override
  public BloomType getBloomFilter(HColumnDescriptor columnDescriptor) {
    return delegate.getBloomFilter(columnDescriptor);
  }

  @Override
  public boolean isGlobalAdmin(Configuration hConf) throws IOException {
    return delegate.isGlobalAdmin(hConf);
  }

  @Override
  public Class<? extends Coprocessor> getTransactionDataJanitorClassForVersion() {
    return delegate.getTransactionDataJanitorClassForVersion();
  }

  @Override
  public Class<? extends Coprocessor> getQueueRegionObserverClassForVersion() {
    return delegate.getQueueRegionObserverClassForVersion();
  }

  @Override
  public Class<? extends Coprocessor> getDequeueScanObserverClassForVersion() {
    return delegate.getDequeueScanObserverClassForVersion();
  }

  @Override
  public Class<? extends Coprocessor> getIncrementHandlerClassForVersion() {
    return delegate.getIncrementHandlerClassForVersion();
  }

  @Override
  public Class<? extends Coprocessor> getMessageTableRegionObserverClassForVersion() {
    return delegate.getMessageTableRegionObserverClassForVersion();
  }

  @Override
  public Class<? extends Coprocessor> getPayloadTableRegionObserverClassForVersion() {
    return delegate.getPayloadTableRegionObserverClassForVersion();
  }
}
