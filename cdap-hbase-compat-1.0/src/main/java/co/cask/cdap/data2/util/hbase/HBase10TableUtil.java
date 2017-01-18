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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.data2.increment.hbase10.IncrementHandler;
import co.cask.cdap.data2.transaction.coprocessor.hbase10.DefaultTransactionProcessor;
import co.cask.cdap.data2.transaction.messaging.coprocessor.hbase10.MessageTableRegionObserver;
import co.cask.cdap.data2.transaction.messaging.coprocessor.hbase10.PayloadTableRegionObserver;
import co.cask.cdap.data2.transaction.queue.coprocessor.hbase10.DequeueScanObserver;
import co.cask.cdap.data2.transaction.queue.coprocessor.hbase10.HBaseQueueRegionObserver;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.hbase.ddl.DefaultHBase10DDLExecutor;
import co.cask.cdap.hbase.ddl.HBaseDDLExecutor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class HBase10TableUtil extends HBaseTableUtil {

  private final HTableNameConverter nameConverter = new HTableNameConverter();

  @Override
  public HBaseDDLExecutor getHBaseDDLExecutor(Configuration hConf) {
    return new DefaultHBase10DDLExecutor(hConf);
  }

  @Override
  public HTable createHTable(Configuration conf, TableId tableId) throws IOException {
    Preconditions.checkArgument(tableId != null, "Table id should not be null");
    return new HTable(conf, nameConverter.toTableName(tablePrefix, tableId));
  }

  @Override
  public HTableDescriptorBuilder buildHTableDescriptor(TableId tableId) {
    Preconditions.checkArgument(tableId != null, "Table id should not be null");
    return new HBase10HTableDescriptorBuilder(nameConverter.toTableName(tablePrefix, tableId));
  }

  @Override
  public HTableDescriptorBuilder buildHTableDescriptor(HTableDescriptor descriptorToCopy) {
    Preconditions.checkArgument(descriptorToCopy != null, "Table descriptor should not be null");
    return new HBase10HTableDescriptorBuilder(descriptorToCopy);
  }

  @Override
  public HTableDescriptor getHTableDescriptor(HBaseAdmin admin, TableId tableId) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    return admin.getTableDescriptor(nameConverter.toTableName(tablePrefix, tableId));
  }

  @Override
  public boolean hasNamespace(HBaseAdmin admin, String namespace) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(namespace != null, "Namespace should not be null.");
    try {
      admin.getNamespaceDescriptor(nameConverter.encodeHBaseEntity(namespace));
      return true;
    } catch (NamespaceNotFoundException e) {
      return false;
    }
  }

  @Override
  public List<HRegionInfo> getTableRegions(HBaseAdmin admin, TableId tableId) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    return admin.getTableRegions(nameConverter.toTableName(tablePrefix, tableId));
  }

  @Override
  public List<TableId> listTablesInNamespace(HBaseAdmin admin, String namespaceId) throws IOException {
    List<TableId> tableIds = Lists.newArrayList();
    HTableDescriptor[] hTableDescriptors =
      admin.listTableDescriptorsByNamespace(nameConverter.encodeHBaseEntity(namespaceId));
    for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
      if (isCDAPTable(hTableDescriptor)) {
        tableIds.add(nameConverter.from(hTableDescriptor));
      }
    }
    return tableIds;
  }

  @Override
  public List<TableId> listTables(HBaseAdmin admin) throws IOException {
    List<TableId> tableIds = Lists.newArrayList();
    HTableDescriptor[] hTableDescriptors = admin.listTables();
    for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
      if (isCDAPTable(hTableDescriptor)) {
        tableIds.add(nameConverter.from(hTableDescriptor));
      }
    }
    return tableIds;
  }

  @Override
  public Class<? extends Coprocessor> getTransactionDataJanitorClassForVersion() {
    return DefaultTransactionProcessor.class;
  }

  @Override
  public Class<? extends Coprocessor> getQueueRegionObserverClassForVersion() {
    return HBaseQueueRegionObserver.class;
  }

  @Override
  public Class<? extends Coprocessor> getDequeueScanObserverClassForVersion() {
    return DequeueScanObserver.class;
  }

  @Override
  public Class<? extends Coprocessor> getIncrementHandlerClassForVersion() {
    return IncrementHandler.class;
  }

  @Override
  public Class<? extends Coprocessor> getMessageTableRegionObserverClassForVersion() {
    return MessageTableRegionObserver.class;
  }

  @Override
  public Class<? extends Coprocessor> getPayloadTableRegionObserverClassForVersion() {
    return PayloadTableRegionObserver.class;
  }

  @Override
  protected HTableNameConverter getHTableNameConverter() {
    return nameConverter;
  }

  @Override
  public ScanBuilder buildScan() {
    return new HBase10ScanBuilder();
  }

  @Override
  public ScanBuilder buildScan(Scan scan) throws IOException {
    return new HBase10ScanBuilder(scan);
  }

  @Override
  public IncrementBuilder buildIncrement(byte[] row) {
    return new HBase10IncrementBuilder(row);
  }

  @Override
  public PutBuilder buildPut(byte[] row) {
    return new HBase10PutBuilder(row);
  }

  @Override
  public PutBuilder buildPut(Put put) {
    return new HBase10PutBuilder(put);
  }

  @Override
  public GetBuilder buildGet(byte[] row) {
    return new HBase10GetBuilder(row);
  }

  @Override
  public GetBuilder buildGet(Get get) {
    return new HBase10GetBuilder(get);
  }

  @Override
  public DeleteBuilder buildDelete(byte[] row) {
    return new HBase10DeleteBuilder(row);
  }

  @Override
  public DeleteBuilder buildDelete(Delete delete) {
    return new HBase10DeleteBuilder(delete);
  }
}
