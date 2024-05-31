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

package io.cdap.cdap.data2.util.hbase;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.cdap.cdap.data2.increment.hbase10cdh.IncrementHandler;
import io.cdap.cdap.data2.transaction.coprocessor.hbase10cdh.DefaultTransactionProcessor;
import io.cdap.cdap.data2.transaction.messaging.coprocessor.hbase10cdh.MessageTableRegionObserver;
import io.cdap.cdap.data2.transaction.messaging.coprocessor.hbase10cdh.PayloadTableRegionObserver;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import io.cdap.cdap.spi.hbase.TableDescriptor;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HBase10CDHTableUtil extends HBaseTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HBase10CDHTableUtil.class);

  @Override
  public HTableDescriptorBuilder buildHTableDescriptor(TableId tableId) {
    Preconditions.checkArgument(tableId != null, "Table id should not be null");
    return new HBase10CDHHTableDescriptorBuilder(
        HTableNameConverter.toTableName(tablePrefix, tableId));
  }

  @Override
  public HTableDescriptorBuilder buildHTableDescriptor(HTableDescriptor tableDescriptorToCopy) {
    Preconditions.checkArgument(tableDescriptorToCopy != null,
        "Table descriptor should not be null");
    return new HBase10CDHHTableDescriptorBuilder(tableDescriptorToCopy);
  }

  @Override
  public HTableDescriptor getHTableDescriptor(HBaseAdmin admin, TableId tableId)
      throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    return admin.getTableDescriptor(HTableNameConverter.toTableName(tablePrefix, tableId));
  }

  @Override
  public boolean hasNamespace(HBaseAdmin admin, String namespace) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(namespace != null, "Namespace should not be null.");
    try {
      admin.getNamespaceDescriptor(HTableNameConverter.encodeHBaseEntity(namespace));
      return true;
    } catch (NamespaceNotFoundException e) {
      return false;
    }
  }

  @Override
  public boolean tableExists(HBaseAdmin admin, TableId tableId) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    return admin.tableExists(HTableNameConverter.toTableName(tablePrefix, tableId));
  }

  @Override
  public void deleteTable(HBaseDDLExecutor ddlExecutor, TableId tableId) throws IOException {
    Preconditions.checkArgument(ddlExecutor != null, "HBaseDDLExecutor should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    TableName tableName = HTableNameConverter.toTableName(tablePrefix, tableId);
    ddlExecutor.deleteTableIfExists(tableName.getNamespaceAsString(),
        tableName.getQualifierAsString());
  }

  @Override
  public void modifyTable(HBaseDDLExecutor ddlExecutor, HTableDescriptor tableDescriptor)
      throws IOException {
    Preconditions.checkArgument(ddlExecutor != null, "HBaseDDLExecutor should not be null");
    Preconditions.checkArgument(tableDescriptor != null, "Table descriptor should not be null.");
    TableName tableName = tableDescriptor.getTableName();
    TableDescriptor tbd = HBase10CDHTableDescriptorUtil.getTableDescriptor(tableDescriptor);
    ddlExecutor.modifyTable(tableName.getNamespaceAsString(), tableName.getQualifierAsString(),
        tbd);
  }

  @Override
  public List<HRegionInfo> getTableRegions(HBaseAdmin admin, TableId tableId) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    return admin.getTableRegions(HTableNameConverter.toTableName(tablePrefix, tableId));
  }

  @Override
  public List<TableId> listTablesInNamespace(HBaseAdmin admin, String namespaceId)
      throws IOException {
    List<TableId> tableIds = Lists.newArrayList();
    HTableDescriptor[] hTableDescriptors =
        admin.listTableDescriptorsByNamespace(HTableNameConverter.encodeHBaseEntity(namespaceId));
    for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
      if (isCDAPTable(hTableDescriptor)) {
        tableIds.add(HTableNameConverter.from(hTableDescriptor));
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
        tableIds.add(HTableNameConverter.from(hTableDescriptor));
      }
    }
    return tableIds;
  }

  @Override
  public void setCompression(HColumnDescriptor columnDescriptor, CompressionType type) {
    switch (type) {
      case LZO:
        columnDescriptor.setCompressionType(Compression.Algorithm.LZO);
        break;
      case SNAPPY:
        columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
        break;
      case GZIP:
        columnDescriptor.setCompressionType(Compression.Algorithm.GZ);
        break;
      case NONE:
        columnDescriptor.setCompressionType(Compression.Algorithm.NONE);
        break;
      default:
        throw new IllegalArgumentException("Unsupported compression type: " + type);
    }
  }

  @Override
  public void setBloomFilter(HColumnDescriptor columnDescriptor, BloomType type) {
    switch (type) {
      case ROW:
        columnDescriptor.setBloomFilterType(org.apache.hadoop.hbase.regionserver.BloomType.ROW);
        break;
      case ROWCOL:
        columnDescriptor.setBloomFilterType(org.apache.hadoop.hbase.regionserver.BloomType.ROWCOL);
        break;
      case NONE:
        columnDescriptor.setBloomFilterType(org.apache.hadoop.hbase.regionserver.BloomType.NONE);
        break;
      default:
        throw new IllegalArgumentException("Unsupported bloom filter type: " + type);
    }
  }

  @Override
  public CompressionType getCompression(HColumnDescriptor columnDescriptor) {
    Compression.Algorithm type = columnDescriptor.getCompressionType();
    switch (type) {
      case LZO:
        return CompressionType.LZO;
      case SNAPPY:
        return CompressionType.SNAPPY;
      case GZ:
        return CompressionType.GZIP;
      case NONE:
        return CompressionType.NONE;
      default:
        throw new IllegalArgumentException("Unsupported compression type: " + type);
    }
  }

  @Override
  public BloomType getBloomFilter(HColumnDescriptor columnDescriptor) {
    org.apache.hadoop.hbase.regionserver.BloomType type = columnDescriptor.getBloomFilterType();
    switch (type) {
      case ROW:
        return BloomType.ROW;
      case ROWCOL:
        return BloomType.ROWCOL;
      case NONE:
        return BloomType.NONE;
      default:
        throw new IllegalArgumentException("Unsupported bloom filter type: " + type);
    }
  }

  @Override
  public boolean isGlobalAdmin(Configuration hConf) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(hConf)) {
      if (!AccessControlClient.isAccessControllerRunning(connection)) {
        return true;
      }
      try {
        AccessControlClient.getUserPermissions(connection, "");
        return true;
      } catch (Throwable t) {
        LOG.warn("Failed to list user permissions to ensure global admin privilege", t);
        return false;
      }
    }
  }

  @Override
  public Class<? extends Coprocessor> getTransactionDataJanitorClassForVersion() {
    return DefaultTransactionProcessor.class;
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
  public ScanBuilder buildScan() {
    return new HBase10CDHScanBuilder();
  }

  @Override
  public ScanBuilder buildScan(Scan scan) throws IOException {
    return new HBase10CDHScanBuilder(scan);
  }

  @Override
  public IncrementBuilder buildIncrement(byte[] row) {
    return new HBase10CDHIncrementBuilder(row);
  }

  @Override
  public PutBuilder buildPut(byte[] row) {
    return new HBase10CDHPutBuilder(row);
  }

  @Override
  public PutBuilder buildPut(Put put) {
    return new HBase10CDHPutBuilder(put);
  }

  @Override
  public GetBuilder buildGet(byte[] row) {
    return new HBase10CDHGetBuilder(row);
  }

  @Override
  public GetBuilder buildGet(Get get) {
    return new HBase10CDHGetBuilder(get);
  }

  @Override
  public DeleteBuilder buildDelete(byte[] row) {
    return new HBase10CDHDeleteBuilder(row);
  }

  @Override
  public DeleteBuilder buildDelete(Delete delete) {
    return new HBase10CDHDeleteBuilder(delete);
  }
}
