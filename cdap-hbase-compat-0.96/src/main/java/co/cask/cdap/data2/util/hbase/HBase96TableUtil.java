/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.increment.hbase96.IncrementHandler;
import co.cask.cdap.data2.transaction.coprocessor.hbase96.DefaultTransactionProcessor;
import co.cask.cdap.data2.transaction.queue.coprocessor.hbase96.DequeueScanObserver;
import co.cask.cdap.data2.transaction.queue.coprocessor.hbase96.HBaseQueueRegionObserver;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class HBase96TableUtil extends HBaseTableUtil {

  @Override
  public HTable createHTable(Configuration conf, TableId tableId) throws IOException {
    Preconditions.checkArgument(tableId != null, "Table id should not be null");
    return new HTable(conf, HTable96NameConverter.toTableName(tableId));
  }

  @Override
  public HTableDescriptor createHTableDescriptor(TableId tableId) {
    Preconditions.checkArgument(tableId != null, "Table id should not be null");
    return new HTableDescriptor(HTable96NameConverter.toTableName(tableId));
  }

  @Override
  public HTableDescriptor getHTableDescriptor(HBaseAdmin admin, TableId tableId) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    return admin.getTableDescriptor(HTable96NameConverter.toTableName(tableId));
  }

  @Override
  public boolean hasNamespace(HBaseAdmin admin, Id.Namespace namespace) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(namespace != null, "Namespace should not be null.");
    try {
      admin.getNamespaceDescriptor(HTableNameConverter.toHBaseNamespace(namespace));
      return true;
    } catch (NamespaceNotFoundException e) {
      return false;
    }
  }

  @Override
  public void createNamespaceIfNotExists(HBaseAdmin admin, Id.Namespace namespace) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(namespace != null, "Namespace should not be null.");
    if (!hasNamespace(admin, namespace)) {
      NamespaceDescriptor namespaceDescriptor =
        NamespaceDescriptor.create(HTableNameConverter.toHBaseNamespace(namespace)).build();
      admin.createNamespace(namespaceDescriptor);
    }
  }

  @Override
  public void deleteNamespaceIfExists(HBaseAdmin admin, Id.Namespace namespace) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(namespace != null, "Namespace should not be null.");
    if (hasNamespace(admin, namespace)) {
      admin.deleteNamespace(HTableNameConverter.toHBaseNamespace(namespace));
    }
  }

  @Override
  public void disableTable(HBaseAdmin admin, TableId tableId) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    admin.disableTable(HTable96NameConverter.toTableName(tableId));
  }

  @Override
  public void enableTable(HBaseAdmin admin, TableId tableId) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    admin.enableTable(HTable96NameConverter.toTableName(tableId));
  }

  @Override
  public boolean tableExists(HBaseAdmin admin, TableId tableId) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    return admin.tableExists(HTable96NameConverter.toTableName(tableId));
  }

  @Override
  public void deleteTable(HBaseAdmin admin, TableId tableId) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    admin.deleteTable(HTable96NameConverter.toTableName(tableId));
  }

  @Override
  public void modifyTable(HBaseAdmin admin, HTableDescriptor tableDescriptor) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableDescriptor != null, "Table decsriptor should not be null.");
    admin.modifyTable(tableDescriptor.getTableName(), tableDescriptor);
  }

  @Override
  public List<HRegionInfo> getTableRegions(HBaseAdmin admin, TableId tableId) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    return admin.getTableRegions(HTable96NameConverter.toTableName(tableId));
  }

  @Override
  public void deleteAllInNamespace(HBaseAdmin admin, Id.Namespace namespaceId, String tablePrefix) throws IOException {
    TableName[] tableNames = admin.listTableNamesByNamespace(HTableNameConverter.toHBaseNamespace(namespaceId));
    for (TableName tableName : tableNames) {
      String name = HTable96NameConverter.fromTableName(tableName).getTableName();
      if (name.startsWith(tablePrefix)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    }
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
  public Map<String, TableStats> getTableStats(HBaseAdmin admin) throws IOException {
    // The idea is to walk thru live region servers, collect table region stats and aggregate them towards table total
    // metrics.
    Map<String, TableStats> datasetStat = Maps.newHashMap();
    ClusterStatus clusterStatus = admin.getClusterStatus();

    for (ServerName serverName : clusterStatus.getServers()) {
      Map<byte[], RegionLoad> regionsLoad = clusterStatus.getLoad(serverName).getRegionsLoad();

      for (RegionLoad regionLoad : regionsLoad.values()) {
        String tableName = Bytes.toString(HRegionInfo.getTableName(regionLoad.getName()));
        TableStats stat = datasetStat.get(tableName);
        if (stat == null) {
          stat = new TableStats(regionLoad.getStorefileSizeMB(), regionLoad.getMemStoreSizeMB());
          datasetStat.put(tableName, stat);
        } else {
          stat.incStoreFileSizeMB(regionLoad.getStorefileSizeMB());
          stat.incMemStoreSizeMB(regionLoad.getMemStoreSizeMB());
        }
      }
    }
    return datasetStat;
  }

}
