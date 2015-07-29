/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NavigableMap;

/**
 * Migrates the tables of a PartitionedFileSet.
 */
public class PartitionedFileSetTableMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileSetTableMigrator.class);

  // copied from IndexedTable.java. This upgrade is only needed from 3.0 -> 3.1, so this code will be removed
  // after 3.1 release. This is done to avoid publicizing the fields on IndexedTable.
  private static final byte[] IDX_COL = {'r'};
  private static final byte DELIMITER_BYTE = 0;
  private static final byte[] KEY_DELIMITER = new byte[] { DELIMITER_BYTE };

  private static final long MILLION = 1000 * 1000;
  protected final HBaseTableUtil tableUtil;
  protected final Configuration conf;
  private final DatasetFramework dsFramework;

  @Inject
  public PartitionedFileSetTableMigrator(HBaseTableUtil tableUtil, Configuration conf, DatasetFramework dsFramework) {
    this.tableUtil = tableUtil;
    this.conf = conf;
    this.dsFramework = dsFramework;
  }

  /**
   * Given a specification of a PartitionedFileSet, creates an IndexedTable for the new embedded partitions Dataset
   * and migrates the data from the old partitions table to the new partitions table.
   *
   * @param namespaceId the namespace that contains the PartitionedFileSet
   * @param dsSpec the specification of the PartitionedFileSet that needs migrating
   * @throws Exception
   */
  public void upgrade(Id.Namespace namespaceId, DatasetSpecification dsSpec) throws Exception {
    DatasetSpecification newPartitionsSpec = dsSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME);
    TableId oldPartitionsTableId = TableId.from(namespaceId, newPartitionsSpec.getName());
    TableId indexTableId = TableId.from(namespaceId, newPartitionsSpec.getName() + ".i");
    TableId dataTableId = TableId.from(namespaceId, newPartitionsSpec.getName() + ".d");

    byte[] columnFamily = HBaseTableAdmin.getColumnFamily(newPartitionsSpec);

    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    if (!tableUtil.tableExists(hBaseAdmin, oldPartitionsTableId)) {
      LOG.info("Old Partitions table does not exist: {}. Nothing to migrate from.", oldPartitionsTableId);
      return;
    }

    DatasetsUtil.createIfNotExists(dsFramework, Id.DatasetInstance.from(namespaceId, newPartitionsSpec.getName()),
                                   IndexedTable.class.getName(),
                                   DatasetProperties.builder().addAll(newPartitionsSpec.getProperties()).build());

    HTable oldTable = tableUtil.createHTable(conf, oldPartitionsTableId);
    HTable newIndexTable = tableUtil.createHTable(conf, indexTableId);
    HTable newDataTable = tableUtil.createHTable(conf, dataTableId);

    LOG.info("Starting upgrade for table {}", Bytes.toString(oldTable.getTableName()));
    try {
      ScanBuilder scan = tableUtil.buildScan();
      scan.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
      // migrate all versions. Since we write the same versions in the new table, transactional semantics should carry
      // across (invalid transactions' data remains invalid, etc). There shouldn't be too many versions of a given row
      // of the partitions table anyways.
      scan.setMaxVersions();
      try (ResultScanner resultScanner = oldTable.getScanner(scan.build())) {
        Result result;
        while ((result = resultScanner.next()) != null) {
          Put put = new Put(result.getRow());

          Long writeVersion = null;
          for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap :
            result.getMap().entrySet()) {
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnMap : familyMap.getValue().entrySet()) {
              for (Map.Entry<Long, byte[]> columnEntry : columnMap.getValue().entrySet()) {
                Long timeStamp = columnEntry.getKey();
                byte[] colVal = columnEntry.getValue();
                put.add(familyMap.getKey(), columnMap.getKey(), timeStamp, colVal);
                LOG.debug("Migrating row. family: '{}', column: '{}', ts: '{}', colVal: '{}'",
                          familyMap.getKey(), columnMap.getKey(), timeStamp, colVal);

                // the hbase version for all the columns are equal since every column of a row corresponds to one
                // partition and a partition is created entirely within a single transaction.
                writeVersion = timeStamp;
              }
            }
          }

          Preconditions.checkNotNull(writeVersion,
                                     "There should have been at least one column in the scan. Table: {}, Rowkey: {}",
                                     oldPartitionsTableId, result.getRow());

          // additionally since 3.1.0, we keep two addition columns for each partition: creation time of the partition
          // and the transaction write pointer of the transaction in which the partition was added
          byte[] hbaseVersionBytes = Bytes.toBytes(writeVersion);
          put.add(columnFamily, PartitionedFileSetDataset.WRITE_PTR_COL,
                  writeVersion, hbaseVersionBytes);
          // here, we make the assumption that dropping the six right-most digits of the transaction write pointer
          // yields the timestamp at which it started
          byte[] creationTimeBytes = Bytes.toBytes(writeVersion / MILLION);
          put.add(columnFamily, PartitionedFileSetDataset.CREATION_TIME_COL,
                  writeVersion, creationTimeBytes);

          newDataTable.put(put);

          // index the data table on the two columns
          Put indexWritePointerPut = new Put(createIndexKey(result.getRow(),
                                                            PartitionedFileSetDataset.WRITE_PTR_COL,
                                                            hbaseVersionBytes));
          indexWritePointerPut.add(columnFamily, IDX_COL, writeVersion, result.getRow());

          Put indexCreationTimePut = new Put(createIndexKey(result.getRow(),
                                                            PartitionedFileSetDataset.CREATION_TIME_COL,
                                                            creationTimeBytes));
          indexCreationTimePut.add(columnFamily, IDX_COL, writeVersion, result.getRow());

          newIndexTable.put(ImmutableList.of(indexCreationTimePut, indexWritePointerPut));

          LOG.debug("Deleting old key {}.", Bytes.toString(result.getRow()));
          oldTable.delete(new Delete(result.getRow()));
        }
      } finally {
        oldTable.close();
        newIndexTable.close();
        newDataTable.close();
      }
      LOG.info("Successfully migrated data from table {} Now deleting it.", Bytes.toString(oldTable.getTableName()));
      tableUtil.dropTable(hBaseAdmin, oldPartitionsTableId);
      LOG.info("Succsefully deleted old data table {}", Bytes.toString(oldTable.getTableName()));
    } catch (Exception e) {
      LOG.error("Error while migrating data from table: {}", oldPartitionsTableId, e);
      throw Throwables.propagate(e);
    } finally {
      oldTable.close();
    }
  }

  private byte[] createIndexKey(byte[] row, byte[] column, byte[] value) {
    return Bytes.concat(column, KEY_DELIMITER, value, KEY_DELIMITER, row);
  }
}
