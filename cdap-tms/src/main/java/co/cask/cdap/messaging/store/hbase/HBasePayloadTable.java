/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.store.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.DistributedScanner;
import co.cask.cdap.messaging.store.AbstractPayloadTable;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.RawPayloadTableEntry;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * HBase implementation of {@link PayloadTable}.
 */
final class HBasePayloadTable extends AbstractPayloadTable {
  private static final byte[] COL = Bytes.toBytes('c');

  private final HBaseTableUtil tableUtil;
  private final byte[] columnFamily;
  private final HTable hTable;
  private final AbstractRowKeyDistributor rowKeyDistributor;
  private final ExecutorService scanExecutor;

  HBasePayloadTable(HBaseTableUtil tableUtil, HTable hTable, byte[] columnFamily,
                    AbstractRowKeyDistributor rowKeyDistributor, ExecutorService scanExecutor) {
    this.tableUtil = tableUtil;
    this.hTable = hTable;
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
    this.rowKeyDistributor = rowKeyDistributor;
    this.scanExecutor = scanExecutor;
  }

  @Override
  public CloseableIterator<RawPayloadTableEntry> read(byte[] startRow, byte[] stopRow,
                                                      final int limit) throws IOException {
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(stopRow)
      .build();
    final ResultScanner scanner = DistributedScanner.create(hTable, scan, rowKeyDistributor, scanExecutor);
    final Iterator<Result> results = scanner.iterator();

    return new AbstractCloseableIterator<RawPayloadTableEntry>() {
      private final RawPayloadTableEntry tableEntry = new RawPayloadTableEntry();
      private boolean closed = false;
      private int maxLimit = limit;

      @Override
      protected RawPayloadTableEntry computeNext() {
        if (closed || maxLimit <= 0 || (!results.hasNext())) {
          return endOfData();
        }

        Result result = results.next();
        maxLimit--;
        return tableEntry.set(rowKeyDistributor.getOriginalKey(result.getRow()), result.getValue(columnFamily, COL));
      }

      @Override
      public void close() {
        try {
          scanner.close();
        } finally {
          endOfData();
          closed = true;
        }
      }
    };
  }

  @Override
  public void persist(Iterator<RawPayloadTableEntry> entries) throws IOException {
    List<Put> batchPuts = new ArrayList<>();
    while (entries.hasNext()) {
      RawPayloadTableEntry tableEntry = entries.next();
      Put put = tableUtil.buildPut(rowKeyDistributor.getDistributedKey(tableEntry.getKey()))
        .add(columnFamily, COL, tableEntry.getValue())
        .build();
      batchPuts.add(put);
    }

    if (!batchPuts.isEmpty()) {
      hTable.put(batchPuts);
      if (!hTable.isAutoFlush()) {
        hTable.flushCommits();
      }
    }
  }

  @Override
  protected void delete(byte[] startRow, byte[] stopRow) throws IOException {
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(stopRow)
      .build();

    List<Delete> batchDeletes = new ArrayList<>();
    try (ResultScanner scanner = DistributedScanner.create(hTable, scan, rowKeyDistributor, scanExecutor)) {
      for (Result result : scanner) {
        // No need to turn the key back to the original row key because we want to delete with the actual row key
        Delete delete = tableUtil.buildDelete(result.getRow()).build();
        batchDeletes.add(delete);
        hTable.delete(delete);
      }
    }

    if (!batchDeletes.isEmpty()) {
      hTable.delete(batchDeletes);
      if (!hTable.isAutoFlush()) {
        hTable.flushCommits();
      }
    }
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }
}
