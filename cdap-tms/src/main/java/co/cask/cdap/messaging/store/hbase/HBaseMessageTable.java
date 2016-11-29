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
import co.cask.cdap.data2.util.hbase.PutBuilder;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.DistributedScanner;
import co.cask.cdap.messaging.store.AbstractMessageTable;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.RawMessageTableEntry;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * HBase implementation of {@link MessageTable}.
 */
final class HBaseMessageTable extends AbstractMessageTable {
  private static final byte[] PAYLOAD_COL = Bytes.toBytes('p');
  private static final byte[] TX_COL = Bytes.toBytes('t');
  private static final int SCAN_CACHE_ROWS = 1000;

  private final HBaseTableUtil tableUtil;
  private final byte[] columnFamily;
  private final HTable hTable;
  private final AbstractRowKeyDistributor rowKeyDistributor;
  private final ExecutorService scanExecutor;

  HBaseMessageTable(HBaseTableUtil tableUtil, HTable hTable, byte[] columnFamily,
                    AbstractRowKeyDistributor rowKeyDistributor, ExecutorService scanExecutor) {
    this.tableUtil = tableUtil;
    this.hTable = hTable;
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
    this.rowKeyDistributor = rowKeyDistributor;
    this.scanExecutor = scanExecutor;
  }

  @Override
  protected CloseableIterator<RawMessageTableEntry> read(byte[] startRow, byte[] stopRow) throws IOException {
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(stopRow)
      .setCaching(SCAN_CACHE_ROWS)
      .build();

    final ResultScanner scanner = DistributedScanner.create(hTable, scan, rowKeyDistributor, scanExecutor);
    final Iterator<Result> results = scanner.iterator();
    final RawMessageTableEntry tableEntry = new RawMessageTableEntry();
    return new AbstractCloseableIterator<RawMessageTableEntry>() {
      private boolean closed = false;

      @Override
      protected RawMessageTableEntry computeNext() {
        if (closed || (!results.hasNext())) {
          return endOfData();
        }

        Result result = results.next();
        return tableEntry.set(rowKeyDistributor.getOriginalKey(result.getRow()), result.getValue(columnFamily, TX_COL),
                              result.getValue(columnFamily, PAYLOAD_COL));
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
  protected void persist(Iterator<RawMessageTableEntry> entries) throws IOException {
    List<Put> batchPuts = new ArrayList<>();
    while (entries.hasNext()) {
      RawMessageTableEntry entry = entries.next();
      PutBuilder putBuilder = tableUtil.buildPut(rowKeyDistributor.getDistributedKey(entry.getKey()));
      if (entry.getTxPtr() != null) {
        putBuilder.add(columnFamily, TX_COL, entry.getTxPtr());
      }

      if (entry.getPayload() != null) {
        putBuilder.add(columnFamily, PAYLOAD_COL, entry.getPayload());
      }
      batchPuts.add(putBuilder.build());
    }

    if (!batchPuts.isEmpty()) {
      hTable.put(batchPuts);
      if (!hTable.isAutoFlush()) {
        hTable.flushCommits();
      }
    }
  }

  @Override
  public void delete(byte[] startKey, byte[] stopKey) throws IOException {
    Scan scan = tableUtil.buildScan()
      .setStartRow(startKey)
      .setStopRow(stopKey)
      .setFilter(new FirstKeyOnlyFilter())
      .setCaching(SCAN_CACHE_ROWS)
      .build();

    List<Delete> batchDeletes = new ArrayList<>();
    try (ResultScanner scanner = DistributedScanner.create(hTable, scan, rowKeyDistributor, scanExecutor)) {
      for (Result result : scanner) {
        // No need to turn the key back to the original row key because we want to delete with the actual row key
        batchDeletes.add(tableUtil.buildDelete(result.getRow()).build());
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
