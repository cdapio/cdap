/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.messaging.store.hbase;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.hbase.wd.AbstractRowKeyDistributor;
import io.cdap.cdap.hbase.wd.DistributedScanner;
import io.cdap.cdap.messaging.store.AbstractPayloadTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.messaging.store.RawPayloadTableEntry;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

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
  private final Table table;
  private final BufferedMutator mutator;
  private final AbstractRowKeyDistributor rowKeyDistributor;
  private final ExecutorService scanExecutor;
  private final int scanCacheRows;
  private final HBaseExceptionHandler exceptionHandler;

  HBasePayloadTable(HBaseTableUtil tableUtil, Table table, byte[] columnFamily,
                    AbstractRowKeyDistributor rowKeyDistributor, ExecutorService scanExecutor,
                    int scanCacheRows, HBaseExceptionHandler exceptionHandler) throws IOException {
    this.tableUtil = tableUtil;
    this.table = table;
    this.mutator = tableUtil.createBufferedMutator(table, HBaseTableUtil.DEFAULT_WRITE_BUFFER_SIZE);
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
    this.rowKeyDistributor = rowKeyDistributor;
    this.scanExecutor = scanExecutor;
    this.scanCacheRows = scanCacheRows;
    this.exceptionHandler = exceptionHandler;
  }

  @Override
  public CloseableIterator<RawPayloadTableEntry> read(byte[] startRow, byte[] stopRow,
                                                      final int limit) throws IOException {
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(stopRow)
      .setCaching(scanCacheRows)
      .build();

    final ResultScanner scanner = DistributedScanner.create(table, scan, rowKeyDistributor, scanExecutor);
    return new AbstractCloseableIterator<RawPayloadTableEntry>() {
      private final RawPayloadTableEntry tableEntry = new RawPayloadTableEntry();
      private boolean closed = false;
      private int maxLimit = limit;

      @Override
      protected RawPayloadTableEntry computeNext() {
        if (closed || maxLimit <= 0) {
          return endOfData();
        }

        Result result;
        try {
          result = scanner.next();
        } catch (IOException e) {
          throw exceptionHandler.handleAndWrap(e);
        }
        if (result == null) {
          return endOfData();
        }
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

    try {
      if (!batchPuts.isEmpty()) {
        mutator.mutate(batchPuts);
        mutator.flush();
      }
    } catch (IOException e) {
      throw exceptionHandler.handle(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      mutator.close();
    } finally {
      table.close();
    }
  }
}
