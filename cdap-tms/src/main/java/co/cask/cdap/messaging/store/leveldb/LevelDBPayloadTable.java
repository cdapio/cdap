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

package co.cask.cdap.messaging.store.leveldb;

import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.store.AbstractPayloadTable;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.RawPayloadTableEntry;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * LevelDB implementation of {@link PayloadTable}.
 */
public class LevelDBPayloadTable extends AbstractPayloadTable {
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions().sync(true);
  private final DB levelDB;

  public LevelDBPayloadTable(DB levelDB) {
    this.levelDB = levelDB;
  }

  @Override
  protected CloseableIterator<RawPayloadTableEntry> read(byte[] startRow, byte[] stopRow,
                                                         final int limit) throws IOException {
    final DBScanIterator iterator = new DBScanIterator(levelDB, startRow, stopRow);
    return new AbstractCloseableIterator<RawPayloadTableEntry>() {
      private final RawPayloadTableEntry tableEntry = new RawPayloadTableEntry();
      private boolean closed = false;
      private int maxLimit = limit;

      @Override
      protected RawPayloadTableEntry computeNext() {
        if (closed || maxLimit <= 0 || (!iterator.hasNext())) {
          return endOfData();
        }

        Map.Entry<byte[], byte[]> row = iterator.next();
        maxLimit--;
        return tableEntry.set(row.getKey(), row.getValue());
      }

      @Override
      public void close() {
        try {
          iterator.close();
        } finally {
          endOfData();
          closed = true;
        }
      }
    };
  }

  @Override
  public void persist(Iterator<RawPayloadTableEntry> entries) throws IOException {
    try (WriteBatch writeBatch = levelDB.createWriteBatch()) {
      while (entries.hasNext()) {
        RawPayloadTableEntry entry = entries.next();
        byte[] key = entry.getKey();
        byte[] value = entry.getValue();
        // LevelDB doesn't make copies, and since we reuse RawPayloadTableEntry object, we need to create copies.
        writeBatch.put(Arrays.copyOf(key, key.length), Arrays.copyOf(value, value.length));
      }
      levelDB.write(writeBatch, WRITE_OPTIONS);
    } catch (DBException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  protected void delete(byte[] startRow, byte[] stopRow) throws IOException {
    List<byte[]> rowKeysToDelete = new ArrayList<>();
    try (CloseableIterator<Map.Entry<byte[], byte[]>> rowIterator = new DBScanIterator(levelDB, startRow, stopRow)) {
      while (rowIterator.hasNext()) {
        Map.Entry<byte[], byte[]> candidateRow = rowIterator.next();
        rowKeysToDelete.add(candidateRow.getKey());
      }
    }

    try {
      for (byte[] deleteRowKey : rowKeysToDelete) {
        levelDB.delete(deleteRowKey);
      }
    } catch (DBException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
