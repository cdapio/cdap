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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.store.AbstractMessageTable;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.RawMessageTableEntry;
import com.google.common.base.Preconditions;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * LevelDB implementation of {@link MessageTable}.
 */
final class LevelDBMessageTable extends AbstractMessageTable {
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions().sync(true);
  private static final String PAYLOAD_COL = "p";
  private static final String TX_COL = "t";

  private enum EncodeType {
    NON_TRANSACTIONAL(0),
    TRANSACTIONAL(1),
    PAYLOAD_REFERENCE(2);

    private final byte type;

    EncodeType(int type) {
      this.type = (byte) type;
    }

    byte getType() {
      return type;
    }
  }

  private final DB levelDB;

  LevelDBMessageTable(DB levelDB) {
    this.levelDB = levelDB;
  }

  @Override
  protected CloseableIterator<RawMessageTableEntry> read(byte[] startRow, byte[] stopRow) throws IOException {
    final DBScanIterator iterator = new DBScanIterator(levelDB, startRow, stopRow);
    final RawMessageTableEntry tableEntry = new RawMessageTableEntry();
    return new AbstractCloseableIterator<RawMessageTableEntry>() {
      private boolean closed = false;

      @Override
      protected RawMessageTableEntry computeNext() {
        if (closed || (!iterator.hasNext())) {
          return endOfData();
        }

        Map.Entry<byte[], byte[]> row = iterator.next();
        Map<String, byte[]> columns = decodeValue(row.getValue());
        return tableEntry.set(row.getKey(), columns.get(TX_COL), columns.get(PAYLOAD_COL));
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
  protected void persist(Iterator<RawMessageTableEntry> entries) throws IOException {
    try (WriteBatch writeBatch = levelDB.createWriteBatch()) {
      while (entries.hasNext()) {
        RawMessageTableEntry entry = entries.next();
        byte[] rowKey = entry.getKey();
        // LevelDB doesn't make copies, and since we reuse RawMessageTableEntry object, we need to create copies.
        writeBatch.put(Arrays.copyOf(rowKey, rowKey.length), encodeValue(entry.getTxPtr(), entry.getPayload()));
      }
      levelDB.write(writeBatch, WRITE_OPTIONS);
    } catch (DBException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  protected void delete(byte[] startKey, byte[] stopKey) throws IOException {
    WriteBatch writeBatch = levelDB.createWriteBatch();
    try (CloseableIterator<Map.Entry<byte[], byte[]>> rowIterator = new DBScanIterator(levelDB, startKey, stopKey)) {
      while (rowIterator.hasNext()) {
        writeBatch.delete(rowIterator.next().getKey());
      }
    }
    levelDB.write(writeBatch, WRITE_OPTIONS);
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  // Encoding:
  // If the returned byte array starts with 0, then it is a non-tx message and all the subsequent bytes are payload
  // If the returned byte array starts with 1, then next 8 bytes correspond to txWritePtr and rest are payload bytes
  private byte[] encodeValue(@Nullable byte[] txWritePtr, @Nullable byte[] payload) {
    // Non-transactional
    if (txWritePtr == null) {
      // For non-tx message, payload cannot be null
      Preconditions.checkArgument(payload != null, "Payload cannot be null for non-transactional message");
      byte[] result = new byte[1 + payload.length];
      result[0] = EncodeType.NON_TRANSACTIONAL.getType();
      Bytes.putBytes(result, 1, payload, 0, payload.length);
      return result;
    }

    // Transactional
    if (payload != null) {
      byte[] result = new byte[1 + Bytes.SIZEOF_LONG + payload.length];
      result[0] = EncodeType.TRANSACTIONAL.getType();
      Bytes.putBytes(result, 1, txWritePtr, 0, txWritePtr.length);
      Bytes.putBytes(result, 1 + Bytes.SIZEOF_LONG, payload, 0, payload.length);
      return result;
    }

    // Transactional but without payload, hence it's a payload table reference
    byte[] result = new byte[1 + Bytes.SIZEOF_LONG];
    result[0] = EncodeType.PAYLOAD_REFERENCE.getType();
    Bytes.putBytes(result, 1, txWritePtr, 0, txWritePtr.length);
    return result;
  }

  private Map<String, byte[]> decodeValue(byte[] value) {
    Map<String, byte[]> data = new HashMap<>();

    if (value[0] == EncodeType.NON_TRANSACTIONAL.getType()) {
      data.put(PAYLOAD_COL, Arrays.copyOfRange(value, 1, value.length));
    } else {
      data.put(TX_COL, Arrays.copyOfRange(value, 1, 1 + Bytes.SIZEOF_LONG));

      // Only transactional type has payload, otherwise payload should be null.
      if (value[0] == EncodeType.TRANSACTIONAL.getType()) {
        data.put(PAYLOAD_COL, Arrays.copyOfRange(value, 1 + Bytes.SIZEOF_LONG, value.length));
      }
    }
    return data;
  }
}
