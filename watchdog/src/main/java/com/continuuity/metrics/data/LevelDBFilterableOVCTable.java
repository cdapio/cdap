/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.engine.leveldb.KeyValue;
import com.continuuity.data.engine.leveldb.LevelDBOVCTable;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.table.Scanner;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.filter.Filter;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 *
 */
public class LevelDBFilterableOVCTable extends LevelDBOVCTable implements FilterableOVCTable {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBFilterableOVCTable.class);

  public LevelDBFilterableOVCTable(final String basePath, final String tableName,
                                   final Integer blockSize, final Long cacheSize) throws OperationException {
    super(basePath, tableName, blockSize, cacheSize);
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns, ReadPointer readPointer) {
    return scan(startRow, stopRow, columns, readPointer, null);
  }

  @Override
  public boolean isFilterSupported(Class<?> filterClass) {
    return true;
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer, Filter filter) {
    return scan(startRow, stopRow, null, readPointer, filter);
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns, ReadPointer readPointer, Filter filter) {
    return new FilterableLevelDBScanner(db.iterator(), startRow, stopRow, readPointer, filter);
  }

  @Override
  public Scanner scan(ReadPointer readPointer, Filter filter) {
    return scan(null, null, readPointer, filter);
  }

  // this is gross...
  // TODO: refactor properly with our own Filter/KeyValue abstraction
  public static org.apache.hadoop.hbase.KeyValue convert(KeyValue kv) {
    org.apache.hadoop.hbase.KeyValue.Type type = org.apache.hadoop.hbase.KeyValue.Type.codeToType(kv.getType());
    return new org.apache.hadoop.hbase.KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(),
                                                kv.getTimestamp(), type, kv.getValue());
  }

  public static KeyValue convert(org.apache.hadoop.hbase.KeyValue kv) {
    KeyValue.Type type = KeyValue.Type.codeToType(kv.getType());
    return new KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(),
                        kv.getTimestamp(), type, kv.getValue());
  }

  /**
   * Scanner on top of levelDB dbiterator with a filter
   */
  public class FilterableLevelDBScanner implements Scanner {

    private final DBIterator iterator;
    private final byte [] endRow;
    private final ReadPointer readPointer;
    private final Filter filter;

    public FilterableLevelDBScanner(DBIterator iterator, byte [] startRow, byte[] endRow, ReadPointer readPointer,
                                    Filter filter) {
      this.iterator = iterator;
      if (startRow == null) {
        iterator.seekToFirst();
      } else {
        this.iterator.seek(createStartKey(startRow));
      }
      this.endRow = endRow;
      this.readPointer = readPointer;
      this.filter = filter;
    }

    private boolean isVisible(KeyValue keyValue) {
      if (readPointer != null && !readPointer.isVisible(keyValue.getTimestamp())) {
        return false;
      } else {
        return true;
      }
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      //From the current iterator do one of the following:
      // a) get all columns for current visible row if it is not the endRow
      // b) return null if we have reached endRow
      // c) return null if there are no more entries

      long lastDelete = -1;
      long undeleted = -1;
      byte[] lastRow = new byte[0];
      byte[] lastCol = new byte[0];
      byte[] curCol = new byte[0];

      Map<byte[], byte[]> columnValues = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.peekNext();
        KeyValue keyValue = createKeyValue(entry.getKey(), entry.getValue());
        byte[] row = keyValue.getRow();

        if (endRow != null && Bytes.compareTo(row, endRow) >= 0) {
          //already reached the end. So break.
          break;
        }

        // we're at a new row
        if (!Bytes.equals(lastRow, row)) {
          lastDelete = -1;
          undeleted = -1;
          lastCol = new byte[0];
          curCol = new byte[0];
          if (columnValues.size() > 0){
            //If we have reached here. We have read all columns for a single row - since current row is not the same
            // as previous row and we have collected atleast one valid value in the columnValues collection. Break.
            break;
          }
        }

        // check if we can skip this row and column
        if (filter != null) {
          if (filter.filterAllRemaining()) {
            break;
          }

          org.apache.hadoop.hbase.KeyValue currKV = convert(keyValue);
          Filter.ReturnCode rc = filter.filterKeyValue(currKV);

          switch (rc) {
            case SEEK_NEXT_USING_HINT:
              KeyValue nextKV = convert(filter.getNextKeyHint(currKV));
              iterator.seek(nextKV.getKey());
              lastRow = row;
              continue;
            case SKIP:
            case NEXT_COL:
            case NEXT_ROW:
              // TODO: handle next col and next row differently, seek to next row instead of iterating through cols
              lastRow = row;
              iterator.next();
              continue;
            case INCLUDE:
            case INCLUDE_AND_NEXT_COL:
            default:
              break;
          }
        }

        lastRow = row;
        iterator.next();

        if (!isVisible(keyValue)) {
          continue;
        }

        if (Bytes.equals(lastCol, keyValue.getQualifier())) {
          continue;
        }

        if (!Bytes.equals(curCol, keyValue.getQualifier())) {
          curCol = keyValue.getQualifier();
          lastDelete = -1;
          undeleted = -1;
        }

        long curVersion = keyValue.getTimestamp();
        KeyValue.Type type = KeyValue.Type.codeToType(keyValue.getType());

        if (type == KeyValue.Type.Delete) {
          lastDelete = curVersion;
          continue;
        }
        if (curVersion == lastDelete) {
          continue;
        }

        if (type == KeyValue.Type.UndeleteColumn) {
          undeleted = curVersion;
          continue;
        }

        if (type == KeyValue.Type.DeleteColumn) {
          if (undeleted == curVersion) {
            continue;
          } else {
            lastCol = keyValue.getQualifier();
            continue;
          }
        }

        if (type == KeyValue.Type.Put) {
          if (curVersion != lastDelete) {
            // if we've gotten here, this is a valid column, add it to the results
            columnValues.put(keyValue.getQualifier(), keyValue.getValue());
            lastCol = keyValue.getQualifier();
          }
        }
      }
      if (columnValues.size() == 0) {
        return null;
      } else {
        return new ImmutablePair<byte[], Map<byte[], byte[]>>(lastRow, columnValues);

      }
    }

    @Override
    public void close() {
      try {
        iterator.close();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
