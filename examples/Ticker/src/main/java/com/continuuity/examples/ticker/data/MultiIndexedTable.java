/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.continuuity.examples.ticker.data;

import com.continuuity.api.annotation.Property;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Put;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Scanner;
import com.continuuity.api.data.dataset.table.Table;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Stores raw data in an table, along with a separate table indexing the origin data by all individual values.
 * Index rows are keyed by the combination of [key qualifier]-[key value]-[timestamp]-[origin rowkey].
 *
 * Note that <em>all key-values for a single row must be written with the same timestamp</em> in order for
 * reading by indexes to work correctly.
 */
public class MultiIndexedTable extends DataSet {
  private static final Logger LOG = LoggerFactory.getLogger(MultiIndexedTable.class);
  // use a null value as separator for multiple field in the row key
  private static final byte NULL_BYTE = (byte) 0x0;
  private static final byte[] KEY_SEP = new byte[]{ NULL_BYTE };

  // column qualifier used to store the origin table row key
  private static final byte[] ROW_KEY_COL = Bytes.toBytes("r");

  // meta key prefix used to store a record of the index rows created for each origin row
  private static final byte[] META_ROW_PREFIX = Bytes.toBytes("_meta_");

  // The index table matching the primary data table will be named by adding this suffix
  private static final String INDEX_SUFFIX = "-idx";

  private final Table table;
  private final Table indexTable;
  // String representation of the field storing timestamp values
  @Property
  private String timestampFieldName;
  private byte[] timestampField;
  private Set<byte[]> ignoreIndexing;

  public MultiIndexedTable(String name, byte[] timestampField, Set<byte[]> doNotIndex) {
    super(name);
    this.table = new Table(name);
    this.indexTable = new Table(name + INDEX_SUFFIX);
    this.timestampFieldName = Bytes.toString(timestampField);
    this.ignoreIndexing = doNotIndex;
  }

  @Override
  public void initialize(DataSetSpecification spec, DataSetContext context) {
    super.initialize(spec, context);
    this.timestampField = Bytes.toBytes(timestampFieldName);
  }

  @Override
  public void close() {
    this.table.close();
    this.indexTable.close();
  }

  /**
   * Adds a {@link Put} to the origin table, while adding index entries for all of the Put's values to
   * the index table.
   * @param put The Put to add to the origin table.
   */
  public void put(Put put) {
    Map<byte[], byte[]> values = put.getValues();
    logValues(values);
    byte[] timestampValue = values.get(timestampField);
    // used to add a record of index keys to the origin row, so we can handle deletes
    Put metaRow = new Put(Bytes.add(META_ROW_PREFIX, KEY_SEP, put.getRow()));
    for (Map.Entry<byte[], byte[]> entry : put.getValues().entrySet()) {
      if (Bytes.equals(entry.getKey(), timestampField) ||
          (ignoreIndexing != null && ignoreIndexing.contains(entry.getKey()))) {
        continue;
      }
      // construct index key as: column (type) + value + timestamp + rowkey
      byte[] indexKey = Bytes.add(entry.getKey(), KEY_SEP,
          Bytes.add(entry.getValue(), KEY_SEP,
              Bytes.add(timestampValue, KEY_SEP, put.getRow())));
      if (LOG.isTraceEnabled()) {
        LOG.trace("Putting index row: " + Bytes.toStringBinary(indexKey));
      }
      indexTable.put(indexKey, ROW_KEY_COL, put.getRow());
      metaRow.add(indexKey, Bytes.EMPTY_BYTE_ARRAY);
    }
    indexTable.put(metaRow);
    table.put(put);
  }

  Table getIndexTable() {
    return indexTable;
  }

  /**
   * Deletes a full row or specific columns from a row in the origin table, plus any associated index rows.
   * @param delete The Delete request for the origin table.
   */
  public void delete(Delete delete) {
    // retrieve the record of any stored index rows
    byte[] metaRowKey = Bytes.add(META_ROW_PREFIX, KEY_SEP, delete.getRow());
    Row metaRow = indexTable.get(metaRowKey);

    List<byte[]> columnsToDelete = delete.getColumns();
    if (columnsToDelete != null && !columnsToDelete.isEmpty()) {
      // only delete index rows matching the deleted columns
      for (byte[] col : columnsToDelete) {
        for (byte[] metaKey : metaRow.getColumns().keySet()) {
          if (Bytes.startsWith(metaKey, Bytes.add(col, KEY_SEP))) {
            indexTable.delete(metaKey);
            break;
          }
        }
      }
    } else {
      // all index rows should be deleted
      for (byte[] key : metaRow.getColumns().keySet()) {
        indexTable.delete(key);
      }
    }
    table.delete(delete);
  }

  /**
   * Reads a set of origin rows by a combination of required index values and a time range.
   */
  public List<Row> readBy(Map<byte[], byte[]> indexValues, long startTime, long endTime) {
    List<Row> results = Lists.newArrayList();
    // open a separate scanner for each requested index value
    List<PeekableScanner> scanners = Lists.newArrayListWithCapacity(indexValues.size());
    for (Map.Entry<byte[], byte[]> val : indexValues.entrySet()) {
      byte[] startKey = Bytes.add(val.getKey(), KEY_SEP,
          Bytes.add(val.getValue(), KEY_SEP, Bytes.toBytes(startTime)));
      byte[] stopKey = Bytes.add(val.getKey(), KEY_SEP,
          Bytes.add(val.getValue(), KEY_SEP, Bytes.toBytes(endTime + 1)));
      LOG.debug("Scanning from " + Bytes.toStringBinary(startKey) + " to " + Bytes.toStringBinary(stopKey));
      scanners.add(new PeekableScanner(indexTable.scan(startKey, stopKey)));
    }

    // merge together the results
    // only row keys that appear in all scanners are valid
    byte[] lastTimestampedRow = null;
    byte[] matchedRow = null;
    List<byte[]> matchedRowKeys = Lists.newArrayList();
    outer:
    do {
      matchedRow = null;
      for (int i = 0; i < scanners.size(); i++) {
        LOG.debug("Last timestamped row = " + Bytes.toStringBinary(lastTimestampedRow));
        PeekableScanner scanner = scanners.get(i);
        Row nextRow = scanner.peek();
        // if we hit the end of any scanner, we're done
        if (nextRow == null) {
          break outer;
        }
        if (matchedRow == null) {
          matchedRow = nextRow.get(ROW_KEY_COL);
        }
        LOG.debug("Scanner " + i + " Next row is: " + Bytes.toStringBinary(nextRow.getRow()));
        byte[] nextTimestampedRow = getTimestampedRowkey(nextRow.getRow());
        if (lastTimestampedRow == null) {
          lastTimestampedRow = nextTimestampedRow;
          continue;
        }
        int order = Bytes.compareTo(lastTimestampedRow, nextTimestampedRow);
        if (order < 0) {
          // no possible match on this scanner
          matchedRow = null;
          lastTimestampedRow = nextTimestampedRow;
          break;
        } else if (order > 0) {
          // this row doesn't exist in other scanners, so advance until we catch up
          while (Bytes.compareTo(nextTimestampedRow, lastTimestampedRow) < 0) {
            scanner.next();
            nextRow = scanner.peek();
            if (nextRow == null) {
              // done with this scanner, no more matches
              break outer;
            }
            nextTimestampedRow = getTimestampedRowkey(nextRow.getRow());
          }
          int newOrder = Bytes.compareTo(nextTimestampedRow, lastTimestampedRow);
          if (newOrder > 0) {
            // passed the last seen without a match
            matchedRow = null;
            lastTimestampedRow = nextTimestampedRow;
            break;
          } else if (newOrder == 0) {
            // matched the current
            matchedRow = nextRow.get(ROW_KEY_COL);
          }
        }
      }
      if (matchedRow != null) {
        LOG.debug("Adding row " + Bytes.toStringBinary(matchedRow));
        matchedRowKeys.add(matchedRow);
        // remove from all scanners
        for (int i = 0; i < scanners.size(); i++) {
          scanners.get(i).next();
        }
        // reset last timestamp
        lastTimestampedRow = null;
      }
    } while (lastTimestampedRow != null || matchedRow != null);

    // get the origin rows
    if (!matchedRowKeys.isEmpty()) {
      for (byte[] key : matchedRowKeys) {
        Row originRow = table.get(key);
        if (originRow != null) {
          results.add(originRow);
        }
      }
    }

    LOG.info("Returning " + results.size() + " results.");
    return results;
  }

  /**
   * Returns just the [timestamp]-[rowkey] portion of the index key.  This will be the same for index rows from the
   * same origin row.
   */
  private static byte[] getTimestampedRowkey(byte[] indexKey) {
    int sepCount = 0;
    for (int i = 0; i < indexKey.length; i++) {
      if (indexKey[i] == NULL_BYTE) {
        sepCount++;
      }
      if (sepCount > 1) {
        return Bytes.tail(indexKey, indexKey.length - (i + 1));
      }
    }
    return null;
  }

  private void logValues(Map<byte[], byte[]> values) {
    if (!LOG.isTraceEnabled()) {
      return;
    }
    LOG.trace("Put values: ");
    for (Map.Entry<byte[], byte[]> entry : values.entrySet()) {
      LOG.trace(Bytes.toStringBinary(entry.getKey()) + "=" + Bytes.toStringBinary(entry.getValue()) + ", ");
    }
  }
  private static final class PeekableScanner implements Scanner {
    private final Scanner wrapped;
    private Row nextRow;

    public PeekableScanner(Scanner toWrap) {
      this.wrapped = toWrap;
    }

    public Row peek() {
      if (nextRow == null) {
        nextRow = wrapped.next();
      }
      return nextRow;
    }

    @Override
    public Row next() {
      if (nextRow != null) {
        Row toReturn = nextRow;
        nextRow = null;
        return toReturn;
      }
      return wrapped.next();
    }

    @Override
    public void close() {
      this.wrapped.close();
      this.nextRow = null;
    }
  }
}
