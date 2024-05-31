/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.annotation.ReadOnly;
import io.cdap.cdap.api.annotation.WriteOnly;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DataSetException;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.table.Filter;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data2.dataset2.lib.table.BufferingTable;
import io.cdap.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import io.cdap.cdap.data2.dataset2.lib.table.IncrementValue;
import io.cdap.cdap.data2.dataset2.lib.table.PutValue;
import io.cdap.cdap.data2.dataset2.lib.table.Update;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.PrefixedNamespaces;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.data2.util.hbase.DeleteBuilder;
import io.cdap.cdap.data2.util.hbase.GetBuilder;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.IncrementBuilder;
import io.cdap.cdap.data2.util.hbase.PutBuilder;
import io.cdap.cdap.data2.util.hbase.ScanBuilder;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TxConstants;
import org.apache.tephra.util.TxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataset client for HBase tables.
 */
// todo: do periodic flush when certain threshold is reached
// todo: extract separate "no delete inside tx" table?
// todo: consider writing & reading using HTable to do in multi-threaded way
public class HBaseTable extends BufferingTable {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTable.class);

  private static final String CONFIG_HBASE_CLIENT_SCANNER_CACHING = HConstants.HBASE_CLIENT_SCANNER_CACHING;
  private static final String CONFIG_HBASE_CLIENT_CACHE_BLOCKS = "hbase.client.cache.blocks";

  public static final String DELTA_WRITE = "d";
  public static final String WRITE_POINTER = "wp";
  public static final String TX_MAX_LIFETIME_MILLIS_KEY = "cdap.tx.max.lifetime.millis";
  public static final String TX_ID = "txid";

  public static final String SAFE_INCREMENTS = "dataset.table.safe.readless.increments";

  private final HBaseTableUtil tableUtil;
  private final Table table;
  private final BufferedMutator mutator;
  private final String hTableName;
  private final byte[] columnFamily;
  private final TransactionCodec txCodec;

  // name length + name of the table: handy to have one cached
  private final byte[] nameAsTxChangePrefix;
  private final boolean safeReadlessIncrements;
  // tx max lifetime property comes usually from cConf in DefaultTransactionProcessor but if it is not available
  // briefly during startup, the coprocessor gets it from the operation's attribute.
  private final byte[] txMaxLifetimeMillis;

  private final Map<String, String> arguments;
  private final Map<String, String> properties;

  private byte[] encodedTx;

  public HBaseTable(DatasetContext datasetContext, DatasetSpecification spec,
      Map<String, String> args,
      CConfiguration cConf, Configuration hConf, HBaseTableUtil tableUtil) throws IOException {
    this(datasetContext, spec, args, cConf, hConf, tableUtil, new TransactionCodec());
  }

  @VisibleForTesting
  HBaseTable(DatasetContext datasetContext, DatasetSpecification spec, Map<String, String> args,
      CConfiguration cConf, Configuration hConf, HBaseTableUtil tableUtil, TransactionCodec txCodec)
      throws IOException {
    super(PrefixedNamespaces.namespace(cConf, datasetContext.getNamespaceId(), spec.getName()),
        TableProperties.getReadlessIncrementSupport(spec.getProperties()), spec.getProperties());
    TableId hBaseTableId = tableUtil.createHTableId(
        new NamespaceId(datasetContext.getNamespaceId()), spec.getName());
    this.table = tableUtil.createTable(hConf, hBaseTableId);
    // todo: make configurable
    this.mutator = tableUtil.createBufferedMutator(table, HBaseTableUtil.DEFAULT_WRITE_BUFFER_SIZE);
    this.tableUtil = tableUtil;
    this.hTableName = Bytes.toStringBinary(table.getTableDescriptor().getTableName().getName());
    this.columnFamily = TableProperties.getColumnFamilyBytes(spec.getProperties());
    this.txCodec = txCodec;
    // Overriding the hbase tx change prefix so it resembles the hbase table name more closely, since the HBase
    // table name is not the same as the dataset name anymore
    this.nameAsTxChangePrefix = Bytes.add(new byte[]{(byte) this.hTableName.length()},
        Bytes.toBytes(this.hTableName));
    this.safeReadlessIncrements =
        args.containsKey(SAFE_INCREMENTS) && Boolean.valueOf(args.get(SAFE_INCREMENTS));
    this.txMaxLifetimeMillis = Bytes.toBytes(TimeUnit.SECONDS.toMillis(
        cConf.getInt(TxConstants.Manager.CFG_TX_MAX_LIFETIME,
            TxConstants.Manager.DEFAULT_TX_MAX_LIFETIME)));
    this.arguments = args;
    this.properties = spec.getProperties();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("table", table)
        .add("hTableName", hTableName)
        .add("nameAsTxChangePrefix", nameAsTxChangePrefix)
        .toString();
  }

  private byte[] getEncodedTx() throws IOException {
    if (encodedTx == null) {
      encodedTx = txCodec.encode(tx);
    }
    return encodedTx;
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    // set this to null in case it was left over - for whatever reason - by the previous tx
    encodedTx = null;
  }

  @Override
  public void updateTx(Transaction transaction) {
    super.updateTx(transaction);
    encodedTx = null;
  }

  @Override
  public void postTxCommit() {
    super.postTxCommit();
    // we don't do this on commitTx() because we may still need the tx for rollback
    encodedTx = null;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    boolean success = super.rollbackTx();
    encodedTx = null;
    return success;
  }

  @Override
  protected List<Map<byte[], byte[]>> getPersisted(List<io.cdap.cdap.api.dataset.table.Get> gets) {
    if (gets.isEmpty()) {
      return Collections.emptyList();
    }

    List<Get> hbaseGets = new ArrayList<>();
    for (io.cdap.cdap.api.dataset.table.Get get : gets) {
      List<byte[]> cols = get.getColumns();
      // Our Get class (io.cdap.cdap.api.dataset.table.Get) with empty array means get nothing, but there is no way to
      // specify this in an HBase Get (org.apache.hadoop.hbase.client.Get). That's why we don't call createGet for
      // every get.
      if (cols == null || !cols.isEmpty()) {
        hbaseGets.add(
            createGet(get.getRow(), cols == null ? null : cols.toArray(new byte[cols.size()][])));
      }
    }

    if (hbaseGets.isEmpty()) {
      return Collections.emptyList();
    }

    Result[] hbaseResults = hbaseGet(hbaseGets);

    List<Map<byte[], byte[]>> results = new ArrayList<>(gets.size());
    int hbaseResultsIndex = 0;
    for (io.cdap.cdap.api.dataset.table.Get get : gets) {
      List<byte[]> cols = get.getColumns();
      if (cols == null || !cols.isEmpty()) {
        Result hbaseResult = hbaseResults[hbaseResultsIndex++];
        Map<byte[], byte[]> familyMap = hbaseResult.getFamilyMap(columnFamily);
        results.add(familyMap != null ? familyMap : Collections.emptyMap());
      } else {
        results.add(Collections.emptyMap());
      }
    }

    return results;
  }

  @ReadOnly
  private Result[] hbaseGet(List<Get> gets) {
    try {
      return table.get(gets);
    } catch (IOException ioe) {
      throw new DataSetException("Multi-get failed on table " + hTableName, ioe);
    }
  }

  @Override
  public byte[] getNameAsTxChangePrefix() {
    return nameAsTxChangePrefix;
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      try {
        mutator.close();
      } finally {
        table.close();
      }
    }
  }

  @Override
  protected void persist(NavigableMap<byte[], NavigableMap<byte[], Update>> updates)
      throws Exception {
    if (updates.isEmpty()) {
      return;
    }

    byte[] txId = tx == null ? null : Bytes.toBytes(tx.getTransactionId());
    byte[] txWritePointer = tx == null ? null : Bytes.toBytes(tx.getWritePointer());
    List<Mutation> mutations = new ArrayList<>();
    List<Increment> increments = new ArrayList<>();
    for (Map.Entry<byte[], NavigableMap<byte[], Update>> row : updates.entrySet()) {
      // create these only when they are needed
      PutBuilder put = null;
      PutBuilder incrementPut = null;
      IncrementBuilder increment = null;

      for (Map.Entry<byte[], Update> column : row.getValue().entrySet()) {
        // we want support tx and non-tx modes
        if (tx != null) {
          // TODO: hijacking timestamp... bad
          Update val = column.getValue();
          if (val instanceof IncrementValue) {
            if (safeReadlessIncrements) {
              increment = getIncrement(increment, row.getKey(), txId, txWritePointer);
              increment.add(columnFamily, column.getKey(), tx.getWritePointer(),
                  ((IncrementValue) val).getValue());
            } else {
              incrementPut = getPutForIncrement(incrementPut, row.getKey(), txId);
              incrementPut.add(columnFamily, column.getKey(), tx.getWritePointer(),
                  Bytes.toBytes(((IncrementValue) val).getValue()));
            }
          } else if (val instanceof PutValue) {
            put = getPut(put, row.getKey(), txId);
            put.add(columnFamily, column.getKey(), tx.getWritePointer(),
                wrapDeleteIfNeeded(((PutValue) val).getValue()));
          }
        } else {
          Update val = column.getValue();
          if (val instanceof IncrementValue) {
            incrementPut = getPutForIncrement(incrementPut, row.getKey(), txId);
            incrementPut.add(columnFamily, column.getKey(),
                Bytes.toBytes(((IncrementValue) val).getValue()));
          } else if (val instanceof PutValue) {
            put = getPut(put, row.getKey(), txId);
            put.add(columnFamily, column.getKey(), ((PutValue) val).getValue());
          }
        }
      }
      if (incrementPut != null) {
        mutations.add(incrementPut.build());
      }
      if (increment != null) {
        increments.add(increment.build());
      }
      if (put != null) {
        mutations.add(put.build());
      }
    }
    if (!hbaseFlush(mutations) && increments.isEmpty()) {
      LOG.info("No writes to persist!");
    }
    if (!increments.isEmpty()) {
      table.batch(increments, new Object[increments.size()]);
    }
  }

  @WriteOnly
  private boolean hbaseFlush(List<Mutation> mutations) throws IOException {
    if (!mutations.isEmpty()) {
      mutator.mutate(mutations);
      mutator.flush();
      return true;
    }
    return false;
  }

  private PutBuilder getPut(PutBuilder existing, byte[] row, @Nullable byte[] txId) {
    if (existing != null) {
      return existing;
    }
    PutBuilder putBuilder = tableUtil.buildPut(row);
    if (txId != null) {
      putBuilder.setAttribute(TX_MAX_LIFETIME_MILLIS_KEY, txMaxLifetimeMillis)
          .setAttribute(TX_ID, txId);
    }
    return putBuilder;
  }

  private PutBuilder getPutForIncrement(PutBuilder existing, byte[] row, @Nullable byte[] txId) {
    if (existing != null) {
      return existing;
    }
    return tableUtil.buildPut(row)
        .setAttribute(DELTA_WRITE, Bytes.toBytes(true))
        .setAttribute(TX_MAX_LIFETIME_MILLIS_KEY, txMaxLifetimeMillis)
        .setAttribute(TX_ID, txId);
  }

  private IncrementBuilder getIncrement(IncrementBuilder existing, byte[] row,
      @Nullable byte[] txId,
      @Nullable byte[] txWritePointer)
      throws IOException {
    if (existing != null) {
      return existing;
    }
    IncrementBuilder builder = tableUtil.buildIncrement(row)
        .setAttribute(DELTA_WRITE, Bytes.toBytes(true));
    if (txId != null) {
      builder.setAttribute(TX_MAX_LIFETIME_MILLIS_KEY, txMaxLifetimeMillis);
      builder.setAttribute(WRITE_POINTER, txWritePointer);
      builder.setAttribute(TX_ID, txId);
      builder.setAttribute(TxConstants.TX_OPERATION_ATTRIBUTE_KEY, getEncodedTx());
    }
    return builder;
  }

  /**
   * Returns the transaction id for an operation.
   *
   * @return transaction id or -1 if not a transactional operation
   */
  public static long getTransactionId(OperationWithAttributes op, @Nullable Transaction tx) {
    // CDAP does not encode the transaction into a Put, a Delete or an Increment
    // Instead sets the transaction id of the transaction as HBaseTable.TX_ID attribute
    long txId;
    if (tx == null) {
      byte[] txIdBytes = op.getAttribute(HBaseTable.TX_ID);
      if (txIdBytes == null) {
        return -1;
      }
      txId = Bytes.toLong(txIdBytes);
      if (TxUtils.isPreExistingVersion(txId)) {
        return -1;
      }
    } else {
      txId = tx.getTransactionId();
    }
    return txId;
  }


  @Override
  protected void undo(NavigableMap<byte[], NavigableMap<byte[], Update>> persisted)
      throws Exception {
    if (persisted.isEmpty()) {
      return;
    }

    // NOTE: we use Delete with the write pointer as the specific version to delete.
    List<Delete> deletes = Lists.newArrayList();
    for (Map.Entry<byte[], NavigableMap<byte[], Update>> row : persisted.entrySet()) {
      DeleteBuilder delete = tableUtil.buildDelete(row.getKey());
      delete.setAttribute(TX_MAX_LIFETIME_MILLIS_KEY, txMaxLifetimeMillis);
      for (Map.Entry<byte[], Update> column : row.getValue().entrySet()) {
        // we want support tx and non-tx modes
        if (tx != null) {
          delete.setAttribute(TxConstants.TX_ROLLBACK_ATTRIBUTE_KEY, new byte[0]);
          // TODO: hijacking timestamp... bad
          delete.deleteColumn(columnFamily, column.getKey(), tx.getWritePointer());
        } else {
          delete.deleteColumns(columnFamily, column.getKey());
        }
      }
      deletes.add(delete.build());
    }

    if (!deletes.isEmpty()) {
      hbaseDelete(deletes);
    }
  }

  @WriteOnly
  private void hbaseDelete(List<Delete> deletes) throws IOException {
    mutator.mutate(deletes);
    mutator.flush();
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[] startColumn,
      byte[] stopColumn, int limit) throws Exception {
    // todo: this is very inefficient: column range + limit should be pushed down via server-side filters
    return getRange(getInternal(row, null), startColumn, stopColumn, limit);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, @Nullable byte[][] columns)
      throws Exception {
    return getInternal(row, columns);
  }

  @ReadOnly
  @Override
  protected Scanner scanPersisted(io.cdap.cdap.api.dataset.table.Scan scan) throws Exception {
    ScanBuilder hScan = tableUtil.buildScan();
    hScan.addFamily(columnFamily);

    // TODO (CDAP-11954): use common utility method to extract these configs
    if (scan.getProperties().containsKey(CONFIG_HBASE_CLIENT_CACHE_BLOCKS)) {
      hScan.setCacheBlocks(
          Boolean.valueOf(scan.getProperties().get(CONFIG_HBASE_CLIENT_CACHE_BLOCKS)));
    } else if (arguments.containsKey(CONFIG_HBASE_CLIENT_CACHE_BLOCKS)) {
      hScan.setCacheBlocks(Boolean.valueOf(arguments.get(CONFIG_HBASE_CLIENT_CACHE_BLOCKS)));
    } else if (properties.containsKey(CONFIG_HBASE_CLIENT_CACHE_BLOCKS)) {
      hScan.setCacheBlocks(Boolean.valueOf(properties.get(CONFIG_HBASE_CLIENT_CACHE_BLOCKS)));
    } else {
      // NOTE: by default we assume scanner is used in mapreduce job, hence no cache blocks
      hScan.setCacheBlocks(false);
    }

    if (scan.getProperties().containsKey(CONFIG_HBASE_CLIENT_SCANNER_CACHING)) {
      hScan.setCaching(
          Integer.valueOf(scan.getProperties().get(CONFIG_HBASE_CLIENT_SCANNER_CACHING)));
    } else if (arguments.containsKey(CONFIG_HBASE_CLIENT_SCANNER_CACHING)) {
      hScan.setCaching(Integer.valueOf(arguments.get(CONFIG_HBASE_CLIENT_SCANNER_CACHING)));
    } else if (properties.containsKey(CONFIG_HBASE_CLIENT_SCANNER_CACHING)) {
      hScan.setCaching(Integer.valueOf(properties.get(CONFIG_HBASE_CLIENT_SCANNER_CACHING)));
    } else {
      // NOTE: by default we use this hard-coded value, for backwards-compatibility with CDAP<4.1.2|4.2.1|4.3
      hScan.setCaching(1000);
    }

    byte[] startRow = scan.getStartRow();
    byte[] stopRow = scan.getStopRow();
    if (startRow != null) {
      hScan.setStartRow(startRow);
    }
    if (stopRow != null) {
      hScan.setStopRow(stopRow);
    }

    setFilterIfNeeded(hScan, scan.getFilter());
    hScan.setAttribute(TxConstants.TX_OPERATION_ATTRIBUTE_KEY, getEncodedTx());

    ResultScanner resultScanner = wrapResultScanner(table.getScanner(hScan.build()));
    return new HBaseScanner(resultScanner, columnFamily);
  }

  private void setFilterIfNeeded(ScanBuilder scan, @Nullable Filter filter) {
    if (filter == null) {
      return;
    }

    if (filter instanceof FuzzyRowFilter) {
      FuzzyRowFilter fuzzyRowFilter = (FuzzyRowFilter) filter;
      List<Pair<byte[], byte[]>> fuzzyPairs =
          Lists.newArrayListWithExpectedSize(fuzzyRowFilter.getFuzzyKeysData().size());
      for (ImmutablePair<byte[], byte[]> pair : fuzzyRowFilter.getFuzzyKeysData()) {
        fuzzyPairs.add(Pair.newPair(pair.getFirst(), pair.getSecond()));
      }
      scan.setFilter(new org.apache.hadoop.hbase.filter.FuzzyRowFilter(fuzzyPairs));
    } else {
      throw new IllegalArgumentException("Unsupported filter: " + filter);
    }
  }

  /**
   * Creates an {@link Get} for the specified row and columns.
   *
   * @param row the rowkey for the Get
   * @param columns the columns to fetch. null means to retrieve all columns.
   * @throws IllegalArgumentException if columns has length 0.
   */
  private Get createGet(byte[] row, @Nullable byte[][] columns) {
    Preconditions.checkArgument(columns == null || columns.length != 0);
    GetBuilder get = tableUtil.buildGet(row);
    get.addFamily(columnFamily);
    if (columns != null) {
      for (byte[] column : columns) {
        get.addColumn(columnFamily, column);
      }
    } else {
      get.addFamily(columnFamily);
    }

    try {
      // no tx logic needed
      if (tx == null) {
        get.setMaxVersions(1);
      } else {
        get.setAttribute(TxConstants.TX_OPERATION_ATTRIBUTE_KEY, getEncodedTx());
      }
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
    return get.build();
  }

  // columns being null means to get all rows; empty columns means get no rows.
  @ReadOnly
  private NavigableMap<byte[], byte[]> getInternal(byte[] row, @Nullable byte[][] columns)
      throws IOException {
    if (columns != null && columns.length == 0) {
      return EMPTY_ROW_MAP;
    }

    Get get = createGet(row, columns);

    Result result = table.get(get);

    // no tx logic needed
    if (tx == null) {
      return result.isEmpty() ? EMPTY_ROW_MAP : result.getFamilyMap(columnFamily);
    }

    return getRowMap(result, columnFamily);
  }

  static NavigableMap<byte[], byte[]> getRowMap(Result result, byte[] columnFamily) {
    if (result.isEmpty()) {
      return EMPTY_ROW_MAP;
    }

    // note: server-side filters all everything apart latest visible for us, so we can flatten it here
    NavigableMap<byte[], NavigableMap<Long, byte[]>> versioned =
        result.getMap().get(columnFamily);

    NavigableMap<byte[], byte[]> rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> column : versioned.entrySet()) {
      rowMap.put(column.getKey(), column.getValue().firstEntry().getValue());
    }

    return unwrapDeletes(rowMap);
  }

  // The following methods assist the Dataset authorization when the ResultScanner is used.

  @ReadOnly
  private Result next(ResultScanner scanner) throws IOException {
    return scanner.next();
  }

  @ReadOnly
  private Result[] next(ResultScanner scanner, int nbRows) throws IOException {
    return scanner.next(nbRows);
  }

  @ReadOnly
  private <T> boolean hasNext(Iterator<T> iterator) {
    return iterator.hasNext();
  }

  @ReadOnly
  private <T> T next(Iterator<T> iterator) {
    return iterator.next();
  }

  private ResultScanner wrapResultScanner(final ResultScanner scanner) {
    return new ResultScanner() {
      @Override
      public Result next() throws IOException {
        return HBaseTable.this.next(scanner);
      }

      @Override
      public Result[] next(int nbRows) throws IOException {
        return HBaseTable.this.next(scanner, nbRows);
      }

      @Override
      public void close() {
        scanner.close();
      }

      @Override
      public Iterator<Result> iterator() {
        final Iterator<Result> iterator = scanner.iterator();
        return new AbstractIterator<Result>() {
          @Override
          protected Result computeNext() {
            return HBaseTable.this.hasNext(iterator) ? HBaseTable.this.next(iterator) : endOfData();
          }
        };
      }
    };
  }
}
