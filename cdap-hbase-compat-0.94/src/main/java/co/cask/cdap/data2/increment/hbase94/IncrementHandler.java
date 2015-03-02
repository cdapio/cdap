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

package co.cask.cdap.data2.increment.hbase94;

import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTable;
import co.cask.cdap.data2.increment.hbase.IncrementHandlerState;
import co.cask.cdap.data2.increment.hbase.TimestampOracle;
import co.cask.cdap.data2.util.hbase.HBase94TableUtil;
import co.cask.tephra.hbase94.Filters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * HBase coprocessor that handles reading and writing read-less increment operations.
 *
 * <p>Writes of incremental values are performed as normal {@code Put}s, flagged with a special attribute
 * {@link HBaseTable#DELTA_WRITE}.  The coprocessor intercepts these
 * writes and rewrites the cell value to use a special marker prefix.</p>
 *
 * <p>For read (for {@code Get} and {@code Scan}) operations, all of the delta values are summed up for a column,
 * up to and including the most recent "full" (non-delta) value.  The sum of these delta values, plus the full value
 * (if found) is returned for the column.</p>
 *
 * <p>To mitigate the performance impact on reading, this coprocessor also overrides the scanner used in flush and
 * compaction operations, using {@link IncrementSummingScanner} to generate a new "full" value aggregated from
 * all the successfully committed delta values.</p>
 */
public class IncrementHandler extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(IncrementHandler.class);

  private HRegion region;
  private IncrementHandlerState state;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      this.region = ((RegionCoprocessorEnvironment) e).getRegion();
      this.state = new IncrementHandlerState(env.getConfiguration(), env.getRegion().getTableDesc().getNameAsString(),
                                             new HBase94TableUtil());

      HTableDescriptor tableDesc = env.getRegion().getTableDesc();
      for (HColumnDescriptor columnDesc : tableDesc.getFamilies()) {
        state.initFamily(columnDesc.getName(), convertFamilyValues(columnDesc.getValues()));
      }
    }
  }

  @VisibleForTesting
  public void setTimestampOracle(TimestampOracle timeOracle) {
    state.setTimestampOracle(timeOracle);
  }

  private Map<byte[], byte[]> convertFamilyValues(Map<ImmutableBytesWritable, ImmutableBytesWritable> writableValues) {
    Map<byte[], byte[]> converted = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : writableValues.entrySet()) {
      converted.put(e.getKey().get(), e.getValue().get());
    }
    return converted;
  }

  @Override
  public void preGet(ObserverContext<RegionCoprocessorEnvironment> ctx, Get get, List<KeyValue> results)
    throws IOException {
    Scan scan = new Scan(get);
    scan.setMaxVersions();
    scan.setFilter(Filters.combine(new IncrementFilter(), scan.getFilter()));
    RegionScanner scanner = null;
    try {
      scanner = new IncrementSummingScanner(region, scan.getBatch(), region.getScanner(scan), ScanType.USER_SCAN);
      scanner.next(results);
      ctx.bypass();
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, boolean writeToWAL)
    throws IOException {
    // we assume that if any of the column families written to are transactional, the entire write is transactional
    boolean transactional = state.containsTransactionalFamily(put.getFamilyMap().keySet());
    boolean isIncrement = put.getAttribute(HBaseTable.DELTA_WRITE) != null;

    if (isIncrement || !transactional) {
      // incremental write
      NavigableMap<byte[], List<KeyValue>> newFamilyMap = new TreeMap<byte[], List<KeyValue>>(Bytes.BYTES_COMPARATOR);

      long tsToAssign = 0;
      if (!transactional) {
        tsToAssign = state.getUniqueTimestamp();
      }
      for (Map.Entry<byte[], List<KeyValue>> entry : put.getFamilyMap().entrySet()) {
        List<KeyValue> newCells = new ArrayList<KeyValue>(entry.getValue().size());
        for (KeyValue kv : entry.getValue()) {
          // rewrite the cell value with a special prefix to identify it as a delta
          // for 0.98 we can update this to use cell tags
          byte[] newValue = isIncrement ? Bytes.add(IncrementHandlerState.DELTA_MAGIC_PREFIX, kv.getValue()) :
              kv.getValue();
          newCells.add(new KeyValue(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
              kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
              kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
              transactional ? kv.getTimestamp() : tsToAssign,
              KeyValue.Type.Put,
              newValue, 0, newValue.length));
        }
        newFamilyMap.put(entry.getKey(), newCells);
      }
      put.setFamilyMap(newFamilyMap);
    }
    // put completes normally with value prefix marker
  }

  @Override
  public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
                        boolean writeToWAL) throws IOException {
    boolean transactional = state.containsTransactionalFamily(delete.getFamilyMap().keySet());
    if (!transactional) {
      long tsToAssign = state.getUniqueTimestamp();
      delete.setTimestamp(tsToAssign);
      // new key values
      NavigableMap<byte[], List<KeyValue>> newFamilyMap = new TreeMap<byte[], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
      for (Map.Entry<byte[], List<KeyValue>> entry : delete.getFamilyMap().entrySet()) {
        List<KeyValue> newCells = new ArrayList<KeyValue>(entry.getValue().size());
        for (KeyValue kv : entry.getValue()) {
          // replace the timestamp
          newCells.add(new KeyValue(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
              kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
              kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
              tsToAssign, KeyValue.Type.codeToType(kv.getType()),
              kv.getBuffer(), kv.getValueOffset(), kv.getValueLength()));
        }
        newFamilyMap.put(entry.getKey(), newCells);
      }
      delete.setFamilyMap(newFamilyMap);
    }
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    // must see all versions to aggregate increments
    scan.setMaxVersions();
    scan.setFilter(Filters.combine(new IncrementFilter(), scan.getFilter()));
    return s;
  }

  @Override
  public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Scan scan,
                                       RegionScanner scanner)
    throws IOException {
    return new IncrementSummingScanner(region, scan.getBatch(), scanner, ScanType.USER_SCAN);
  }

  @Override
  public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                  InternalScanner scanner) throws IOException {
    byte[] family = store.getFamily().getName();
    return new IncrementSummingScanner(region, IncrementHandlerState.BATCH_UNLIMITED, scanner,
        ScanType.MINOR_COMPACT, state.getCompactionBound(family), state.getOldestVisibleTimestamp(family));
  }

  public static boolean isIncrement(KeyValue kv) {
    return !KeyValue.isDelete(kv.getType()) && kv.getValueLength() == IncrementHandlerState.DELTA_FULL_LENGTH &&
      Bytes.equals(kv.getBuffer(), kv.getValueOffset(), IncrementHandlerState.DELTA_MAGIC_PREFIX.length,
                   IncrementHandlerState.DELTA_MAGIC_PREFIX, 0, IncrementHandlerState.DELTA_MAGIC_PREFIX.length);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                    InternalScanner scanner) throws IOException {
    byte[] family = store.getFamily().getName();
    return new IncrementSummingScanner(region, IncrementHandlerState.BATCH_UNLIMITED, scanner, ScanType.MINOR_COMPACT,
        state.getCompactionBound(family), state.getOldestVisibleTimestamp(family));
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                    InternalScanner scanner, CompactionRequest request)
    throws IOException {
    byte[] family = store.getFamily().getName();
    return new IncrementSummingScanner(region, IncrementHandlerState.BATCH_UNLIMITED, scanner,
                                       request.isMajor() ? ScanType.MAJOR_COMPACT : ScanType.MINOR_COMPACT,
                                       state.getCompactionBound(family), state.getOldestVisibleTimestamp(family));
  }
}
