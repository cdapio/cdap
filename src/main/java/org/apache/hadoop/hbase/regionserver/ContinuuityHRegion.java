//package org.apache.hadoop.hbase.regionserver;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.NavigableMap;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.DoNotRetryIOException;
//import org.apache.hadoop.hbase.HConstants;
//import org.apache.hadoop.hbase.HRegionInfo;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.Increment;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Row;
//import org.apache.hadoop.hbase.client.RowLock;
//import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
//import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
//import org.apache.hadoop.hbase.io.TimeRange;
//import org.apache.hadoop.hbase.regionserver.wal.HLog;
//import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
//import org.apache.hadoop.io.Writable;
//
///**
// * Custom HBase HRegion class that extends a normal HBase region with  
// * @author jgray
// *
// */
//public class ContinuuityHRegion extends HRegion {
//
//  public ContinuuityHRegion(final Path path, final HLog hlog,
//      final FileSystem fs, Configuration conf, HRegionInfo hri,
//      HTableDescriptor htd, RegionServerServices rss) {
//    super(path, hlog, fs, conf, hri, htd, rss);
//  }
//
//  /**
//   *
//   * @param row
//   * @param family
//   * @param qualifier
//   * @param compareOp
//   * @param comparator
//   * @param w 
//   * @param lockId
//   * @param writeToWAL
//   * @param readVersion 
//   * @param writeVersion 
//   * @throws IOException
//   * @return true if the new put was execute, false otherwise
//   */
//  public boolean checkAndMutate(byte [] row, byte [] family, byte [] qualifier,
//      CompareOp compareOp, WritableByteArrayComparable comparator, Writable w,
//      Integer lockId, boolean writeToWAL,
//      long readVersion, long writeVersion)
//  throws IOException{
//    checkReadOnly();
//    //TODO, add check for value length or maybe even better move this to the
//    //client if this becomes a global setting
//    checkResources();
//    boolean isPut = w instanceof Put;
//    if (!isPut && !(w instanceof Delete))
//      throw new DoNotRetryIOException("Action must be Put or Delete");
//    Row r = (Row)w;
//    if (!Bytes.equals(row, r.getRow())) {
//      throw new DoNotRetryIOException("Action's getRow must match the passed row");
//    }
//
//    startRegionOperation();
//    this.writeRequestsCount.increment();
//    try {
//      RowLock lock = isPut ? ((Put)w).getRowLock() : ((Delete)w).getRowLock();
//      Get get = new Get(row, lock);
//      checkFamily(family);
//      get.addColumn(family, qualifier);
//      get.setTimeRange(0L, readVersion + 1);
//
//      // Lock row
//      Integer lid = getLock(lockId, get.getRow(), true);
//      List<KeyValue> result = new ArrayList<KeyValue>();
//      try {
//        result = get(get, false);
//
//        boolean valueIsNull = comparator.getValue() == null ||
//          comparator.getValue().length == 0;
//        boolean matches = false;
//        if (result.size() == 0 && valueIsNull) {
//          matches = true;
//        } else if (result.size() > 0 && result.get(0).getValue().length == 0 &&
//            valueIsNull) {
//          matches = true;
//        } else if (result.size() == 1 && !valueIsNull) {
//          KeyValue kv = result.get(0);
//          int compareResult = comparator.compareTo(kv.getBuffer(),
//              kv.getValueOffset(), kv.getValueLength());
//          switch (compareOp) {
//          case LESS:
//            matches = compareResult <= 0;
//            break;
//          case LESS_OR_EQUAL:
//            matches = compareResult < 0;
//            break;
//          case EQUAL:
//            matches = compareResult == 0;
//            break;
//          case NOT_EQUAL:
//            matches = compareResult != 0;
//            break;
//          case GREATER_OR_EQUAL:
//            matches = compareResult > 0;
//            break;
//          case GREATER:
//            matches = compareResult >= 0;
//            break;
//          default:
//            throw new RuntimeException("Unknown Compare op " + compareOp.name());
//          }
//        }
//        //If matches put the new put or delete the new delete
//        if (matches) {
//          // All edits for the given row (across all column families) must
//          // happen atomically.
//          //
//          // Using default cluster id, as this can only happen in the
//          // originating cluster. A slave cluster receives the result as a Put
//          // or Delete
//          if (isPut) {
//            internalPut(((Put) w), HConstants.DEFAULT_CLUSTER_ID, writeToWAL);
//          } else {
//            Delete d = (Delete)w;
//            prepareDelete(d);
//            internalDelete(d, HConstants.DEFAULT_CLUSTER_ID, writeToWAL);
//          }
//          return true;
//        }
//        return false;
//      } finally {
//        if(lockId == null) releaseRowLock(lid);
//      }
//    } finally {
//      closeRegionOperation();
//    }
//  }
//  /**
//  *
//  * Perform one or more increment operations on a row.
//  * <p>
//  * Increments performed are done under row lock but reads do not take locks
//  * out so this can be seen partially complete by gets and scans.
//  * @param increment
//  * @param lockid
//  * @param writeToWAL
//  * @return new keyvalues after increment
//  * @throws IOException
//  */
// public Result increment(Increment increment, Integer lockid,
//     boolean writeToWAL, long readVersion, long writeVersion)
// throws IOException {
//   // TODO: Use MVCC to make this set of increments atomic to reads
//   byte [] row = increment.getRow();
//   checkRow(row, "increment");
//   TimeRange tr = increment.getTimeRange();
//   boolean flush = false;
//   WALEdit walEdits = null;
//   List<KeyValue> allKVs = new ArrayList<KeyValue>(increment.numColumns());
//   Map<Store, List<KeyValue>> tempMemstore = new HashMap<Store, List<KeyValue>>();
//   long before = EnvironmentEdgeManager.currentTimeMillis();
//   long size = 0;
//   long txid = 0;
//
//   // Lock row
//   startRegionOperation();
//   this.writeRequestsCount.increment();
//   try {
//     Integer lid = getLock(lockid, row, true);
//     this.updatesLock.readLock().lock();
//     try {
//       long now = EnvironmentEdgeManager.currentTimeMillis();
//       // Process each family
//       for (Map.Entry<byte [], NavigableMap<byte [], Long>> family :
//         increment.getFamilyMap().entrySet()) {
//
//         Store store = stores.get(family.getKey());
//         List<KeyValue> kvs = new ArrayList<KeyValue>(family.getValue().size());
//
//         // Get previous values for all columns in this family
//         Get get = new Get(row);
//         for (Map.Entry<byte [], Long> column : family.getValue().entrySet()) {
//           get.addColumn(family.getKey(), column.getKey());
//         }
//         get.setTimeRange(tr.getMin(), tr.getMax());
//         List<KeyValue> results = get(get, false);
//
//         // Iterate the input columns and update existing values if they were
//         // found, otherwise add new column initialized to the increment amount
//         int idx = 0;
//         for (Map.Entry<byte [], Long> column : family.getValue().entrySet()) {
//           long amount = column.getValue();
//           if (idx < results.size() &&
//               results.get(idx).matchingQualifier(column.getKey())) {
//             KeyValue kv = results.get(idx);
//             amount += Bytes.toLong(kv.getBuffer(), kv.getValueOffset());
//             idx++;
//           }
//
//           // Append new incremented KeyValue to list
//           KeyValue newKV = new KeyValue(row, family.getKey(), column.getKey(),
//               now, Bytes.toBytes(amount));
//           kvs.add(newKV);
//
//           // Append update to WAL
//           if (writeToWAL) {
//             if (walEdits == null) {
//               walEdits = new WALEdit();
//             }
//             walEdits.add(newKV);
//           }
//         }
//
//         //store the kvs to the temporary memstore before writing HLog
//         tempMemstore.put(store, kvs);
//       }
//
//       // Actually write to WAL now
//       if (writeToWAL) {
//         // Using default cluster id, as this can only happen in the orginating
//         // cluster. A slave cluster receives the final value (not the delta)
//         // as a Put.
//         txid = this.log.appendNoSync(regionInfo, this.htableDescriptor.getName(),
//             walEdits, HConstants.DEFAULT_CLUSTER_ID, EnvironmentEdgeManager.currentTimeMillis(),
//             this.htableDescriptor);
//       }
//
//       //Actually write to Memstore now
//       for (Map.Entry<Store, List<KeyValue>> entry : tempMemstore.entrySet()) {
//         Store store = entry.getKey();
//         size += store.upsert(entry.getValue());
//         allKVs.addAll(entry.getValue());
//       }
//       size = this.addAndGetGlobalMemstoreSize(size);
//       flush = isFlushSize(size);
//     } finally {
//       this.updatesLock.readLock().unlock();
//       releaseRowLock(lid);
//     }
//     if (writeToWAL) {
//       this.log.sync(txid); // sync the transaction log outside the rowlock
//     }
//   } finally {
//     closeRegionOperation();
//   }
//   
//   long after = EnvironmentEdgeManager.currentTimeMillis();
//   this.opMetrics.updateIncrementMetrics(increment.getFamilyMap().keySet(), after - before);
//
//   if (flush) {
//     // Request a cache flush.  Do it outside update lock.
//     requestFlush();
//   }
//
//   return new Result(allKVs);
// }
//
//}
