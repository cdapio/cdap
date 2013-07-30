package com.continuuity.data2.transaction.inmemory;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
// todo: synchronize all
// todo: optimize heavily
public class InMemoryTransactionOracle {
  private static LongList excludedList;
//  private static List<Long> excludedList;

  // todo: clean it up
  // todo: use moving array instead
  // tx id (write pointer) -> changes made by this tx
  private static Map<Long, Set<byte[]>> committedChangeSets;
  // not committed yet
  private static Map<Long, Set<byte[]>> committingChangeSets;

  private static long readPointer;

  private static long nextWritePointer;

  // todo: do not use static fields, use proper singleton or smth else
  static {
    reset();
  }

  // public for unit-tests
  public static synchronized void reset() {
//    excludedList = Lists.newArrayList();
    excludedList = new LongArrayList();
    committedChangeSets = Maps.newHashMap();
    committingChangeSets = Maps.newHashMap();
    readPointer = 0;
    nextWritePointer = 1;
  }

  public static synchronized Transaction start() {
    Transaction tx = new Transaction(readPointer, nextWritePointer, getExcludedListAsArray(excludedList));
    excludedList.add(nextWritePointer);
    Collections.sort(excludedList);
    nextWritePointer++;
    return tx;
  }

  public static synchronized boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    if (hasConflicts(tx, changeIds)) {
      return false;
    }

    Set<byte[]> set = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
    set.addAll(changeIds);
    committingChangeSets.put(tx.getWritePointer(), set);

    return true;
  }

  public static synchronized boolean commit(Transaction tx) {
    // todo: these should be atomic
    // NOTE: whether we succeed or not we don't need to keep changes in committing state: same tx cannot be attempted to
    //       commit twice
    Set<byte[]> changeSet = committingChangeSets.remove(tx.getWritePointer());

    if (changeSet != null) {
      // double-checking if there are conflicts: someone may have committed since canCommit check
      if (hasConflicts(tx, changeSet)) {
        return false;
      }

      committedChangeSets.put(tx.getWritePointer(), changeSet);
    }
    makeVisible(tx);
    return true;
  }

  public static synchronized boolean abort(Transaction tx) {
    committingChangeSets.remove(tx.getWritePointer());
    // makes tx visible (assumes that all operations were rolled back)
    makeVisible(tx);
    return true;
  }

  private static boolean hasConflicts(Transaction tx, Collection<byte[]> changeIds) {
    if (changeIds.size() == 0) {
      return false;
    }

    // Go thru all tx committed after given tx was started and check if any of them has change
    // conflicting with the given
    return hasConflicts(tx, changeIds, committedChangeSets);

    // NOTE: we could try to optimize for some use-cases and also check those being committed for conflicts to
    //       avoid later the cost of rollback. This is very complex, but the cost of rollback is so high that we
    //       can go a bit crazy optimizing stuff around it...
  }

  private static boolean hasConflicts(Transaction tx, Collection<byte[]> changeIds, Map<Long, Set<byte[]>> changeSets) {
    for (Map.Entry<Long, Set<byte[]>> changeSet : changeSets.entrySet()) {
      // basically not visible but committed means "tx committed after given tx was started"
      if (!visible(tx, changeSet.getKey())) {
        if (containsAny(changeSet.getValue(), changeIds)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean containsAny(Set<byte[]> set, Collection<byte[]> toSearch) {
    for (byte[] item : toSearch) {
      if (set.contains(item)) {
        return true;
      }
    }

    return false;
  }

  // todo: move to Tx?
  private static boolean visible(Transaction tx, long pointer) {
    if (pointer > tx.getReadPointer()) {
      return false;
    }

    // todo: optimize heavily
    // we rely on array of excludes to be sorted
    return Arrays.binarySearch(tx.getExcludedList(), pointer) < 0;
  }

  private static void makeVisible(Transaction tx) {
    excludedList.remove(new Long(tx.getWritePointer()));

    // moving read pointer
    moveReadPointerIfNeeded(tx.getWritePointer());
  }

  private static void moveReadPointerIfNeeded(long committedWritePointer) {
    if (committedWritePointer > readPointer) {
      readPointer = committedWritePointer;
    }
  }

  private static long[] getExcludedListAsArray(LongList excludedList) {
    // todo: optimize (cache, etc. etc.)
    long[] result = new long[excludedList.size()];
    return excludedList.toArray(result);
//    for (int i = 0; i < result.length; i++) {
//      result[i] = excludedList.get(i);
//    }
//
//    return result;
  }

}
