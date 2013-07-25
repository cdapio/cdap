package com.continuuity.data2.tx.inmemory;

import com.continuuity.data2.tx.Tx;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 */
// todo: synchronize all
public class InMemoryOracle {
  private static List<Long> excludedList = Lists.newArrayList();

  private static long readPointer = 0;

  private static long nextWritePointer = 1;

  public static Tx start() {
    return new Tx(readPointer, nextWritePointer++, getExcludedListAsArray(excludedList));
  }

  public static boolean canCommit(Tx tx, Collection<byte[]> changeIds) {
    // todo: implement
    return true;
  }

  public static boolean commit(Tx tx) {
    excludedList.remove(new Long(tx.getWritePointer()));
    // todo: use heap or smth to optimize
    readPointer = excludedList.isEmpty() ? nextWritePointer - 1 : Collections.min(excludedList);
    return true;
  }

  public static boolean abort(Tx tx) {
    // has same affect: makes tx visible (assumes that all operations were rolled back)
    return commit(tx);
  }

  private static long[] getExcludedListAsArray(List<Long> excludedList) {
    // todo: optimize (cache, etc. etc.)
    long[] result = new long[excludedList.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = excludedList.get(i);
    }

    return result;
  }

}
