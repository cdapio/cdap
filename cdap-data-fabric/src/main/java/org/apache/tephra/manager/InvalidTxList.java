/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tephra.manager;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import org.apache.tephra.TransactionManager;

import java.util.Arrays;
import java.util.Collection;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This is an internal class used by the {@link TransactionManager} to store invalid transaction ids.
 * This class uses both a list and an array to keep track of the invalid ids. The list is the primary
 * data structure for storing the invalid ids. The array is populated lazily on changes to the list.
 * The array is used to avoid creating a new array every time method {@link #toSortedArray()} is invoked.
 *
 * This class is not thread safe and relies on external synchronization. TransactionManager always
 * accesses an instance of this class after synchronization.
 */
@NotThreadSafe
public class InvalidTxList {
  private static final long[] NO_INVALID_TX = { };

  private final LongList invalid = new LongArrayList();
  private long[] invalidArray = NO_INVALID_TX;

  private boolean dirty = false; // used to track changes to the invalid list

  public int size() {
    return invalid.size();
  }

  public boolean isEmpty() {
    return invalid.isEmpty();
  }

  public boolean add(long id) {
    boolean changed = invalid.add(id);
    dirty = dirty || changed;
    return changed;
  }

  public boolean addAll(Collection<? extends Long> ids) {
    boolean changed = invalid.addAll(ids);
    dirty = dirty || changed;
    return changed;
  }

  public boolean addAll(LongList ids) {
    boolean changed = invalid.addAll(ids);
    dirty = dirty || changed;
    return changed;
  }

  public boolean contains(long id) {
    return invalid.contains(id);
  }

  public boolean remove(long id) {
    boolean changed = invalid.rem(id);
    dirty = dirty || changed;
    return changed;
  }

  public boolean removeAll(Collection<? extends Long> ids) {
    boolean changed = invalid.removeAll(ids);
    dirty = dirty || changed;
    return changed;
  }

  @SuppressWarnings("WeakerAccess")
  public boolean removeAll(LongList ids) {
    boolean changed = invalid.removeAll(ids);
    dirty = dirty || changed;
    return changed;
  }

  public void clear() {
    invalid.clear();
    invalidArray = NO_INVALID_TX;
    dirty = false;
  }

  /**
   * @return sorted array of invalid transactions
   */
  public long[] toSortedArray() {
    lazyUpdate();
    return invalidArray;
  }

  /**
   * @return list of invalid transactions. The list is not sorted.
   */
  public LongList toRawList() {
    return LongLists.unmodifiable(invalid);
  }

  private void lazyUpdate() {
    if (dirty) {
      invalidArray = invalid.toLongArray();
      Arrays.sort(invalidArray);
      dirty = false;
    }
  }
}
