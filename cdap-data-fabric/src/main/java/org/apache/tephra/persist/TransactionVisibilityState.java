/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.persist;

import org.apache.tephra.TransactionManager;

import java.util.Collection;
import java.util.NavigableMap;

/**
 * Transaction Visibility state contains information required by TransactionProcessor CoProcessor
 * to determine cell visibility.
 */
public interface TransactionVisibilityState {

  /**
   * Returns the timestamp from when this snapshot was created.
   */
  long getTimestamp();

  /**
   * Returns the read pointer at the time of the snapshot.
   */
  long getReadPointer();

  /**
   * Returns the next write pointer at the time of the snapshot.
   */
  long getWritePointer();

  /**
   * Returns the list of invalid write pointers at the time of the snapshot.
   */
  Collection<Long> getInvalid();

  /**
   * Returns the map of write pointers to in-progress transactions at the time of the snapshot.
   */
  NavigableMap<Long, TransactionManager.InProgressTx> getInProgress();

  /**
   * @return transaction id {@code X} such that any of the transactions newer than {@code X} might be invisible to
   *         some of the currently in-progress transactions or to those that will be started <p>
   *         NOTE: the returned tx id can be invalid.
   */
  long getVisibilityUpperBound();
}
