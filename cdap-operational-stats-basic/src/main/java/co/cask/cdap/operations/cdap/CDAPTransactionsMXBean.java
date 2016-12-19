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

package co.cask.cdap.operations.cdap;

import javax.management.MXBean;

/**
 * {@link MXBean} for reporting CDAP transaction stats.
 */
public interface CDAPTransactionsMXBean {

  /**
   * Returns the time when the transaction snapshot was taken.
   */
  long getSnapshotTime();

  /**
   * Returns the read pointer at the time of the snapshot.
   */
  long getReadPointer();

  /**
   * Returns the next write pointer at the time of the snapshot.
   */
  long getWritePointer();

  /**
   * Returns the number of transactions that are currently in progress.
   */
  int getNumInProgressTransactions();

  /**
   * Returns the size of the invalid transaction list.
   */
  int getNumInvalidTransactions();

  /**
   * Returns the number of committing change sets (that have not yet been committed) across all write pointers.
   */
  int getNumCommittingChangeSets();

  /**
   * Returns the number of committed change sets across all write pointers.
   */
  int getNumCommittedChangeSets();

  /**
   * Returns a transaction id {@code X} such that any of the transactions newer than {@code X} might be invisible to
   * some of the currently in-progress transactions or to those that will be started
   * NOTE: the returned tx id can be invalid.
   */
  long getVisibilityUpperBound();
}
