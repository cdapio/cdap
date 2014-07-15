/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction.distributed;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.distributed.thrift.TTransaction;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Utility methods to convert to thrift and back.
 */
final class ConverterUtils {

  public static TTransaction wrap(Transaction tx) {
    List<Long> invalids = Lists.newArrayListWithCapacity(tx.getInvalids().length);
    for (long txid : tx.getInvalids()) {
      invalids.add(txid);
    }
    List<Long> inProgress = Lists.newArrayListWithCapacity(tx.getInProgress().length);
    for (long txid : tx.getInProgress()) {
      inProgress.add(txid);
    }
    return new TTransaction(tx.getWritePointer(), tx.getReadPointer(),
                             invalids, inProgress, tx.getFirstShortInProgress());
  }

  public static Transaction unwrap(TTransaction tx) {
    long[] invalids = new long[tx.invalids.size()];
    int i = 0;
    for (Long txid : tx.invalids) {
      invalids[i++] = txid;
    }
    long[] inProgress = new long[tx.inProgress.size()];
    i = 0;
    for (Long txid : tx.inProgress) {
      inProgress[i++] = txid;
    }
    return new Transaction(tx.readPointer, tx.writePointer,
                                                             invalids, inProgress, tx.getFirstShort());
  }
}
