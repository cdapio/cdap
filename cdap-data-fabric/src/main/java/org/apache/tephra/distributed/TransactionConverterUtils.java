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

package org.apache.tephra.distributed;

import com.google.common.primitives.Longs;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionType;
import org.apache.tephra.distributed.thrift.TTransaction;
import org.apache.tephra.distributed.thrift.TTransactionType;
import org.apache.tephra.distributed.thrift.TVisibilityLevel;

/**
 * Utility methods to convert to thrift and back.
 */
public final class TransactionConverterUtils {
  private static final long[] EMPTY_LONG_ARRAY = {};

  public static TTransaction wrap(Transaction tx) {
    return new TTransaction(tx.getTransactionId(), tx.getReadPointer(),
                            Longs.asList(tx.getInvalids()), Longs.asList(tx.getInProgress()),
                            tx.getFirstShortInProgress(), getTTransactionType(tx.getType()),
                            tx.getWritePointer(), Longs.asList(tx.getCheckpointWritePointers()),
                            getTVisibilityLevel(tx.getVisibilityLevel()));
  }

  public static Transaction unwrap(TTransaction thriftTx) {
    return new Transaction(thriftTx.getReadPointer(), thriftTx.getTransactionId(), thriftTx.getWritePointer(),
                           thriftTx.getInvalids() == null ? EMPTY_LONG_ARRAY : Longs.toArray(thriftTx.getInvalids()),
                           thriftTx.getInProgress() == null ? EMPTY_LONG_ARRAY :
                               Longs.toArray(thriftTx.getInProgress()),
                           thriftTx.getFirstShort(), getTransactionType(thriftTx.getType()),
                           thriftTx.getCheckpointWritePointers() == null ? EMPTY_LONG_ARRAY :
                               Longs.toArray(thriftTx.getCheckpointWritePointers()),
                           getVisibilityLevel(thriftTx.getVisibilityLevel()));
  }

  private static TransactionType getTransactionType(TTransactionType tType) {
    return tType == TTransactionType.SHORT ? TransactionType.SHORT : TransactionType.LONG;
  }

  private static TTransactionType getTTransactionType(TransactionType type) {
    return type == TransactionType.SHORT ? TTransactionType.SHORT : TTransactionType.LONG;
  }

  private static Transaction.VisibilityLevel getVisibilityLevel(TVisibilityLevel tLevel) {
    // default to SNAPSHOT
    if (tLevel == null) {
      return Transaction.VisibilityLevel.SNAPSHOT;
    }

    switch (tLevel) {
      case SNAPSHOT:
        return Transaction.VisibilityLevel.SNAPSHOT;
      case SNAPSHOT_EXCLUDE_CURRENT:
        return Transaction.VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT;
      case SNAPSHOT_ALL:
        return Transaction.VisibilityLevel.SNAPSHOT_ALL;
      default:
        throw new IllegalArgumentException("Unknown TVisibilityLevel: " + tLevel);
    }
  }

  private static TVisibilityLevel getTVisibilityLevel(Transaction.VisibilityLevel level) {
    switch (level) {
      case SNAPSHOT:
        return TVisibilityLevel.SNAPSHOT;
      case SNAPSHOT_EXCLUDE_CURRENT:
        return TVisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT;
      case SNAPSHOT_ALL:
        return TVisibilityLevel.SNAPSHOT_ALL;
      default:
        throw new IllegalArgumentException("Unknown VisibilityLevel: " + level);
    }
  }
}
