/*
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
*/

namespace java org.apache.tephra.distributed.thrift

enum TTransactionType {
  SHORT = 1,
  LONG = 2
}

enum TVisibilityLevel {
  SNAPSHOT = 1,
  SNAPSHOT_EXCLUDE_CURRENT = 2,
  SNAPSHOT_ALL = 3
}

struct TTransaction {
  1: i64 transactionId,
  2: i64 readPointer,
  3: list<i64> invalids,
  4: list<i64> inProgress,
  5: i64 firstShort,
  6: TTransactionType type,
  7: i64 writePointer,
  8: list<i64> checkpointWritePointers,
  9: TVisibilityLevel visibilityLevel
}

exception TTransactionNotInProgressException {
  1: string message
}

exception TTransactionCouldNotTakeSnapshotException {
  1: string message
}

exception TInvalidTruncateTimeException {
  1: string message
}

exception TGenericException {
  1: string message,
  2: string originalExceptionClass
}

# workaround for THRIFT-1474
struct TBoolean {
  1: bool value
}

service TTransactionServer {
  // temporary tx2 stuff
  TTransaction startLong(),
  TTransaction startShort(),
  // TODO remove this as it was replaced with startShortWithTimeout in 0.10
  TTransaction startShortTimeout(1: i32 timeout),
  TTransaction startShortWithTimeout(1: i32 timeout) throws (1:TGenericException e),
  TBoolean canCommitTx(1: TTransaction tx, 2: set<binary> changes) throws (1:TTransactionNotInProgressException e),
  TBoolean commitTx(1: TTransaction tx) throws (1:TTransactionNotInProgressException e),
  void abortTx(1: TTransaction tx),
  bool invalidateTx(1: i64 tx),
  binary getSnapshot() throws (1:TTransactionCouldNotTakeSnapshotException e),
  void resetState(),
  string status(),
  TBoolean truncateInvalidTx(1: set<i64> txns),
  TBoolean truncateInvalidTxBefore(1: i64 time) throws (1: TInvalidTruncateTimeException e),
  i32 invalidTxSize(),
  TTransaction checkpoint(1: TTransaction tx) throws (1: TTransactionNotInProgressException e),
}
