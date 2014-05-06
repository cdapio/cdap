namespace java com.continuuity.data2.transaction.distributed.thrift

struct TTransaction {
  1: i64 writePointer,
  2: i64 readPointer,
  3: list<i64> invalids,
  4: list<i64> inProgress,
  5: i64 firstShort,
}

exception TTransactionNotInProgressException {
  1: string message
}

exception TTransactionCouldNotTakeSnapshotException {
  1: string message
}

# workaround for THRIFT-1474
struct TBoolean {
  1: bool value
}

service TTransactionServer {
  // temporary tx2 stuff
  TTransaction startLong(),
  TTransaction startShort(),
  TTransaction startShortTimeout(1: i32 timeout),
  TBoolean canCommitTx(1: TTransaction tx, 2: set<binary> changes) throws (1:TTransactionNotInProgressException e),
  TBoolean commitTx(1: TTransaction tx) throws (1:TTransactionNotInProgressException e),
  void abortTx(1: TTransaction tx),
  bool invalidateTx(1: i64 tx),
  binary getSnapshot() throws (1:TTransactionCouldNotTakeSnapshotException e),
  void resetState(),
  string status(),
}
