namespace java com.continuuity.data.operation.executor.remote.stubs

struct TTransaction2 {
  1: i64 writePointer,
  2: i64 readPointer,
  3: list<i64> invalids,
  4: list<i64> inProgress,
  5: i64 firstShort,
}

exception TOperationException {
  1: required i32 status,
  2: string message,
}

service TOperationExecutor {
  // temporary tx2 stuff
  TTransaction2 startLong() throws (1: TOperationException ex),
  TTransaction2 startShort() throws (1: TOperationException ex),
  TTransaction2 startShortTimeout(1: i32 timeout) throws (1: TOperationException ex),
  bool canCommitTx(1: TTransaction2 tx, 2: set<binary> changes) throws (1: TOperationException ex),
  bool commitTx(1: TTransaction2 tx) throws (1: TOperationException ex),
  void abortTx(1: TTransaction2 tx) throws (1: TOperationException ex),
  void invalidateTx(1: TTransaction2 tx) throws (1: TOperationException ex),
}
