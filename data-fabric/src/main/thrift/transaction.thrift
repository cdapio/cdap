namespace java com.continuuity.data2.transaction.distributed.thrift

struct TTransaction {
  1: i64 writePointer,
  2: i64 readPointer,
  3: list<i64> invalids,
  4: list<i64> inProgress,
  5: i64 firstShort,
}

service TTransactionServer {
  // temporary tx2 stuff
  TTransaction startLong(),
  TTransaction startShort(),
  TTransaction startShortTimeout(1: i32 timeout),
  bool canCommitTx(1: TTransaction tx, 2: set<binary> changes),
  bool commitTx(1: TTransaction tx),
  void abortTx(1: TTransaction tx),
  void invalidateTx(1: TTransaction tx),
}
