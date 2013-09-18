namespace java com.continuuity.data.transaction.thrift

struct TTransaction {
  1: i64 readPointer,
  2: i64 writePointer,
  3: binary invalids,
  4: binary inProgress,
  5: i64 firstShort,
}

service TTransactionService {

  TTransaction startShort(),
  TTransaction startShortTimeout(1: i32 timeout),
  TTransaction startLong(),

  bool canCommit(1: TTransaction tx, 2: list<binary> changeIds),
  bool commit(1: TTransaction tx),
  void abort(1: TTransaction tx),
  void invalidate(1: TTransaction tx),
}