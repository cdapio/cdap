namespace java com.continuuity.data.transaction.thrift

struct TTransaction {
  1: i64 readPointer,
  2: i64 writePointer,
  3: binary excludedList
}

service TTransactionService {

  TTransaction start(),

  bool canCommit(1: TTransaction tx, 2: list<binary> changeIds),

  bool commit(1: TTransaction tx),

  bool abort(1: TTransaction tx)
}