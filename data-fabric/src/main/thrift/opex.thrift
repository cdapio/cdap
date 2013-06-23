namespace java com.continuuity.data.operation.executor.remote.stubs

// read operations

struct TOperationContext {
  1: string account,
  2: optional string application,
}

struct TRead {
  1: optional string table,
  2: optional string metric,
  3: binary key,
  4: list<binary> columns,
  5: i64 id,
}

struct TReadAllKeys {
  1: optional string table,
  2: optional string metric,
  3: i32 offset,
  4: i32 limit,
  5: i64 id,
}

struct TGetSplits {
  1: string table,
  2: optional string metric,
  3: optional binary start,
  4: optional binary stop,
  5: optional list<binary> columns,
  6: i32 numSplits,
  7: i64 id,
}

struct TReadColumnRange {
  1: optional string table,
  2: optional string metric,
  3: binary key,
  4: binary startColumn,
  5: binary stopColumn,
  6: i32 limit,
  7: i64 id,
}

// write operations

struct TWrite {
  1: optional string table,
  2: optional string metric,
  3: binary key,
  4: list<binary> columns,
  5: list<binary> values,
  6: i64 id,
}

struct TDelete {
  1: optional string table,
  2: optional string metric,
  3: binary key,
  4: list<binary> columns,
  5: i64 id,
}

struct TIncrement {
  1: optional string table,
  2: optional string metric,
  3: binary key,
  4: list<binary> columns,
  5: list<i64> amounts,
  6: i64 id,
}

struct TCompareAndSwap {
  1: optional string table,
  2: optional string metric,
  3: binary key,
  4: binary column,
  5: binary expectedValue,
  6: binary newValue,
  7: i64 id,
}

// queue stuff

struct TQueueProducer {
  1: optional string name,
}

struct TQueueEntry {
  1: optional map<string,i32> header,
  2: binary data,
}

struct TQueueEnqueue {
  1: binary queueName,
  2: list<TQueueEntry> entries,
  3: i64 id,
  4: optional TQueueProducer producer,
  5: optional string metric,
}

enum TQueuePartitioner {
  FIFO,
  HASH,
  ROBIN,
}

struct TQueueConfig {
  1: TQueuePartitioner partitioner,
  2: bool singleEntry,
  3: i32 batchSize,
  4: bool returnBatch,
}

enum TQueueStateType {
  UNINITIALIZED,
  INITIALIZED,
  NOT_FOUND,
}

struct TQueueConsumer {
  1: i32 instanceId,
  2: i64 groupId,
  3: i32 groupSize,
  4: optional string groupName,
  5: optional string partitioningKey,
  6: optional TQueueConfig queueConfig,
  7: bool isStateful,
  8: TQueueStateType stateType,
}

struct TQueueEntryPointer {
  1: binary queueName,
  2: i64 entryId,
  3: i64 shardId,
  4: i32 tries,
}

struct TQueueAck {
  1: binary queueName,
  2: list<TQueueEntryPointer> entryPointers,
  3: TQueueConsumer consumer,
  4: i32 numGroups,
  5: i64 id,
  6: optional string metric,
}

struct TQueueDequeue {
  1: binary queueName,
  2: TQueueConsumer consumer,
  3: TQueueConfig config,
  4: i64 id,
  5: optional string metric,
}

enum TDequeueStatus {
  SUCCESS,
  EMPTY,
}

struct TDequeueResult {
  1: TDequeueStatus status,
  2: optional list<TQueueEntryPointer> pointers,
  3: optional list<TQueueEntry> entries,
  4: optional TQueueConsumer consumer,
}

struct TGetGroupId {
  1: binary queueName,
  2: optional string metric,
}

struct TGetQueueInfo {
  1: binary queueName,
  2: i64 id,
  3: optional string metric,
}

struct TQueueConfigure {
  1: binary queueName,
  2: TQueueConsumer newConsumer,
  3: optional string metric,
}

struct TQueueConfigureGroups {
  1: binary queueName,
  2: list<i64> groupIds,
  3: optional string metric,
}

struct TQueueDropInflight {
  1: binary queueName,
  2: TQueueConsumer consumer,
  3: optional string metric,
}

// we add a virtual field "nulled" to indicate a null object
struct TQueueInfo {
  1: bool empty,
  2: optional string json,
}

// using an undocumented Thrift feature: union,
// see https://issues.apache.org/jira/browse/THRIFT-409

union TWriteOperation {
  1: TWrite write,
  2: TDelete delet,
  3: TIncrement increment,
  4: TCompareAndSwap compareAndSwap,
  5: TQueueEnqueue queueEnqueue,
  6: TQueueAck queueAck,
}

typedef list<TWriteOperation> TWriteBatch

struct TClearFabric {
  1: bool clearData,
  2: bool clearMeta,
  3: bool clearTables,
  4: bool clearQueues,
  5: bool clearStreams,
  6: i64 id,
  7: optional string metric,
}

struct TOpenTable {
  1: string table,
  2: i64 id,
  3: optional string metric,
}

struct TOptionalBinary {
  1: optional binary value,
  2: optional i32 status,
  3: optional string message,
}

struct TOptionalBinaryList {
  1: optional list<binary> theList,
  2: optional i32 status,
  3: optional string message,
}

struct TOptionalBinaryMap {
  1: optional map<binary,TOptionalBinary> theMap,
  2: optional i32 status,
  3: optional string message,
}

struct TReadPointer {
  1: i64 readPoint,
  2: i64 writePoint,
  3: set<i64> excludes,
}

struct TTransaction {
  1: bool isNull,
  2: optional i64 txid,
  3: optional TReadPointer readPointer,
  4: optional bool trackChanges,
}

struct TKeyRange {
  1: binary start,
  2: binary stop,
}

exception TOperationException {
  1: required i32 status,
  2: string message,
}

service TOperationExecutor {

  // batch op ex
  TTransaction start(1: TOperationContext context, 2: bool trackChanges) throws (1: TOperationException ex),
  void batch(1: TOperationContext context, 2: TWriteBatch batch) throws (1: TOperationException ex),
  TTransaction execute(1: TOperationContext context, 2: TTransaction tx, 3: TWriteBatch batch) throws (1: TOperationException ex),
  void finish(1: TOperationContext context, 2: TTransaction tx, 3: TWriteBatch batch) throws (1: TOperationException ex),
  void commit(1: TOperationContext context, 2: TTransaction tx) throws (1: TOperationException ex),
  void abort(1: TOperationContext context, 2: TTransaction tx) throws (1: TOperationException ex),

  // read op ex
  TOptionalBinaryMap read(1: TOperationContext context, 2: TRead read) throws (1: TOperationException ex),
  TOptionalBinaryMap readTx(1: TOperationContext context, 2: TTransaction tx, 3: TRead read) throws (1: TOperationException ex),
  TOptionalBinaryList readAllKeys(1: TOperationContext context, 2: TReadAllKeys readAllKeys) throws (1: TOperationException ex),
  TOptionalBinaryList readAllKeysTx(1: TOperationContext context, 2: TTransaction tx, 3: TReadAllKeys readAllKeys) throws (1: TOperationException ex),
  TOptionalBinaryMap readColumnRange(1: TOperationContext context, 2: TReadColumnRange readColumnRange) throws (1: TOperationException ex),
  TOptionalBinaryMap readColumnRangeTx(1: TOperationContext context, 2: TTransaction tx, 3: TReadColumnRange readColumnRange) throws (1: TOperationException ex),
  map<binary, i64> increment(1: TOperationContext context, 2: TIncrement increment) throws (1: TOperationException ex),
  map<binary, i64> incrementTx(1: TOperationContext context, 2: TTransaction tx, 3: TIncrement increment) throws (1: TOperationException ex),
  list<TKeyRange> getSplits(1: TOperationContext context, 2: TGetSplits getSplits) throws (1: TOperationException ex),
  list<TKeyRange> getSplitsTx(1: TOperationContext context, 2: TTransaction tx, 3: TGetSplits getSplits) throws (1: TOperationException ex),

  // internal op ex
  TDequeueResult dequeue(1: TOperationContext context, 2: TQueueDequeue dequeue) throws (1: TOperationException ex),
  i64 getGroupId(1: TOperationContext context, 2: TGetGroupId getGroupId) throws (1: TOperationException ex),
  TQueueInfo getQueueInfo(1: TOperationContext context, 2: TGetQueueInfo getQueueInfo) throws (1: TOperationException ex),
  void clearFabric(1: TOperationContext context, 2: TClearFabric clearFabric) throws (1: TOperationException ex),
  void openTable(1: TOperationContext context, 2: TOpenTable openTable) throws (1: TOperationException ex),
  void configureQueue(1: TOperationContext context, 2: TQueueConfigure configure) throws (1: TOperationException ex),
  void configureQueueGroups(1: TOperationContext context, 2: TQueueConfigureGroups configure) throws (1: TOperationException ex),
  void queueDropInflight(1: TOperationContext context, 2: TQueueDropInflight op) throws (1: TOperationException ex),

}
