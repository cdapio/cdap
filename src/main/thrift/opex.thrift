namespace java com.continuuity.data.operation.executor.remote.stubs

// read operations

struct TOperationContext {
  1: string account,
  2: optional string application,
}

struct TReadKey {
  1: optional string table,
  2: binary key,
}

struct TRead {
  1: optional string table,
  2: binary key,
  3: list<binary> columns,
}

struct TReadAllKeys {
  1: optional string table,
  2: i32 offset,
  3: i32 limit,
}

struct TReadColumnRange {
  1: optional string table,
  2: binary key,
  3: binary startColumn,
  4: binary stopColumn,
  5: i32 limit,
}

// write operations

struct TWrite {
  1: optional string table,
  2: binary key,
  3: list<binary> columns,
  4: list<binary> values,
}

struct TDelete {
  1: optional string table,
  2: binary key,
  3: list<binary> columns,
}

struct TIncrement {
  1: optional string table,
  2: binary key,
  3: list<binary> columns,
  4: list<i64> amounts,
}

struct TCompareAndSwap {
  1: optional string table,
  2: binary key,
  3: binary column,
  4: binary expectedValue,
  5: binary newValue,
}

// queue stuff

struct TQueueEnqueue {
  1: binary queueName,
  2: binary value,
}

struct TQueueConsumer {
  1: i32 instanceId,
  2: i64 groupId,
  3: i32 groupSize,
}

struct TQueueEntryPointer {
  1: binary queueName,
  2: i64 entryId,
  3: i64 shardId,
}

struct TQueueAck {
  1: binary queueName,
  2: TQueueEntryPointer entryPointer,
  3: TQueueConsumer consumer,
  4: i32 numGroups,
}

enum TQueuePartitioner {
  RANDOM,
  HASH,
  LONGMOD,
}

struct TQueueConfig {
  1: TQueuePartitioner partitioner,
  2: bool singleEntry,
}

struct TQueueDequeue {
  1: binary queueName,
  2: TQueueConsumer consumer,
  3: TQueueConfig config,
}

enum TDequeueStatus {
  SUCCESS,
  EMPTY,
  RETRY,
}


struct TDequeueResult {
  1: TDequeueStatus status,
  2: TQueueEntryPointer pointer,
  3: binary value,
}

struct TGetGroupId {
  1: binary queueName,
}

struct TGetQueueMeta {
  1: binary queueName,
}

typedef TQueueEntryPointer TEntryPointer

enum TExecutionMode {
  SINGLE_ENTRY,
  MULTI_ENTRY,
}

struct TGroupState {
  1: i32 groupSize,
  2: TEntryPointer head,
  3: TExecutionMode mode,
  4: bool nulled,
}

// we add a virtual field "nulled" to indicate a null object
struct TQueueMeta {
  1: bool empty,
  2: optional i64 globalHeadPointer,
  3: optional i64 currentWritePointer,
  4: optional list<TGroupState> groups,
  5: optional i32 status,
  6: optional string message,
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

exception TOperationException {
  1: required i32 status,
  2: string message,
}

service TOperationExecutor {

  // write op ex
  void write(1: TOperationContext context, 2: TWrite write) throws (1: TOperationException ex),
  void delet(1: TOperationContext context, 2: TDelete delet) throws (1: TOperationException ex),
  void increment(1: TOperationContext context, 2: TIncrement increment) throws (1: TOperationException ex),
  void compareAndSwap(1: TOperationContext context, 2: TCompareAndSwap compareAndSwap) throws (1: TOperationException ex),
  void queueEnqueue(1: TOperationContext context, 2: TQueueEnqueue queueEnqueue) throws (1: TOperationException ex),
  void queueAck(1: TOperationContext context, 2: TQueueAck queueAck) throws (1: TOperationException ex),

  // batch op ex
  void batch(1: TOperationContext context, 2: TWriteBatch batch) throws (1: TOperationException ex),

  // read op ex
  TOptionalBinary readKey(1: TOperationContext context, 2: TReadKey readKey) throws (1: TOperationException ex),
  TOptionalBinaryMap read(1: TOperationContext context, 2: TRead read) throws (1: TOperationException ex),
  TOptionalBinaryList readAllKeys(1: TOperationContext context, 2: TReadAllKeys readAllKeys) throws (1: TOperationException ex),
  TOptionalBinaryMap readColumnRange(1: TOperationContext context, 2: TReadColumnRange readColumnRange) throws (1: TOperationException ex),

  // internal op ex
  TDequeueResult dequeuePayload(1: TOperationContext context, 2: TQueueDequeue dequeuePayload) throws (1: TOperationException ex),
  i64 getGroupId(1: TOperationContext context, 2: TGetGroupId getGroupId) throws (1: TOperationException ex),
  TQueueMeta getQueueMeta(1: TOperationContext context, 2: TGetQueueMeta getQueueMeta) throws (1: TOperationException ex),
  void clearFabric(1: TOperationContext context, 2: TClearFabric clearFabric) throws (1: TOperationException ex),

}
