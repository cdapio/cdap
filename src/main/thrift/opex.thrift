namespace java com.continuuity.data.operation.executor.remote.stubs

// read operations

struct TReadKey {
  1: binary key,
}

struct TRead {
  1: binary key,
  2: list<binary> columns,
}

struct TReadAllKeys {
  1: i32 offset,
  2: i32 limit,
}

struct TReadColumnRange {
  1: binary key,
  2: binary startColumn,
  3: binary stopColumn,
  4: i32 limit,
}

// write operations

struct TWrite {
  1: binary key,
  2: list<binary> columns,
  3: list<binary> values,
}

struct TDelete {
  1: binary key,
  2: list<binary> columns,
}

struct TIncrement {
  1: binary key,
  2: list<binary> columns,
  3: list<i64> amounts,
}

struct TCompareAndSwap {
  1: binary key,
  2: binary column,
  3: binary expectedValue,
  4: binary newValue,
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
  FAILURE,
  RETRY,
}


struct TDequeueResult {
  1: TDequeueStatus status,
  2: TQueueEntryPointer pointer,
  3: binary value,
  4: string message,
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
}

// we add a virtual field "nulled" to indicate a null object
struct TQueueMeta {
  1: bool nulled,
  2: i64 globalHeadPointer,
  3: i64 currentWritePointer,
  4: list<TGroupState> groups,
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
  2: bool clearQueues,
  3: bool clearStreams,
}

struct TBatchOperationResult {
  1: bool success,
  2: string message,
}

struct TOptionalBinary {
  1: optional binary value,
}

struct TOptionalBinaryList {
  1: optional list<binary> theList,
}

struct TOptionalBinaryMap {
  1: optional map<binary,TOptionalBinary> theMap,
}

exception TBatchOperationException {
  1: string message,
}

service TOperationExecutor {

  // write op ex
  bool write(1: TWrite write),
  bool delet(1: TDelete delet),
  bool increment(1: TIncrement increment),
  bool compareAndSwap(1: TCompareAndSwap compareAndSwap),
  bool queueEnqueue(1: TQueueEnqueue queueEnqueue),
  bool queueAck(1: TQueueAck queueAck),

  // read op ex
  TOptionalBinary readKey(1: TReadKey readKey),
  TOptionalBinaryMap read(1: TRead read),
  TOptionalBinaryList readAllKeys(1: TReadAllKeys readAllKeys),
  TOptionalBinaryMap readColumnRange(1: TReadColumnRange readColumnRange),

  // batch op ex
  TBatchOperationResult batch(1: TWriteBatch batch) throws (1: TBatchOperationException ex),

  // internal op ex
  TDequeueResult dequeue(1: TQueueDequeue dequeue),
  i64 getGroupId(1: TGetGroupId getGroupId),
  TQueueMeta getQueueMeta(1: TGetQueueMeta getQueueMeta),
  void clearFabric(1: TClearFabric clearFabric),

}
