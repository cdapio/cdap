package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.engine.ReadPointer;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.DequeueResult.DequeueStatus;
import com.continuuity.data.operation.ttqueue.EnqueueResult.EnqueueStatus;
import com.continuuity.data.operation.ttqueue.internal.EntryGroupMeta;
import com.continuuity.data.operation.ttqueue.internal.EntryGroupMeta.EntryGroupState;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta.EntryState;
import com.continuuity.data.operation.ttqueue.internal.EntryPointer;
import com.continuuity.data.operation.ttqueue.internal.ExecutionMode;
import com.continuuity.data.operation.ttqueue.internal.GroupState;
import com.continuuity.data.operation.ttqueue.internal.ShardMeta;
import com.continuuity.data.table.VersionedColumnarTable;

/**
 * Implementation of a single {@link TTQueue} on a single
 * {@link VersionedColumnarTable} using a multi-row, sharded schema.
 */
public class TTQueueOnVCTable implements TTQueue {

  private final VersionedColumnarTable table;
  private final byte [] queueName;
  private final TimestampOracle timeOracle;
  
  long maxEntriesPerShard;
  long maxBytesPerShard;
  
  // Row header names and flags
  static final byte [] GLOBAL_ENTRY_HEADER = bytes((byte)10);
  static final byte [] GLOBAL_ENTRY_WRITEPOINTER_HEADER = bytes((byte)20);
  static final byte [] GLOBAL_SHARDS_HEADER = bytes((byte)30);
  static final byte [] GLOBAL_GROUPS_HEADER = bytes((byte)40);
  static final byte [] GLOBAL_DATA_HEADER = bytes((byte)50);
  
  // Columns for row = GLOBAL_ENTRY_HEADER
  static final byte [] GLOBAL_ENTRYID_COUNTER = bytes((byte)10);
  
  // Columns for row = GLOBAL_ENTRY_WRITEPOINTER_HEADER
  static final byte [] GLOBAL_ENTRYID_WRITEPOINTER_COUNTER = bytes((byte)10);
  
  // Columns for row = GLOBAL_SHARDS_HEADER
  static final byte [] GLOBAL_SHARD_META = bytes((byte)10);
  
  // Columns for row = GLOBAL_GROUPS_HEADER
  static final byte [] GROUP_STATE = bytes((byte)10);

  // Columns for row = GLOBAL_DATA_HEADER
  static final byte [] ENTRY_META = bytes((byte)10);
  static final byte [] ENTRY_GROUP_META = bytes((byte)20);
  static final byte [] ENTRY_DATA = bytes((byte)30);
  
  /**
   * Constructs a TTQueue with the specified queue name, backed by the specified
   * table, and utilizing the specified time oracle to generate stamps for
   * dirty reads and writes.  Utilizes specified Configuration to determine
   * shard maximums.
   * @param table
   * @param queueName
   * @param timeOracle
   * @param conf
   */
  public TTQueueOnVCTable(final VersionedColumnarTable table,
      final byte [] queueName, final TimestampOracle timeOracle,
      final Configuration conf) {
    this.table = table;
    this.queueName = queueName;
    this.timeOracle = timeOracle;
    this.maxEntriesPerShard = conf.getLong("ttqueue.shard.max.entries", 1024);
    this.maxBytesPerShard = conf.getLong("ttqueue.shard.max.bytes", 1024*1024);
  }

  @Override
  public EnqueueResult enqueue(byte[] data, long cleanWriteVersion) {
    // Get a dirty pointer
    ImmutablePair<ReadPointer,Long> dirty = dirtyPointer();
    // Get our unique entry id
    long entryId = this.table.increment(makeRow(GLOBAL_ENTRY_HEADER),
        GLOBAL_ENTRYID_COUNTER, 1, dirty.getFirst(), dirty.getSecond());
    
    // Get exclusive lock on shard determination
    byte [] entryWritePointerRow = makeRow(GLOBAL_ENTRY_WRITEPOINTER_HEADER);
    while (getCounter(entryWritePointerRow, GLOBAL_ENTRYID_WRITEPOINTER_COUNTER,
        dirty.getFirst()) != (entryId - 1)) {
      // Wait
      Thread.yield();
    }
    
    // We have an exclusive lock, determine updated shard state
    ShardMeta shardMeta = null;
    boolean movedShards = false;
    byte [] shardMetaRow = makeRow(GLOBAL_SHARDS_HEADER);
    if (entryId == 1) {
      // If our entryId is 1 we are first, initialize state
      shardMeta = new ShardMeta(1, data.length, 1);
    } else {
      // Not first, read existing and determine which shard we should be in
      shardMeta = ShardMeta.fromBytes(this.table.get(shardMetaRow,
          GLOBAL_SHARD_META, dirty.getFirst()));
      // Check if we need to move to next shard
      if (shardMeta.getShardBytes() + data.length > this.maxBytesPerShard ||
          shardMeta.getShardEntries() == this.maxEntriesPerShard) {
        // Move to next shard
        movedShards = true;
        shardMeta = new ShardMeta(shardMeta.getShardId() + 1, data.length, 1);
      } else {
        // Update current shard sizing
        shardMeta = new ShardMeta(shardMeta.getShardId(),
            shardMeta.getShardBytes() + data.length,
            shardMeta.getShardEntries() + 1);
      }
    }
    
    // Write the updated shard meta (can do dirty because we have lock)
    this.table.put(shardMetaRow, GLOBAL_SHARD_META, dirty.getSecond(),
        shardMeta.getBytes());
    // Increment entry write pointer (release shard lock)
    this.table.increment(entryWritePointerRow,
        GLOBAL_ENTRYID_WRITEPOINTER_COUNTER, 1, dirty.getFirst(),
        dirty.getSecond());
    
    // If we moved shards, insert end-of-shard entry in previously active shard
    if (movedShards) {
      this.table.put(makeRow(GLOBAL_DATA_HEADER, shardMeta.getShardId() - 1),
          makeColumn(entryId, ENTRY_META), cleanWriteVersion,
          new EntryMeta(EntryState.SHARD_END).getBytes());
    }
    
    // Insert entry at active shard
    this.table.put(makeRow(GLOBAL_DATA_HEADER, shardMeta.getShardId()),
        makeColumn(entryId, ENTRY_DATA), cleanWriteVersion, data);
    
    // Return success with pointer to entry
    return new EnqueueResult(EnqueueStatus.SUCCESS,
        new QueueEntryPointer(entryId, shardMeta.getShardId()));
  }

  @Override
  public void invalidate(QueueEntryPointer entryPointer,
      long cleanWriteVersion) {
    byte [] shardRow = makeRow(GLOBAL_DATA_HEADER, entryPointer.getShardId());
    // Change meta data to INVALID
    this.table.put(shardRow, makeColumn(entryPointer.getEntryId(), ENTRY_META),
        cleanWriteVersion, new EntryMeta(EntryState.INVALID).getBytes());
    // Delete data since it's invalidated
    this.table.delete(shardRow,
        makeColumn(entryPointer.getEntryId(), ENTRY_DATA), cleanWriteVersion);
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config,
      ReadPointer readPointer) {
    
    // Get a dirty pointer
    ImmutablePair<ReadPointer,Long> dirty = dirtyPointer();

    // Loop until we have properly upserted and verified group information
    GroupState groupState = null;
    byte [] groupRow = makeRow(GLOBAL_GROUPS_HEADER, consumer.getGroupId());
    while (true) {
      
      // Do a dirty read of the global group information
      byte [] existingValue = this.table.get(groupRow, GROUP_STATE,
          dirty.getFirst());
      
      if (existingValue == null || existingValue.length == 0) {
        // Group information did not exist, create blank initial group state
        groupState = new GroupState(consumer.getGroupSize(),
            new EntryPointer(1, 1), config.isSingleEntry() ?
                ExecutionMode.SINGLE_ENTRY : ExecutionMode.MULTI_ENTRY);
        
        // Atomically insert group state
        if (this.table.compareAndSwap(groupRow, GROUP_STATE, existingValue,
            groupState.getBytes(), dirty.getFirst(), dirty.getSecond())) {
          // CAS was successful, we created the group, exit loop
          break;
        } else {
          // CAS was not successful, someone else created group, loop
          continue;
        }
        
      } else {
        // Group information already existed, verify group state
        groupState = GroupState.fromBytes(existingValue);
        
        // Check group size and execution mode
        if (groupState.getGroupSize() == consumer.getGroupSize() &&
            groupState.getMode() == ExecutionMode.fromQueueConfig(config)) {
          // Group state matches, break from loop using current group head
          break;
        }
        
        // Group size and/or execution mode do not match, check if group empty
        if (!groupIsEmpty(groupState, consumer.getGroupId())) {
          // Group is not empty, cannot change group size or exec mode
          return new DequeueResult(DequeueStatus.FAILURE,
              "Attempted to change group configuration when group not empty");
        }
        
        // Group is empty so we can reconfigure, attempt atomic reconfig
        groupState = new GroupState(consumer.getGroupSize(),
            groupState.getHead(), ExecutionMode.fromQueueConfig(config));
        if (this.table.compareAndSwap(groupRow, GROUP_STATE, existingValue,
            groupState.getBytes(), dirty.getFirst(), dirty.getSecond())) {
          // Group config update success, break from loop
          break;
        } else {
          // Update of group meta failed, someone else conflicted, loop
          continue;
        }
      }
    }
    // GroupState is correct now
    
    // Begin iterating entries starting from group head, loop until entry found
    // or queue is empty
    EntryMeta entryMeta = null;
    EntryGroupMeta entryGroupMeta = null;
//    while (true) {
//
//    }
    
    return null;
  }

  @Override
  public boolean ack(QueueEntryPointer entryPointer, QueueConsumer consumer) {
    // Get a dirty pointer
    ImmutablePair<ReadPointer,Long> dirty = dirtyPointer();
    // Do a dirty read of EntryGroupMeta for this entry
    byte [] shardRow = makeRow(GLOBAL_DATA_HEADER, entryPointer.getShardId());
    byte [] groupColumn = makeColumn(entryPointer.getEntryId(),
        ENTRY_GROUP_META, consumer.getGroupId());
    byte [] existingValue = this.table.get(shardRow, groupColumn,
        dirty.getFirst());
    if (existingValue == null || existingValue.length == 0) return false;
    EntryGroupMeta groupMeta = EntryGroupMeta.fromBytes(existingValue);
    
    // Check if instance id matches
    if (groupMeta.getInstanceId() != consumer.getInstanceId()) return false;
    
    // Instance ids match, check if in an invalid state for ack'ing
    if (groupMeta.isAvailable() || groupMeta.isAcked()) return false;
    
    // It is in the right state, attempt atomic ack
    // (ack passed if this CAS works, fails if this CAS fails)
    byte [] newValue = new EntryGroupMeta(EntryGroupState.ACKED,
        System.currentTimeMillis(), consumer.getInstanceId()).getBytes();
    return this.table.compareAndSwap(shardRow, groupColumn, existingValue,
        newValue, dirty.getFirst(), dirty.getSecond());
  }

  // Public accessors

  VersionedColumnarTable getTable() {
    return table;
  }

  byte [] getQueueName() {
    return queueName;
  }
  
  // Private helpers

  private boolean groupIsEmpty(GroupState groupState, long groupId) {
    // A group is empty if the head pointer points to the 
    // TODO Auto-generated method stub
    return false;
  }
  
  private byte[] makeRow(byte[] bytesToAppendToQueueName) {
    return Bytes.add(this.queueName, bytesToAppendToQueueName);
  }

  private byte[] makeRow(byte[] bytesToAppendToQueueName, long idToAppend) {
    return Bytes.add(this.queueName,
        bytesToAppendToQueueName, Bytes.toBytes(idToAppend));
  }

  private byte[] makeColumn(long id, byte[] bytesToAppendToId) {
    return Bytes.add(Bytes.toBytes(id), bytesToAppendToId);
  }

  private byte[] makeColumn(long prepend, byte[] middle, long append) {
    return Bytes.add(Bytes.toBytes(prepend), middle, Bytes.toBytes(append));
  }

  private ImmutablePair<ReadPointer, Long> dirtyPointer() {
    long now = this.timeOracle.getTimestamp();
    return new ImmutablePair<ReadPointer,Long>(new MemoryReadPointer(now), now);
  }

  private long getCounter(byte[] row, byte[] column, ReadPointer readPointer) {
    byte [] value = this.table.get(row, column, readPointer);
    if (value == null || value.length == 0) return 0;
    return Bytes.toLong(value);
  }
  
  private static byte [] bytes(byte b) {
    return new byte [] { b };
  }
}
