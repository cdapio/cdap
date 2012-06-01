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

  //  private static final Logger LOG =
  //      LoggerFactory.getLogger(TTQueueOnVCTable.class);

  private final VersionedColumnarTable table;
  private final byte [] queueName;
  private final TimestampOracle timeOracle;

  long maxEntriesPerShard;
  long maxBytesPerShard;
  long maxAgeBeforeExpirationInMillis;

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
    this.maxAgeBeforeExpirationInMillis = conf.getLong("ttqueue.entry.age.max",
        10 * 1000); // 10 seconds default
  }

  @Override
  public EnqueueResult enqueue(byte[] data, long cleanWriteVersion) {
    log("Enqueueing (data.len=" + data.length + ", writeVersion=" +
        cleanWriteVersion + ")");
    // Get a dirty pointer
    ImmutablePair<ReadPointer,Long> dirty = dirtyPointer();
    // Get our unique entry id
    long entryId = this.table.increment(makeRow(GLOBAL_ENTRY_HEADER),
        GLOBAL_ENTRYID_COUNTER, 1, dirty.getFirst(), dirty.getSecond());
    log("New enqueue got entry id " + entryId);

    // Get exclusive lock on shard determination
    byte [] entryWritePointerRow = makeRow(GLOBAL_ENTRY_WRITEPOINTER_HEADER);
    while (getCounter(entryWritePointerRow, GLOBAL_ENTRYID_WRITEPOINTER_COUNTER,
        dirty.getFirst()) != (entryId - 1)) {
      // Wait
      log("Waiting for exclusive lock on shard determination");
      quickWait();
    }
    log("Exclusive lock acquired for entry id " + entryId);

    // We have an exclusive lock, determine updated shard state
    ShardMeta shardMeta = null;
    boolean movedShards = false;
    byte [] shardMetaRow = makeRow(GLOBAL_SHARDS_HEADER);
    if (entryId == 1) {
      // If our entryId is 1 we are first, initialize state
      shardMeta = new ShardMeta(1, data.length, 1);
      log("First entry, initializing first shard meta");
    } else {
      // Not first, read existing and determine which shard we should be in
      shardMeta = ShardMeta.fromBytes(this.table.get(shardMetaRow,
          GLOBAL_SHARD_META, dirty.getFirst()));
      log("Found existing global shard meta: " + shardMeta.toString());
      // Check if we need to move to next shard (pass max bytes or max entries)
      if ((shardMeta.getShardBytes() + data.length > this.maxBytesPerShard &&
          shardMeta.getShardEntries() > 1) ||
          shardMeta.getShardEntries() == this.maxEntriesPerShard) {
        // Move to next shard
        movedShards = true;
        shardMeta = new ShardMeta(shardMeta.getShardId() + 1, data.length, 1);
        log("Moving to next shard");
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
    long newWritePointer = this.table.increment(entryWritePointerRow,
        GLOBAL_ENTRYID_WRITEPOINTER_COUNTER, 1, dirty.getFirst(),
        dirty.getSecond());
    log("Updated shard meta (" + shardMeta + ") and incremented write " +
        "pointer to " + newWritePointer);

    // If we moved shards, insert end-of-shard entry in previously active shard
    if (movedShards) {
      this.table.put(makeRow(GLOBAL_DATA_HEADER, shardMeta.getShardId() - 1),
          makeColumn(entryId, ENTRY_META), cleanWriteVersion,
          new EntryMeta(EntryState.SHARD_END).getBytes());
      log("Moved shard, inserting end-of-shard marker for: " + shardMeta);
    }

    // Insert entry at active shard
    this.table.put(makeRow(GLOBAL_DATA_HEADER, shardMeta.getShardId()),
        new byte [][] {
          makeColumn(entryId, ENTRY_META), makeColumn(entryId, ENTRY_DATA)
        }, cleanWriteVersion,
        new byte [][] {
          new EntryMeta(EntryState.VALID).getBytes(), data
        });

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
    log("Invalidated " + entryPointer);
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config,
      ReadPointer readPointer) {

    log("Attempting dequeue(" + consumer + ", " + config + ", " +
        readPointer + ")");

    // Get a dirty pointer
    ImmutablePair<ReadPointer,Long> dirty = dirtyPointer();

    // Loop until we have properly upserted and verified group information
    GroupState groupState = null;
    byte [] groupRow = makeRow(GLOBAL_GROUPS_HEADER, consumer.getGroupId());
    while (true) { // TODO: Should probably put a max retry on here

      // Do a dirty read of the global group information
      byte [] existingValue = this.table.get(groupRow, GROUP_STATE,
          dirty.getFirst());

      if (existingValue == null || existingValue.length == 0) {
        // Group information did not exist, create blank initial group state
        log("Group information DNE, creating initial group state");
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
        log("Group state already existed: " + groupState);

        // Check group size and execution mode
        if (groupState.getGroupSize() == consumer.getGroupSize() &&
            groupState.getMode() == ExecutionMode.fromQueueConfig(config)) {
          // Group state matches, break from loop using current group head
          break;
        }

        // Group size and/or execution mode do not match, check if group empty
        if (!groupIsEmpty(groupState, consumer.getGroupId(),
            dirty.getFirst())) {
          // Group is not empty, cannot change group size or exec mode
          log("Attempted to change group config but it is not empty");
          return new DequeueResult(DequeueStatus.FAILURE,
              "Attempted to change group configuration when group not empty");
        }

        // Group is empty so we can reconfigure, attempt atomic reconfig
        groupState = new GroupState(consumer.getGroupSize(),
            groupState.getHead(), ExecutionMode.fromQueueConfig(config));
        if (this.table.compareAndSwap(groupRow, GROUP_STATE, existingValue,
            groupState.getBytes(), dirty.getFirst(), dirty.getSecond())) {
          // Group config update success, break from loop
          log("Group config updated successfully!");
          break;
        } else {
          // Update of group meta failed, someone else conflicted, loop
          log("Group config update failed");
          continue;
        }
      }
    }
    // GroupState is correct now

    // Begin iterating entries starting from group head, loop until entry found
    // or queue is empty
    EntryMeta entryMeta = null;
    EntryGroupMeta entryGroupMeta = null;
    EntryPointer entryPointer = groupState.getHead();
    while (entryPointer != null) { // TODO: Should probably put a max retry

      // We are pointed at {entryPointer}=(shardid,entryid) and we are either
      // at the head of this group or we have skipped everything between where
      // we are and the head.

      byte [] shardRow = makeRow(GLOBAL_DATA_HEADER, entryPointer.getShardId());
      byte [] entryMetaColumn = makeColumn(entryPointer.getEntryId(),
          ENTRY_META);

      // Do a dirty read of the entry meta data
      ImmutablePair<byte[],Long> entryMetaDataAndStamp =
          this.table.getWithVersion(shardRow, entryMetaColumn,
              dirty.getFirst());
      if (entryMetaDataAndStamp == null) {
        // This entry doesn't exist, queue is empty for this consumer
        log("Queue is empty, nothing found at " + entryPointer);
        return new DequeueResult(DequeueStatus.EMPTY);
      }

      // Entry exists, check if it should actually be visible
      if (!readPointer.isVisible(entryMetaDataAndStamp.getSecond())) {
        // This is currently being enqueued in an uncommitted transaction,
        // wait and loop back without changing entry pointer
        log("Data exists but is not yet visible at " + entryPointer +
            ", retrying");
        quickWait();
        continue;
      }

      // Queue entry exists and is visible, check the global state of it
      entryMeta = EntryMeta.fromBytes(entryMetaDataAndStamp.getFirst());

      // Check if entry has been invalidated
      if (entryMeta.isInvalid()) {
        // Invalidated.  Check head update and move to next entry in this shard
        log("Found invalidated entry at " + entryPointer);
        EntryPointer nextEntryPointer = new EntryPointer(
            entryPointer.getEntryId() + 1, entryPointer.getShardId());
        if (entryPointer.equals(groupState.getHead())) {
          // The head is invalid, attempt to move head down
          GroupState newGroupState = new GroupState(groupState.getGroupSize(),
              nextEntryPointer, groupState.getMode());
          if (this.table.compareAndSwap(groupRow, GROUP_STATE,
              groupState.getBytes(), newGroupState.getBytes(),
              dirty.getFirst(), dirty.getSecond())) {
            // Successfully moved group head, move on
            groupState = newGroupState;
          } else {
            // Group head update failed, someone else has changed it, move on
            groupState = GroupState.fromBytes(this.table.get(
                groupRow, GROUP_STATE, dirty.getFirst()));
          }
        }
        // This entry is invalid, move to the next entry and loop
        entryPointer = nextEntryPointer;
        continue;
      }

      // Check if entry is an end-of-shard marker
      if (entryMeta.iEndOfShard()) {
        // Entry is an indicator that this is the end of the shard, move
        // to the next shardId with the same entryId, check head update
        EntryPointer nextEntryPointer = new EntryPointer(
            entryPointer.getEntryId(), entryPointer.getShardId() + 1);
        if (entryPointer.equals(groupState.getHead())) {
          // The head is an end-ofshard, attempt to move head down
          GroupState newGroupState = new GroupState(groupState.getGroupSize(),
              nextEntryPointer, groupState.getMode());
          if (this.table.compareAndSwap(groupRow, GROUP_STATE,
              groupState.getBytes(), newGroupState.getBytes(),
              dirty.getFirst(), dirty.getSecond())) {
            // Successfully moved group head, move on
            groupState = newGroupState;
          } else {
            // Group head update failed, someone else has changed it, move on
            groupState = GroupState.fromBytes(this.table.get(
                groupRow, GROUP_STATE, dirty.getFirst()));
          }
        }
        // This entry is invalid, move to the next entry and loop
        entryPointer = nextEntryPointer;
        continue;
      }

      // Entry is visible and valid!
      assert(entryMeta.isValid());

      // Do a dirty read of the entry group meta data
      byte [] entryGroupMetaColumn = makeColumn(entryPointer.getEntryId(),
          ENTRY_GROUP_META, consumer.getGroupId());
      byte[] entryGroupMetaData = this.table.get(shardRow, entryGroupMetaColumn,
          dirty.getFirst());
      if (entryGroupMetaData == null || entryGroupMetaData.length == 0) {
        // Group has not processed this entry yet, consider available for now
        entryGroupMetaData = null;
      } else {
        entryGroupMeta = EntryGroupMeta.fromBytes(entryGroupMetaData);

        // Check if group has already acked this entry
        if (entryGroupMeta.isAcked()) {
          // Group has acked this, check head, move to next entry in shard
          EntryPointer nextEntryPointer = new EntryPointer(
              entryPointer.getEntryId() + 1, entryPointer.getShardId());
          if (entryPointer.equals(groupState.getHead())) {
            // The head is acked for this group, attempt to move head down
            GroupState newGroupState = new GroupState(groupState.getGroupSize(),
                nextEntryPointer, groupState.getMode());
            if (this.table.compareAndSwap(groupRow, GROUP_STATE,
                groupState.getBytes(), newGroupState.getBytes(),
                dirty.getFirst(), dirty.getSecond())) {
              // Successfully moved group head, move on
              groupState = newGroupState;
            } else {
              // Group head update failed, someone else has changed it, move on
              groupState = GroupState.fromBytes(this.table.get(
                  groupRow, GROUP_STATE, dirty.getFirst()));
            }
          }
          // This entry is acked, move to the next entry and loop
          entryPointer = nextEntryPointer;
          continue;
        }

        // Check if entry is currently dequeued by group
        if (entryGroupMeta.isDequeued()) {
          // Entry is dequeued, check if it is for us, expired, etc.

          if (config.isSingleEntry() &&
              entryGroupMeta.getInstanceId() == consumer.getInstanceId()) {
            // Sync mode, same consumer, try to update stamp and give it back
            EntryGroupMeta newEntryGroupMeta = new EntryGroupMeta(
                EntryGroupState.DEQUEUED, now(), consumer.getInstanceId());
            // Attempt to update with updated timestamp
            if (this.table.compareAndSwap(shardRow, entryGroupMetaColumn,
                entryGroupMetaData, newEntryGroupMeta.getBytes(),
                dirty.getFirst(), dirty.getSecond())) {
              // Successfully updated timestamp, still own it, return this
              return new DequeueResult(DequeueStatus.SUCCESS, entryPointer,
                  this.table.get(shardRow,
                      makeColumn(entryPointer.getEntryId(), ENTRY_DATA),
                      dirty.getFirst()));
            } else {
              // Failed to update group meta, someone else must own it now,
              // move to next entry in shard
              entryPointer = new EntryPointer(
                  entryPointer.getEntryId() + 1, entryPointer.getShardId());
              continue;
            }
          }

          if (entryGroupMeta.getTimestamp() + this.maxAgeBeforeExpirationInMillis >=
              now()) {
            // Entry is dequeued and not expired, move to next entry in shard
            entryPointer = new EntryPointer(
                entryPointer.getEntryId() + 1, entryPointer.getShardId());
            continue;
          }
        }
      }

      // Entry is available for this consumer and group

      // Get the data and check the partitioner
      byte [] data = this.table.get(shardRow,
          makeColumn(entryPointer.getEntryId(), ENTRY_DATA), dirty.getFirst());
      if (!config.getPartitioner().shouldEmit(consumer,
          entryPointer.getEntryId(), data)) {
        // Partitioner says skip, move to next entry in shard
        entryPointer = new EntryPointer(
            entryPointer.getEntryId() + 1, entryPointer.getShardId());
        continue;
      }

      // Atomically update group meta to point to this consumer and return!
      EntryGroupMeta newEntryGroupMeta = new EntryGroupMeta(
          EntryGroupState.DEQUEUED, now(), consumer.getInstanceId());
      if (this.table.compareAndSwap(shardRow, entryGroupMetaColumn,
          entryGroupMetaData, newEntryGroupMeta.getBytes(),
          dirty.getFirst(), dirty.getSecond())) {
        // We own it!  Return it.
        return new DequeueResult(DequeueStatus.SUCCESS, entryPointer, data);
      } else {
        // Someone else has grabbed it, on to the next one
        entryPointer = new EntryPointer(
            entryPointer.getEntryId() + 1, entryPointer.getShardId());
        continue;
      }
    }

    // If you exit this loop, something is wrong.  Fail.
    throw new RuntimeException("Somehow broke from loop, bug in TTQueue");
    //    return new DequeueResult(DequeueStatus.FAILURE,
    //        "Somehow broke from loop, bug in TTQueue");
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
        now(), consumer.getInstanceId()).getBytes();
    return this.table.compareAndSwap(shardRow, groupColumn, existingValue,
        newValue, dirty.getFirst(), dirty.getSecond());
  }

  // Public accessors

  VersionedColumnarTable getTable() {
    return this.table;
  }

  byte [] getQueueName() {
    return this.queueName;
  }

  // Private helpers

  private boolean groupIsEmpty(GroupState groupState, long groupId,
      ReadPointer readPointer) {
    // A group is empty if the head pointer points to the entry write pointer
    byte [] writePointerRow = makeRow(GLOBAL_ENTRY_WRITEPOINTER_HEADER);
    long writePointer = getCounter(writePointerRow,
        GLOBAL_ENTRYID_WRITEPOINTER_COUNTER, readPointer);
    // head pointer points at the next available slot (at an empty slot here)
    // write pointer points at the last committed slot (one before head)
    return writePointer == groupState.getHead().getEntryId() - 1;
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
    return new ImmutablePair<ReadPointer,Long>(new MemoryReadPointer(now), 1L);
  }

  private long getCounter(byte[] row, byte[] column, ReadPointer readPointer) {
    byte [] value = this.table.get(row, column, readPointer);
    if (value == null || value.length == 0) return 0;
    return Bytes.toLong(value);
  }

  private void quickWait() {
    Thread.yield();
  }

  private long now() {
    return System.currentTimeMillis();
  }

  private void log(String msg) {
    System.out.println(msg);
    // LOG.debug(msg);
  }

  private static byte [] bytes(byte b) {
    return new byte [] { b };
  }
}
