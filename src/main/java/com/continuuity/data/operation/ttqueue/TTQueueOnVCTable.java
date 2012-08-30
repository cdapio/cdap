package com.continuuity.data.operation.ttqueue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.DequeueResult.DequeueStatus;
import com.continuuity.data.operation.ttqueue.EnqueueResult.EnqueueStatus;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.operation.ttqueue.internal.EntryGroupMeta;
import com.continuuity.data.operation.ttqueue.internal.EntryGroupMeta.EntryGroupState;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta.EntryState;
import com.continuuity.data.operation.ttqueue.internal.EntryPointer;
import com.continuuity.data.operation.ttqueue.internal.ExecutionMode;
import com.continuuity.data.operation.ttqueue.internal.GroupState;
import com.continuuity.data.operation.ttqueue.internal.ShardMeta;
import com.continuuity.data.table.ReadPointer;
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
  final TimestampOracle timeOracle;

  long maxEntriesPerShard;
  long maxBytesPerShard;
  long maxAgeBeforeExpirationInMillis;
  long maxAgeBeforeSemiAckedToAcked;

  // For testing
  AtomicLong dequeueReturns = new AtomicLong(0);

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
  static final byte [] GROUP_ID_GEN = bytes((byte)5);
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
    this.maxBytesPerShard = conf.getLong("ttqueue.shard.max.bytes",
        1024*1024*1024);
    this.maxAgeBeforeExpirationInMillis = conf.getLong("ttqueue.entry.age.max",
        120 * 1000); // 120 seconds default
    this.maxAgeBeforeSemiAckedToAcked = conf.getLong(
        "ttqueue.entry.semiacked.max",
        10 * 1000); // 10 second default
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
      makeColumn(entryId, ENTRY_DATA), makeColumn(entryId, ENTRY_META)
    }, cleanWriteVersion,
    new byte [][] {
      data, new EntryMeta(EntryState.VALID).getBytes()
    });

    // Return success with pointer to entry
    return new EnqueueResult(EnqueueStatus.SUCCESS,
        new QueueEntryPointer(this.queueName, entryId, shardMeta.getShardId()));
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

    if (TRACE)
      log("Attempting dequeue [curNumDequeues=" + this.dequeueReturns.get() + "] (" +
          consumer + ", " + config + ", " + readPointer + ")");

    // Get a dirty pointer
    ImmutablePair<ReadPointer,Long> dirty = dirtyPointer();

    // Loop until we have properly upserted and verified group information
    GroupState groupState = null;
    byte [] groupListRow = makeRow(GLOBAL_GROUPS_HEADER, -1);
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
        try {
          if (this.table.compareAndSwap(groupRow, GROUP_STATE, existingValue,
              groupState.getBytes(), dirty.getFirst(), dirty.getSecond())) {
            // CAS was successful, we created the group, add us to list, exit loop
            this.table.put(groupListRow, Bytes.toBytes(consumer.getGroupId()),
                dirty.getSecond(), groupState.getBytes());
            break;
          } else {
            // CAS was not successful, someone else created group, loop
            continue;
          }
        } catch (com.continuuity.api.data.OperationException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

      } else {
        // Group information already existed, verify group state
        groupState = GroupState.fromBytes(existingValue);
        if (TRACE) log("Group state already existed: " + groupState);

        // Check group size and execution mode
        if (groupState.getGroupSize() == consumer.getGroupSize() &&
            groupState.getMode() == ExecutionMode.fromQueueConfig(config)) {
          // Group state matches, break from loop using current group head
          break;
        }

        // Group configuration has changed
        if (groupHasPendingEntries(groupState, consumer.getGroupId(),
            dirty.getFirst())) {
          // Group has pending entries but reconfig was attempted, fail
          if (TRACE)
            log("Attempted to change group config but entries pending");
          return new DequeueResult(DequeueStatus.FAILURE,
              "Attempted to change group configuration but group has pending " +
                  "entries not acked");
        }
        
        // Group has no outstanding entries, attempt atomic reconfig
        groupState = new GroupState(consumer.getGroupSize(),
            groupState.getHead(), ExecutionMode.fromQueueConfig(config));
        try {
          if (this.table.compareAndSwap(groupRow, GROUP_STATE, existingValue,
              groupState.getBytes(), dirty.getFirst(), dirty.getSecond())) {
            // Group config update success, update state, break from loop
            this.table.put(groupListRow, Bytes.toBytes(consumer.getGroupId()),
                dirty.getSecond(), groupState.getBytes());
            log("Group config updated successfully!");
            break;
          } else {
            // Update of group meta failed, someone else conflicted, loop
            log("Group config atomic update failed, retry group validate");
            continue;
          }
        } catch (com.continuuity.api.data.OperationException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
        log("Queue is empty, nothing found at " + entryPointer + " using " +
            "read pointer " + dirty.getFirst());
        return new DequeueResult(DequeueStatus.EMPTY);
      }

      // Entry exists, check if it should actually be visible
      if (!readPointer.isVisible(entryMetaDataAndStamp.getSecond())) {
        // This is currently being enqueued in an uncommitted transaction,
        // wait and loop back without changing entry pointer
        log("Data exists but is not yet visible at " + entryPointer +
            ", retrying");
        quickWait();
        return new DequeueResult(DequeueStatus.RETRY);
      }

      // Queue entry exists and is visible, check the global state of it
      entryMeta = EntryMeta.fromBytes(entryMetaDataAndStamp.getFirst());
      if (TRACE) log("entryMeta : " + entryMeta.toString());

      // Check if entry has been invalidated or evicted
      if (entryMeta.isInvalid() || entryMeta.isEvicted()) {
        // Invalidated.  Check head update and move to next entry in this shard
        if (TRACE) log("Found invalidated or evicted entry at " + entryPointer +
            " (" + entryMeta.toString() + ")");
        EntryPointer nextEntryPointer = new EntryPointer(
            entryPointer.getEntryId() + 1, entryPointer.getShardId());
        if (entryPointer.equals(groupState.getHead())) {
          // The head is invalid, attempt to move head down
          GroupState newGroupState = new GroupState(groupState.getGroupSize(),
              nextEntryPointer, groupState.getMode());
          try {
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
          } catch (com.continuuity.api.data.OperationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
          }
        }
        // This entry is invalid, move to the next entry and loop
        entryPointer = nextEntryPointer;
        continue;
      }

      // Check if entry is an end-of-shard marker
      if (entryMeta.isEndOfShard()) {
        // Entry is an indicator that this is the end of the shard, move
        // to the next shardId with the same entryId, check head update
        if (TRACE) log("Found endOfShard marker to jump from " + entryPointer.getShardId()
            + " to " + (entryPointer.getShardId() + 1));
        EntryPointer nextEntryPointer = new EntryPointer(
            entryPointer.getEntryId(), entryPointer.getShardId() + 1);
        if (entryPointer.equals(groupState.getHead())) {
          // The head is an end-ofshard, attempt to move head down
          GroupState newGroupState = new GroupState(groupState.getGroupSize(),
              nextEntryPointer, groupState.getMode());
          try {
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
          } catch (com.continuuity.api.data.OperationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
        if (TRACE) log("Group has not processed entry at id " + entryPointer.getEntryId());
        entryGroupMetaData = null;
      } else {
        entryGroupMeta = EntryGroupMeta.fromBytes(entryGroupMetaData);
        if (TRACE) log("Group has already seen entry at id " + entryPointer.getEntryId() +
            ", groupMeta = " + entryGroupMeta.toString());

        // Check if group has already acked/semi-acked this entry
        if (entryGroupMeta.isAckedOrSemiAcked()) {
          if (TRACE) log("Entry is acked/semi-acked");
          // Group has acked this, check head, move to next entry in shard
          EntryPointer nextEntryPointer = new EntryPointer(
              entryPointer.getEntryId() + 1, entryPointer.getShardId());
          if (entryPointer.equals(groupState.getHead()) &&
              safeToMoveHead(entryGroupMeta)) {
            // The head is acked for this group, attempt to move head down
            GroupState newGroupState = new GroupState(groupState.getGroupSize(),
                nextEntryPointer, groupState.getMode());
            try {
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
            } catch (com.continuuity.api.data.OperationException e) {
              e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
          }
          // This entry is acked, move to the next entry and loop
          entryPointer = nextEntryPointer;
          continue;
        }

        // Check if entry is currently dequeued by group
        if (entryGroupMeta.isDequeued()) {
          // Entry is dequeued, check if it is for us, expired, etc.
          if (TRACE) log("Entry is dequeued already");

          if (config.isSingleEntry() &&
              entryGroupMeta.getInstanceId() == consumer.getInstanceId()) {
            // Sync mode, same consumer, try to update stamp and give it back
            if (TRACE) log("Sync mode, same consumer, update and give back");
            EntryGroupMeta newEntryGroupMeta = new EntryGroupMeta(
                EntryGroupState.DEQUEUED, now(), consumer.getInstanceId());
            // Attempt to update with updated timestamp
            try {
              if (this.table.compareAndSwap(shardRow, entryGroupMetaColumn,
                  entryGroupMetaData, newEntryGroupMeta.getBytes(),
                  dirty.getFirst(), dirty.getSecond())) {
                // Successfully updated timestamp, still own it, return this
                this.dequeueReturns.incrementAndGet();
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
            } catch (com.continuuity.api.data.OperationException e) {
              e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
          }

          if (entryGroupMeta.isDequeued() &&
              entryGroupMeta.getTimestamp() +
                this.maxAgeBeforeExpirationInMillis >= now()) {
            log("Entry is dequeued but not expired! (entryGroupMetaTS=" +
                entryGroupMeta.getTimestamp() + ", maxAge=" +
                this.maxAgeBeforeExpirationInMillis + ", now=" + now());
            // Entry is dequeued and not expired, move to next entry in shard
            entryPointer = new EntryPointer(
                entryPointer.getEntryId() + 1, entryPointer.getShardId());
            continue;
          }
        }
      }

      // Entry is available for this consumer and group
      if (TRACE) log("Fell through, entry is available!");

      // Get the data and check the partitioner
      byte [] data = this.table.get(shardRow,
          makeColumn(entryPointer.getEntryId(), ENTRY_DATA), dirty.getFirst());
      assert(data != null);
      if (!config.getPartitioner().shouldEmit(consumer,
          entryPointer.getEntryId(), data)) {
        // Partitioner says skip, flag as available, move to next entry in shard
        if (TRACE) log("Partitioner rejected this entry, skip");
        entryPointer = new EntryPointer(
            entryPointer.getEntryId() + 1, entryPointer.getShardId());
        continue;
      }

      // Atomically update group meta to point to this consumer and return!
      EntryGroupMeta newEntryGroupMeta = new EntryGroupMeta(
          EntryGroupState.DEQUEUED, now(), consumer.getInstanceId());
      try {
        if (this.table.compareAndSwap(shardRow, entryGroupMetaColumn,
            entryGroupMetaData, newEntryGroupMeta.getBytes(),
            dirty.getFirst(), dirty.getSecond())) {
          // We own it!  Return it.
          this.dequeueReturns.incrementAndGet();
          if (TRACE) log("Returning " + entryPointer + " with data " + newEntryGroupMeta);
          return new DequeueResult(DequeueStatus.SUCCESS, entryPointer, data);
        } else {
          // Someone else has grabbed it, on to the next one
          if (TRACE) log("\t !!! Got a collision trying to own " + entryPointer);
          entryPointer = new EntryPointer(
              entryPointer.getEntryId() + 1, entryPointer.getShardId());
          continue;
        }
      } catch (com.continuuity.api.data.OperationException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
    if (groupMeta.isAvailable() || groupMeta.isAckedOrSemiAcked()) return false;

    // It is in the right state, attempt atomic semi_ack
    // (ack passed if this CAS works, fails if this CAS fails)
    byte [] newValue = new EntryGroupMeta(EntryGroupState.SEMI_ACKED,
        now(), consumer.getInstanceId()).getBytes();
    try {
      return this.table.compareAndSwap(shardRow, groupColumn, existingValue,
          newValue, dirty.getFirst(), dirty.getSecond());
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public boolean finalize(QueueEntryPointer entryPointer,
      QueueConsumer consumer, int totalNumGroups) {
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

    // Should be in semiAcked state
    if (groupMeta != null && !groupMeta.isSemiAcked()) return false;

    // It is in the right state, attempt atomic semi_ack to ack
    // (finalize passed if this CAS works, fails if this CAS fails)
    byte [] newValue = new EntryGroupMeta(EntryGroupState.ACKED,
        now(), consumer.getInstanceId()).getBytes();
    try {
      boolean finalized = this.table.compareAndSwap(shardRow, groupColumn,
          existingValue, newValue, dirty.getFirst(), dirty.getSecond());
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    if (finalized) {
      // We successfully finalized our ack.  Perform evict-on-ack if possible.
      if (totalNumGroups == 1 ||
          (totalNumGroups > 0 && allOtherGroupsFinalized(entryPointer,
              totalNumGroups, consumer.getGroupId(), dirty))) {
        // Evict!
        byte [] entryMetaColumn = makeColumn(entryPointer.getEntryId(),
            ENTRY_META);
        this.table.put(shardRow, entryMetaColumn, dirty.getSecond(),
            new EntryMeta(EntryState.EVICTED).getBytes());
      }
    }
    return finalized;
  }

  private boolean allOtherGroupsFinalized(QueueEntryPointer entryPointer,
      int totalNumGroups, long curGroup,
      ImmutablePair<ReadPointer,Long> dirtyPointer) {
    byte [] shardRow = makeRow(GLOBAL_DATA_HEADER, entryPointer.getShardId());
    for (long groupId = 1; groupId <= totalNumGroups ; groupId++) {
      if (groupId == curGroup) continue;
      byte [] groupColumn = makeColumn(entryPointer.getEntryId(),
          ENTRY_GROUP_META, groupId);
      byte [] existingValue = this.table.get(shardRow, groupColumn,
          dirtyPointer.getFirst());
      if (existingValue == null || existingValue.length == 0) return false;
      EntryGroupMeta groupMeta = EntryGroupMeta.fromBytes(existingValue);
      if (!groupMeta.isAcked()) return false;
    }
    return true;
  }

  @Override
  public boolean unack(QueueEntryPointer entryPointer,
      QueueConsumer consumer) {
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

    // Should be in semiAcked state
    if (groupMeta != null && !groupMeta.isSemiAcked()) return false;

    // It is in the right state, attempt atomic semi_ack to dequeued
    // (finalize passed if this CAS works, fails if this CAS fails)
    byte [] newValue = new EntryGroupMeta(EntryGroupState.DEQUEUED,
        now(), consumer.getInstanceId()).getBytes();
    try {
      return this.table.compareAndSwap(shardRow, groupColumn, existingValue,
          newValue, dirty.getFirst(), dirty.getSecond());
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  // Public accessors

  VersionedColumnarTable getTable() {
    return this.table;
  }

  byte [] getQueueName() {
    return this.queueName;
  }

  // Private helpers

  private boolean safeToMoveHead(EntryGroupMeta entryGroupMeta) {
    return entryGroupMeta.isAcked() ||
        (entryGroupMeta.isSemiAcked() &&
            entryGroupMeta.getTimestamp() + this.maxAgeBeforeSemiAckedToAcked <=
            now());
  }

  /**
   * Checks if the specified group has any currently pending entries (entries
   * that have been dequeued but not acked).
   * @param groupState
   * @param readPointer
   * @return true if there are pending entries, false if no pending entries
   */
  private boolean groupHasPendingEntries(GroupState groupState, long groupId,
      ReadPointer readPointer) {
    EntryPointer curEntry = groupState.getHead();
    while (curEntry != null) {
      // We are pointed at {entryPointer}=(shardid,entryid) and we are either
      // at the head of this group or we have skipped everything between where
      // we are and the head.
      byte [] shardRow = makeRow(GLOBAL_DATA_HEADER, curEntry.getShardId());
      byte [] entryMetaColumn = makeColumn(curEntry.getEntryId(), ENTRY_META);

      // Do a dirty read of the entry meta data
      ImmutablePair<byte[],Long> entryMetaDataAndStamp =
          this.table.getWithVersion(shardRow, entryMetaColumn, readPointer);
      if (entryMetaDataAndStamp == null) {
        // Entry does not exist, if we haven't found a pending entry by now
        // then we are good (no pending entries, return false)
        return false;
      }
      
      // Entry exists, check if it should actually be visible
      if (!readPointer.isVisible(entryMetaDataAndStamp.getSecond())) {
        // We have reached a point of an actively being written queue entry
        // No consumers will have ever gotten past here so if we make it here
        // then we are good (no pending entries, return false)
        return false;
      }

      // Queue entry exists and is visible, check the global state of it
      EntryMeta entryMeta = EntryMeta.fromBytes(
          entryMetaDataAndStamp.getFirst());
      
      if (entryMeta.isEndOfShard()) {
        // Move to same entry in next shard
        curEntry = new EntryPointer(curEntry.getEntryId(),
            curEntry.getShardId() + 1);
        continue;
      }
      
      if (entryMeta.isInvalid() || entryMeta.isEvicted()) {
        // Move to next entry in same shard
        curEntry = new EntryPointer(curEntry.getEntryId() + 1,
            curEntry.getShardId());
        continue;
      }
      
      byte [] entryGroupMetaColumn = makeColumn(curEntry.getEntryId(),
          ENTRY_GROUP_META, groupId);
      byte[] entryGroupMetaData = this.table.get(shardRow, entryGroupMetaColumn,
          readPointer);
      if (entryGroupMetaData == null || entryGroupMetaData.length == 0) {
        // Group has not processed this entry yet, consider available for now
        // There can currently be gaps in entries without group data in the case
        // of using a hash partitioner.
        // TODO: Optimize this (ENG-416)
        curEntry = new EntryPointer(curEntry.getEntryId() + 1,
            curEntry.getShardId());
        continue;
      } else {
        EntryGroupMeta entryGroupMeta =
            EntryGroupMeta.fromBytes(entryGroupMetaData);
        // If we have an entry that is in a dequeued state, pending entry!
        if (entryGroupMeta.isDequeued()) {
          if (TRACE)
            log("In pending entry check, found dequeued entry : " +
                entryGroupMeta + " , " + curEntry);
          return true;
        }
        // Otherwise, it's either available or acked, both are okay, move entry
        curEntry = new EntryPointer(curEntry.getEntryId() + 1,
            curEntry.getShardId());
        continue;
      }
    }
    // If we reach here, something has gone wrong
    throw new RuntimeException("Bug in TTQueue groupHasPendingEntries check");
  }

  @SuppressWarnings("unused")
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

  public static boolean TRACE = false;

  private void log(String msg) {
    if (TRACE) System.out.println(Thread.currentThread().getId() + " : " + msg);
    // LOG.debug(msg);
  }

  private static byte [] bytes(byte b) {
    return new byte [] { b };
  }

  @Override
  public long getGroupID() {
    // Get a dirty pointer
    ImmutablePair<ReadPointer,Long> dirty = dirtyPointer();
    // Get our unique entry id
    long groupId = this.table.increment(makeRow(GLOBAL_GROUPS_HEADER),
        GROUP_ID_GEN, 1, dirty.getFirst(), dirty.getSecond());
    return groupId;
  }

  @Override
  public QueueMeta getQueueMeta() {

    // Get global queue state information

    QueueMeta meta = new QueueMeta();
    
    ImmutablePair<ReadPointer,Long> dirty = dirtyPointer();
    long nextEntryId = Bytes.toLong(this.table.get(makeRow(GLOBAL_ENTRY_HEADER),
        GLOBAL_ENTRYID_COUNTER, dirty.getFirst()));
    meta.globalHeadPointer = nextEntryId;

    byte [] entryWritePointerRow = makeRow(GLOBAL_ENTRY_WRITEPOINTER_HEADER);
    long curEntryLock = getCounter(entryWritePointerRow,
        GLOBAL_ENTRYID_WRITEPOINTER_COUNTER, dirty.getFirst());
    meta.currentWritePointer = curEntryLock;

    // Get group state information
    byte [] groupListRow = makeRow(GLOBAL_GROUPS_HEADER, -1);
    
    // Do a dirty read of the global group information
    Map<byte[],byte[]> groups = this.table.get(groupListRow, dirty.getFirst());
    if (groups == null || groups.isEmpty()) {
      meta.groups = null;
      return meta;
    }
    
    meta.groups = new GroupState[groups.size()];
    int i=0;
    for (Map.Entry<byte[],byte[]> entry : groups.entrySet()) {
      meta.groups[i] = GroupState.fromBytes(entry.getValue());
    }
    return meta;
  }

  public String getInfo(int groupId) {

    StringBuilder sb = new StringBuilder();
    sb.append("TTQueueONVCTable (" + Bytes.toString(this.queueName) + ")\n");
    sb.append("DequeueReturns = " + this.dequeueReturns.get() + "\n");

    // Get global queue state information

    ImmutablePair<ReadPointer,Long> dirty = dirtyPointer();
    long nextEntryId = Bytes.toLong(this.table.get(makeRow(GLOBAL_ENTRY_HEADER),
        GLOBAL_ENTRYID_COUNTER, dirty.getFirst()));
    sb.append("Next available entryId: " + nextEntryId + "\n");

    byte [] entryWritePointerRow = makeRow(GLOBAL_ENTRY_WRITEPOINTER_HEADER);
    long curEntryLock = getCounter(entryWritePointerRow,
        GLOBAL_ENTRYID_WRITEPOINTER_COUNTER, dirty.getFirst());
    sb.append("Currently locked entryId: " + curEntryLock + "\n");

    byte [] shardMetaRow = makeRow(GLOBAL_SHARDS_HEADER);
    ShardMeta shardMeta = ShardMeta.fromBytes(this.table.get(shardMetaRow,
        GLOBAL_SHARD_META, dirty.getFirst()));
    sb.append("Shard meta: " + shardMeta.toString() + "\n");


    // Get group state information
    sb.append("\nGroup State Info (groupid= " + groupId + ")\n");

    byte [] groupRow = makeRow(GLOBAL_GROUPS_HEADER, groupId);
    // Do a dirty read of the global group information
    byte [] existingValue = this.table.get(groupRow, GROUP_STATE,
        dirty.getFirst());

    if (existingValue == null || existingValue.length == 0) {
      sb.append("No group info exists!\n");
    } else {
      // Group information already existed, verify group state
      GroupState groupState = GroupState.fromBytes(existingValue);
      sb.append(groupState.toString() + "\n");
    }

    return sb.toString();
  }

  public String getEntryInfo(long entryId) {

    long curShard = 1;
    long curEntry = 1;
    ReadPointer rp = new MemoryReadPointer(Long.MAX_VALUE);
    while (true) {

      byte [] shardRow = makeRow(GLOBAL_DATA_HEADER, curShard);
      byte [] entryMetaColumn = makeColumn(curEntry, ENTRY_META);

      // Do a dirty read of the entry meta data
      ImmutablePair<byte[],Long> entryMetaDataAndStamp =
          this.table.getWithVersion(shardRow, entryMetaColumn, rp);

      if (entryMetaDataAndStamp == null) {
        if (entryId == curEntry) {
          return "Iterated all the way to specified entry but that did not " +
              "exist (entry " + curEntry + " in shard " + curShard + ")";
        } else {
          return "Didn't iterate to the specified entry (" + entryId + ") ," +
              "tripped over empty entry at (entry " + curEntry + " in shard " +
              curShard + ")";
        }
      }

      EntryMeta entryMeta =
          EntryMeta.fromBytes(entryMetaDataAndStamp.getFirst());

      if (curEntry == entryId) {
        return "Found entry at " + entryId + " in shard " + curShard + " (" +
            entryMeta.toString() + ") with timestamp = " +
            entryMetaDataAndStamp.getSecond();
      }

      if (entryMeta.isInvalid() || entryMeta.isValid()) {
        // Move to next entry
        curEntry++;
      } else if (entryMeta.isEndOfShard()) {
        // Move to next shard
        curShard++;
      }
    }
  }
}
