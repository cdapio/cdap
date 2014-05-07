/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.file.FileReader;
import com.continuuity.data.file.ReadFilter;
import com.continuuity.data.stream.ForwardingStreamEvent;
import com.continuuity.data.stream.StreamEventOffset;
import com.continuuity.data.stream.StreamFileOffset;
import com.continuuity.data.stream.StreamUtils;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.ConsumerEntryState;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StreamConsumer} that read events from stream file and uses a table to store consumer states.
 *
 * <p>
 * State table ROW key schema:
 *
 * <pre>{@code
 *   row_key = <group_id> <stream_file_offset>
 *   group_id = 8 bytes consumer group id
 *   stream_file_offset = <partition_start> <partition_end> <name_prefix> <sequence_id> <offset>
 *   partition_start = 8 bytes timestamp of partition start time
 *   partition_end = 8 bytes timestamp of partition end time
 *   name_prefix = DataOutput UTF-8 output.
 *   sequence_id = 4 bytes stream file sequence id
 *   offset = 8 bytes offset inside the stream file
 * }</pre>
 *
 * The state table has single column
 * ({@link QueueEntryRow#COLUMN_FAMILY}:{@link QueueEntryRow#STATE_COLUMN_PREFIX}) to store state.
 *
 * The state value:
 *
 * <pre>{@code
 *   state_value = <write_pointer> <instance_id> <state>
 *   write_pointer = 8 bytes Transaction write point of the consumer who update this state.
 *   instance_id = 4 bytes Instance id of the consumer who update this state.
 *   state = ConsumerEntryState.getState(), either CLAIMED or PROCESSED
 * }</pre>
 *
 */
@NotThreadSafe
public abstract class AbstractStreamFileConsumer implements StreamConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamFileConsumer.class);
  private static final HashFunction ROUND_ROBIN_HASHER = Hashing.murmur3_32();

  // Persist state at most once per second.
  private static final long STATE_PERSIST_MIN_INTERVAL = TimeUnit.SECONDS.toNanos(1);

  private static final DequeueResult<StreamEvent> EMPTY_RESULT = DequeueResult.Empty.result();
  private static final Function<PollStreamEvent, byte[]> EVENT_ROW_KEY = new Function<PollStreamEvent, byte[]>() {
    @Override
    public byte[] apply(PollStreamEvent input) {
      return input.getStateRow();
    }
  };
  private static final Function<PollStreamEvent, StreamEventOffset> CONVERT_STREAM_EVENT_OFFSET =
    new Function<PollStreamEvent, StreamEventOffset>() {
      @Override
      public StreamEventOffset apply(PollStreamEvent input) {
        return input.getStreamEventOffset();
      }
    };
  private static final Function<PollStreamEvent, StreamEvent> CONVERT_STREAM_EVENT =
    new Function<PollStreamEvent, StreamEvent>() {
      @Override
      public StreamEvent apply(PollStreamEvent input) {
        return input;
      }
    };

  protected final byte[] stateColumnName;

  private final QueueName streamName;
  private final StreamConfig streamConfig;
  private final ConsumerConfig consumerConfig;
  private final StreamConsumerStateStore consumerStateStore;
  private final FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader;
  private final ReadFilter readFilter;

  // Map from row key prefix (row key without last eight bytes offset) to
  // a sorted map of row key to state value when first encountered a given row prefix.
  // The rows are only needed for entries that are already in the state table when this consumer start.
  private final Map<byte[], SortedMap<byte[], byte[]>> entryStates;

  private final StreamConsumerState consumerState;
  private final List<StreamEventOffset> eventCache;
  private Transaction transaction;
  private List<PollStreamEvent> polledEvents;
  private long nextPersistStateTime;
  private boolean committed;
  private boolean closed;
  private StreamConsumerState lastPersistedState;

  /**
   *
   * @param streamConfig Stream configuration.
   * @param consumerConfig Consumer configuration.
   * @param reader For reading stream events. This class is responsible for closing the reader.
   * @param consumerStateStore The state store for saving consumer state
   * @param beginConsumerState Consumer state to begin with.
   */
  protected AbstractStreamFileConsumer(StreamConfig streamConfig, ConsumerConfig consumerConfig,
                                       FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
                                       StreamConsumerStateStore consumerStateStore,
                                       StreamConsumerState beginConsumerState) {

    LOG.info("Create consumer {}, reader offsets: {}", consumerConfig, reader.getPosition());
    this.streamName = QueueName.fromStream(streamConfig.getName());
    this.streamConfig = streamConfig;
    this.consumerConfig = consumerConfig;
    this.consumerStateStore = consumerStateStore;
    this.reader = reader;
    this.readFilter = createReadFilter(consumerConfig);

    // Use a special comparator to only compare the row prefix, hence different row key can be used directly against
    // this map, hence reduce the need for byte[] array creation.
    this.entryStates = Maps.newTreeMap(new Comparator<byte[]>() {
      @Override
      public int compare(byte[] bytes1, byte[] bytes2) {
        // Compare row keys without the offset part (last 8 bytes).
        return Bytes.compareTo(bytes1, 0, bytes1.length - Longs.BYTES, bytes2, 0, bytes2.length - Longs.BYTES);
      }
    });

    this.eventCache = Lists.newArrayList();
    this.consumerState = beginConsumerState;
    this.lastPersistedState = new StreamConsumerState(beginConsumerState);
    this.stateColumnName = Bytes.add(QueueEntryRow.STATE_COLUMN_PREFIX, Bytes.toBytes(consumerConfig.getGroupId()));
  }

  protected void doClose() throws IOException {
    // No-op.
  }

  protected abstract boolean claimFifoEntry(byte[] row, byte[] value, byte[] oldValue) throws IOException;

  protected abstract void updateState(Iterable<byte[]> rows, int size, byte[] value) throws IOException;

  protected abstract void undoState(Iterable<byte[]> rows, int size) throws IOException;

  protected abstract StateScanner scanStates(byte[] startRow, byte[] endRow) throws IOException;

  @Override
  public final QueueName getStreamName() {
    return streamName;
  }

  @Override
  public final ConsumerConfig getConsumerConfig() {
    return consumerConfig;
  }

  @Override
  public final DequeueResult<StreamEvent> poll(int maxEvents, long timeout,
                                               TimeUnit timeoutUnit) throws IOException, InterruptedException {

    // Only need the CLAIMED state for FIFO with group size > 1.
    byte[] fifoStateContent = null;
    if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
      fifoStateContent = encodeStateColumn(ConsumerEntryState.CLAIMED);
    }

    // Try to read from cache if any
    if (!eventCache.isEmpty()) {
      getEvents(eventCache, polledEvents, maxEvents, fifoStateContent);
    }

    if (polledEvents.size() == maxEvents) {
      return new SimpleDequeueResult(polledEvents);
    }

    // Number of events it tries to read by multiply the maxEvents with the group size. It doesn't have to be exact,
    // just a rough estimate for better read throughput.
    // Also, this maxRead is used throughout the read loop below, hence some extra events might be read and cached
    // for next poll call.
    int maxRead = maxEvents * consumerConfig.getGroupSize();

    long timeoutNano = timeoutUnit.toNanos(timeout);
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    // Save the reader position.
    // It's a conservative approach to save the reader position before reading so that no
    // event will be missed upon restart.
    consumerState.setState(reader.getPosition());

    // Read from the underlying file reader
    while (timeoutNano >= 0 && polledEvents.size() < maxEvents) {
      int readCount = reader.read(eventCache, maxRead, timeoutNano, TimeUnit.NANOSECONDS, readFilter);
      timeoutNano -= stopwatch.elapsedTime(TimeUnit.NANOSECONDS);

      if (readCount > 0) {
        getEvents(eventCache, polledEvents, maxEvents - polledEvents.size(), fifoStateContent);
      }
    }

    if (polledEvents.isEmpty()) {
      return EMPTY_RESULT;
    } else {
      return new SimpleDequeueResult(polledEvents);
    }
  }

  @Override
  public final void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      persistConsumerState();
      doClose();
    } finally {
      try {
        reader.close();
      } finally {
        consumerStateStore.close();
      }
    }
  }

  @Override
  public final void startTx(Transaction tx) {
    transaction = tx;
    if (polledEvents == null) {
      polledEvents = Lists.newArrayList();
    } else {
      polledEvents.clear();
    }

    committed = false;
  }

  @Override
  public final Collection<byte[]> getTxChanges() {
    // Guaranteed no conflict in the consumer logic
    return ImmutableList.of();
  }

  @Override
  public final boolean commitTx() throws Exception {
    if (polledEvents.isEmpty()) {
      return true;
    }

    // For each polled events, set the state column to PROCESSED
    updateState(Iterables.transform(polledEvents, EVENT_ROW_KEY), polledEvents.size(),
                encodeStateColumn(ConsumerEntryState.PROCESSED));

    committed = true;
    return true;
  }

  @Override
  public void postTxCommit() {
    long currentNano = System.nanoTime();
    if (currentNano >= nextPersistStateTime) {
      nextPersistStateTime = currentNano + STATE_PERSIST_MIN_INTERVAL;
      persistConsumerState();
    }

    // Cleanup the entryStates map to free up memory
    for (PollStreamEvent event : polledEvents) {
      SortedMap<byte[], byte[]> states = entryStates.get(event.getStateRow());
      if (states != null) {
        states.headMap(event.getStateRow()).clear();
      }
    }
  }

  @Override
  public boolean rollbackTx() throws Exception {
    if (polledEvents.isEmpty()) {
      return true;
    }

    // Reset the consumer state to some earlier persisted state.
    // This is to avoid upon close() is called right after rollback, it recorded uncommitted file offsets.
    consumerState.setState(lastPersistedState.getState());

    // Insert all polled events back to beginning of the eventCache
    eventCache.addAll(0, Lists.transform(polledEvents, CONVERT_STREAM_EVENT_OFFSET));

    // Special case for FIFO. On rollback, put the CLAIMED state into the entry states for claim entry to use.
    byte[] fifoState = null;
    if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
      fifoState = encodeStateColumn(ConsumerEntryState.CLAIMED);
      for (PollStreamEvent event : polledEvents) {
        entryStates.get(event.getStateRow()).put(event.getStateRow(), fifoState);
      }
    }

    // If committed, also need to rollback backing store.
    if (committed) {
      // Special case for FIFO.
      // If group size > 1, need to update the rows states to CLAIMED state with this instance Id.
      // The transaction pointer used for the entry doesn't matter.
      if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
        updateState(Iterables.transform(polledEvents, EVENT_ROW_KEY), polledEvents.size(), fifoState);
      } else {
        undoState(Iterables.transform(polledEvents, EVENT_ROW_KEY), polledEvents.size());
      }
    }

    return true;
  }

  @Override
  public String getName() {
    return toString();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("stream", streamConfig)
      .add("consumer", consumerConfig)
      .toString();
  }

  private ReadFilter createReadFilter(final ConsumerConfig consumerConfig) {
    final int groupSize = consumerConfig.getGroupSize();
    final DequeueStrategy strategy = consumerConfig.getDequeueStrategy();

    if (groupSize == 1 || strategy == DequeueStrategy.FIFO) {
      return ReadFilter.ALWAYS_ACCEPT;
    }

    // For RoundRobin and Hash partition, the claim is done by matching hashCode to instance id.
    // For Hash, to preserve existing behavior, everything route to instance 0.
    // For RoundRobin, the idea is to scatter the events across consumers evenly. Since there is no way to known
    // about the absolute starting point to do true round robin, we employ a good enough hash function on the
    // file offset as a way to spread events across consumers
    final int instanceId = consumerConfig.getInstanceId();

    return new ReadFilter() {
      @Override
      public boolean acceptOffset(long offset) {
        int hashValue = Math.abs(strategy == DequeueStrategy.HASH ? 0 : ROUND_ROBIN_HASHER.hashLong(offset).hashCode());
        return instanceId == (hashValue % groupSize);
      }
    };
  }

  private void getEvents(List<? extends StreamEventOffset> source,
                         List<? super PollStreamEvent> result,
                         int maxEvents, byte[] stateContent) throws IOException {
    Iterator<? extends StreamEventOffset> iterator = Iterators.consumingIterator(source.iterator());
    while (result.size() < maxEvents && iterator.hasNext()) {
      StreamEventOffset event = iterator.next();
      byte[] stateRow = claimEntry(event.getOffset(), stateContent);
      if (stateRow == null) {
        continue;
      }
      result.add(new PollStreamEvent(event, stateRow));
    }
  }

  private void persistConsumerState() {
    try {
      if (lastPersistedState == null || !consumerState.equals(lastPersistedState)) {
        consumerStateStore.save(consumerState);
        lastPersistedState = new StreamConsumerState(consumerState);
      }
    } catch (IOException e) {
      LOG.error("Failed to persist consumer state for consumer {} of stream {}", consumerConfig, getStreamName(), e);
    }
  }

  /**
   * Encodes the value for the state column with the current transaction and consumer information.
   *
   * @param state The state to encode
   * @return The stateContent byte array
   */
  // TODO: This method is copied from AbstractQueue2Consumer. Future effort is needed to unify them.
  private byte[] encodeStateColumn(ConsumerEntryState state) {
    byte[] stateContent = new byte[Longs.BYTES + Ints.BYTES + 1];

    // State column content is encoded as (writePointer) + (instanceId) + (state)
    Bytes.putLong(stateContent, 0, transaction.getWritePointer());
    Bytes.putInt(stateContent, Longs.BYTES, consumerConfig.getInstanceId());
    Bytes.putByte(stateContent, Longs.BYTES + Ints.BYTES, state.getState());

    return stateContent;
  }

  /**
   * Try to claim a stream event offset.
   *
   * @return The row key for writing to the state table if successfully claimed or {@code null} if not claimed.
   */
  private byte[] claimEntry(StreamFileOffset offset, byte[] claimedStateContent) throws IOException {
    ByteArrayDataOutput out = ByteStreams.newDataOutput(50);
    out.writeLong(consumerConfig.getGroupId());
    StreamUtils.encodeOffset(out, offset);
    byte[] row = out.toByteArray();

    SortedMap<byte[], byte[]> rowStates = getInitRowStates(row);

    // See if the entry should be ignored. If it is in the rowStates with null value, then it should be ignored.
    byte[] rowState = rowStates.get(row);
    if (rowStates.containsKey(row) && rowState == null) {
      return null;
    }

    // Only need to claim entry if FIFO and group size > 1
    if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
      return claimFifoEntry(row, claimedStateContent, rowState) ? row : null;
    }

    // For Hash, RR and FIFO with group size == 1, no need to claim and check,
    // as it's already handled by the readFilter
    return row;
  }

  /**
   * Performs initial scan for the given entry key. Scan will perform from the given entry key till the
   * end of entry represented by that stream file (i.e. offset = Long.MAX_VALUE).
   *
   * @param row the entry row key.
   */
  private SortedMap<byte[], byte[]> getInitRowStates(byte[] row) throws IOException {
    SortedMap<byte[], byte[]> rowStates = entryStates.get(row);

    if (rowStates != null) {
      return rowStates;
    }

    // Scan till the end.
    // TODO: Current assumption is that a given consumer instance shouldn't be too far behind.
    byte[] stopRow = Arrays.copyOf(row, row.length);
    rowStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    entryStates.put(row, rowStates);

    // Last 8 bytes are the file offset, make it max value so that it scans till last offset.
    Bytes.putLong(stopRow, stopRow.length - Longs.BYTES, Long.MAX_VALUE);
    StateScanner scanner = scanStates(row, stopRow);
    try {
      while (scanner.nextStateRow()) {
        storeInitState(scanner.getRow(), scanner.getState(), rowStates);
      }
    } finally {
      scanner.close();
    }
    return rowStates;
  }

  private void storeInitState(byte[] row, byte[] stateValue, Map<byte[], byte[]> states) {
    // Logic is adpated from QueueEntryRow.canConsume(), with modification.

    if (stateValue == null) {
      // State value shouldn't be null, as the row is only written with state value.
      return;
    }

    // If state is PROCESSED and committed, need to memorize it so that it can be skipped.
    long stateWritePointer = QueueEntryRow.getStateWritePointer(stateValue);
    ConsumerEntryState state = QueueEntryRow.getState(stateValue);
    if (state == ConsumerEntryState.PROCESSED && transaction.isVisible(stateWritePointer)) {
      // No need to store the state value.
      states.put(row, null);
      return;
    }

    // Special case for FIFO.
    // For group size > 1 case, if the state is not committed, need to memorize current state value for claim entry.
    if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
      int stateInstanceId = QueueEntryRow.getStateInstanceId(stateValue);

      // If the state was written by a consumer that is still live, and not by itself,
      // record the state value as null so that it'll get skipped in the claim entry logic.
      if (stateInstanceId < consumerConfig.getGroupSize() && stateInstanceId != consumerConfig.getInstanceId()) {
        states.put(row, null);
      } else {
        // Otherwise memorize the value for checkAndPut operation in claim entry.
        states.put(row, stateValue);
      }
    }
  }

  /**
   * Scanner for scanning state table.
   */
  protected interface StateScanner extends Closeable {

    boolean nextStateRow() throws IOException;

    byte[] getRow();

    byte[] getState();
  }

  /**
   * Represents a {@link StreamEvent} created by the {@link #poll(int, long, java.util.concurrent.TimeUnit)} call.
   */
  private static final class PollStreamEvent extends ForwardingStreamEvent {

    private final byte[] stateRow;
    private final StreamEventOffset streamEventOffset;

    protected PollStreamEvent(StreamEventOffset streamEventOffset, byte[] stateRow) {
      super(streamEventOffset);
      this.streamEventOffset = streamEventOffset;
      this.stateRow = stateRow;
    }

    public StreamEventOffset getStreamEventOffset() {
      return streamEventOffset;
    }

    @Override
    public ByteBuffer getBody() {
      // Return a copy of the ByteBuffer (not copy of content),
      // so that the underlying stream buffer can be reused (rollback, retries).
      return streamEventOffset.getBody().slice();
    }

    private byte[] getStateRow() {
      return stateRow;
    }
  }

  /**
   * A {@link DequeueResult} returned by {@link #poll(int, long, java.util.concurrent.TimeUnit)}.
   */
  private final class SimpleDequeueResult implements DequeueResult<StreamEvent> {

    private final List<PollStreamEvent> events;

    private SimpleDequeueResult(List<PollStreamEvent> events) {
      this.events = ImmutableList.copyOf(events);
    }

    @Override
    public boolean isEmpty() {
      return events.isEmpty();
    }

    @Override
    public void reclaim() {
      // Copy events back to polledEvents and need to remove them from eventCache
      polledEvents.clear();
      polledEvents.addAll(events);

      eventCache.removeAll(Lists.transform(events, CONVERT_STREAM_EVENT_OFFSET));
    }

    @Override
    public int size() {
      return events.size();
    }

    @Override
    public Iterator<StreamEvent> iterator() {
      return Iterators.transform(events.iterator(), CONVERT_STREAM_EVENT);
    }
  }
}
