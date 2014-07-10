/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.file.FileReader;
import com.continuuity.data.file.ReadFilter;
import com.continuuity.data.file.ReadFilters;
import com.continuuity.data.stream.ForwardingStreamEvent;
import com.continuuity.data.stream.StreamEventOffset;
import com.continuuity.data.stream.StreamFileOffset;
import com.continuuity.data.stream.StreamUtils;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TxConstants;
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
import com.google.common.collect.Sets;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
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

  protected static final int MAX_SCAN_ROWS = 1000;

  // Persist state at most once per second.
  private static final long STATE_PERSIST_MIN_INTERVAL = TimeUnit.SECONDS.toNanos(1);

  private static final DequeueResult<StreamEvent> EMPTY_RESULT = DequeueResult.Empty.result();
  private static final Function<PollStreamEvent, byte[]> EVENT_ROW_KEY = new Function<PollStreamEvent, byte[]>() {
    @Override
    public byte[] apply(PollStreamEvent input) {
      return input.getStateRow();
    }
  };
  // Special comparator to only compare the row prefix, hence different row key can be used directly,
  // reducing the need for byte[] array creation.
  private static final Comparator<byte[]> ROW_PREFIX_COMPARATOR = new Comparator<byte[]>() {
    @Override
    public int compare(byte[] bytes1, byte[] bytes2) {
      // Compare row keys without the offset part (last 8 bytes).
      return Bytes.compareTo(bytes1, 0, bytes1.length - Longs.BYTES, bytes2, 0, bytes2.length - Longs.BYTES);
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

  private final long txTimeoutNano;
  private final QueueName streamName;
  private final StreamConfig streamConfig;
  private final ConsumerConfig consumerConfig;
  private final StreamConsumerStateStore consumerStateStore;
  private final FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader;
  private final ReadFilter readFilter;

  // Map from row key prefix (row key without last eight bytes offset) to a sorted map of row key to state value
  // The rows are only needed for entries that are already in the state table when this consumer start.
  private final Map<byte[], SortedMap<byte[], byte[]>> entryStates;
  private final Set<byte[]> entryStatesScanCompleted;

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
   * @param extraFilter Extra {@link ReadFilter} that is ANDed with default read filter and applied first.
   */
  protected AbstractStreamFileConsumer(CConfiguration cConf,
                                       StreamConfig streamConfig, ConsumerConfig consumerConfig,
                                       FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
                                       StreamConsumerStateStore consumerStateStore,
                                       StreamConsumerState beginConsumerState,
                                       @Nullable ReadFilter extraFilter) {

    LOG.info("Create consumer {}, reader offsets: {}", consumerConfig, reader.getPosition());

    this.txTimeoutNano = TimeUnit.SECONDS.toNanos(cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT,
                                                               TxConstants.Manager.DEFAULT_TX_TIMEOUT));
    this.streamName = QueueName.fromStream(streamConfig.getName());
    this.streamConfig = streamConfig;
    this.consumerConfig = consumerConfig;
    this.consumerStateStore = consumerStateStore;
    this.reader = reader;
    this.readFilter = createReadFilter(consumerConfig, extraFilter);

    this.entryStates = Maps.newTreeMap(ROW_PREFIX_COMPARATOR);
    this.entryStatesScanCompleted = Sets.newTreeSet(ROW_PREFIX_COMPARATOR);

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
    while (polledEvents.size() < maxEvents) {
      int readCount = reader.read(eventCache, maxRead, timeoutNano, TimeUnit.NANOSECONDS, readFilter);
      long elapsedNano = stopwatch.elapsedTime(TimeUnit.NANOSECONDS);
      timeoutNano -= elapsedNano;

      if (readCount > 0) {
        int eventsClaimed = getEvents(eventCache, polledEvents, maxEvents - polledEvents.size(), fifoStateContent);

        // TODO: This is a quick fix for preventing backoff logic in flowlet drive kicks in too early.
        // But it doesn't entirely prevent backoff. A proper fix would have a special state in the dequeue result
        // to let flowlet driver knows it shouldn't have backoff.

        // If able to read some events but nothing is claimed, don't check for normal timeout.
        // Only do short transaction timeout checks.
        if (eventsClaimed == 0 && polledEvents.isEmpty()) {
          if (elapsedNano < (txTimeoutNano / 2)) {
            // If still last than half of tx timeout, continue polling without checking normal timeout.
            continue;
          }
        }
      }

      if (timeoutNano <= 0) {
        break;
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

  private ReadFilter createReadFilter(ConsumerConfig consumerConfig, @Nullable ReadFilter extraFilter) {
    ReadFilter baseFilter = createBaseReadFilter(consumerConfig);

    if (extraFilter != null) {
      return ReadFilters.and(extraFilter, baseFilter);
    } else {
      return baseFilter;
    }
  }

  private ReadFilter createBaseReadFilter(final ConsumerConfig consumerConfig) {
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

  private int getEvents(List<? extends StreamEventOffset> source,
                         List<? super PollStreamEvent> result,
                         int maxEvents, byte[] stateContent) throws IOException {
    Iterator<? extends StreamEventOffset> iterator = Iterators.consumingIterator(source.iterator());
    int eventsClaimed = 0;
    while (result.size() < maxEvents && iterator.hasNext()) {
      StreamEventOffset event = iterator.next();
      byte[] stateRow = claimEntry(event.getOffset(), stateContent);
      if (stateRow == null) {
        continue;
      }
      result.add(new PollStreamEvent(event, stateRow));
      eventsClaimed++;
    }
    return eventsClaimed;
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
   * Returns the initial scanned states for the given entry key.
   *
   * Conceptually scan will perform from the given entry key till the end of entry represented
   * by that stream file (i.e. offset = Long.MAX_VALUE) as indicated by the row prefix (row prefix uniquely identify
   * the stream file).
   * However, due to memory limit, scanning is done progressively until it sees an entry with state value
   * written with transaction write pointer later than the this consumer starts.
   *
   * @param row the entry row key.
   */
  private SortedMap<byte[], byte[]> getInitRowStates(byte[] row) throws IOException {
    SortedMap<byte[], byte[]> rowStates = entryStates.get(row);

    if (rowStates != null) {
      // If scan is completed for this row prefix, simply return the cached entries.
      // Or if the cached states is beyond current row, just return as the caller only use the cached state to do
      // point lookup.
      if (entryStatesScanCompleted.contains(row) || !rowStates.tailMap(row).isEmpty()) {
        return rowStates;
      }
    } else {
      rowStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      entryStates.put(row, rowStates);
    }

    // Scan from the given row till to max file offset
    // Last 8 bytes are the file offset, make it max value so that it scans till last offset.
    byte[] stopRow = Arrays.copyOf(row, row.length);
    Bytes.putLong(stopRow, stopRow.length - Longs.BYTES, Long.MAX_VALUE);

    StateScanner scanner = scanStates(row, stopRow);
    try {
      // Scan until MAX_SCAN_ROWS or exhausted the scanner
      int rowCached = 0;
      while (scanner.nextStateRow() && rowCached < MAX_SCAN_ROWS) {
        if (storeInitState(scanner.getRow(), scanner.getState(), rowStates)) {
          rowCached++;
        }
      }

      // If no row is cached, no need to scan again, as they'll be inserted after this consumer starts
      if (rowCached == 0) {
        entryStatesScanCompleted.add(row);
      }
    } finally {
      scanner.close();
    }
    return rowStates;
  }

  /**
   * Determines if need to cache initial entry states.
   *
   * @param row Entry row key
   * @param stateValue Entry state value
   * @param cache The cache to fill it if the row key and state value needs to be cached.
   * @return {@code true} if the entry is stored into cache, {@code false} if the entry is not stored.
   */
  private boolean storeInitState(byte[] row, byte[] stateValue, Map<byte[], byte[]> cache) {
    // Logic is adpated from QueueEntryRow.canConsume(), with modification.

    if (stateValue == null) {
      // State value shouldn't be null, as the row is only written with state value.
      return false;
    }

    long offset = Bytes.toLong(row, row.length - Longs.BYTES);
    long stateWritePointer = QueueEntryRow.getStateWritePointer(stateValue);

    // If the entry offset is not accepted by the read filter, this consumer won't see this entry in future read.
    // If it is written after the current transaction, it happens with the current consumer config.
    // In both cases, no need to cache
    if (!readFilter.acceptOffset(offset) || stateWritePointer >= transaction.getWritePointer()) {
      return false;
    }

    // If state is PROCESSED and committed, need to memorize it so that it can be skipped.
    ConsumerEntryState state = QueueEntryRow.getState(stateValue);
    if (state == ConsumerEntryState.PROCESSED && transaction.isVisible(stateWritePointer)) {
      // No need to store the state value.
      cache.put(row, null);
      return true;
    }

    // Special case for FIFO.
    // For group size > 1 case, if the state is not committed, need to memorize current state value for claim entry.
    if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
      int stateInstanceId = QueueEntryRow.getStateInstanceId(stateValue);

      // If the state was written by a consumer that is still live, and not by itself,
      // record the state value as null so that it'll get skipped in the claim entry logic.
      if (stateInstanceId < consumerConfig.getGroupSize() && stateInstanceId != consumerConfig.getInstanceId()) {
        cache.put(row, null);
      } else {
        // Otherwise memorize the value for checkAndPut operation in claim entry.
        cache.put(row, stateValue);
      }
      return true;
    }

    return false;
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
