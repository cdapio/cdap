package com.continuuity.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.IteratorBasedSplitReader;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.Scanner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Implementation of Stream as a dataset. In the current implementation Streams are backed by queues for data storage.
 * Stream metadata is stored in StreamMeta table:
 *   - Stream Meta RowKey is constructed with streamname and offset - with last 8 bytes used for offset and rest for
 *      stream name. Offsets are time in seconds when the meta entry is written to StreamMeta table.
 *   - Value that is stored in column "o" for the row is QueueEntryPointer
 *   - StreamMeta data is stored in configurable intervals - by default the interval is 1 hour
 */
public class Stream extends DataSet
                    implements BatchReadable<byte[], QueueEntry>, BatchWritable<byte[], QueueEntry> {
  //TODO: Move StreamEvent to data-fabric and use StreamEntry instead of queue entry
  private final byte [] streamMetaTable =  Bytes.toBytes("streamMeta");
  private final byte [] streamTableName ;
  private long startTime = Long.MIN_VALUE;
  private long endTime = Long.MAX_VALUE;
  //TODO: Wire in the readPointer from map-reduce job
  private ReadPointer readPointer = TransactionOracle.DIRTY_READ_POINTER;

  @Inject
  OVCTableHandle handle;

  /**
   * Construct Stream with StreamName and Table handle
   * @param name  String - name of the stream
   * @param handle OVCTableHandle
   */
  public Stream(String name, OVCTableHandle handle) {
    super(name);
    this.streamTableName = Bytes.toBytes(String.format("stream://%s",name));
    this.handle = handle;
  }

  /**
   * Set Start time for streams. Any read operation using stream will use the start time to determine the position
   * to read from. The first entry that is read from the stream will be greater than or equal to the entry corresponding
   * to the start time
   * @param startTime Start timee
   */
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * Set the endTime to stop reading from the streams. Streams will stop reading from the last available endTime that
   * is available in the StreamMeta table is less than or equal to the endTime - i.e., it will only read complete
   * partitions.
   * @param endTime EndTime
   */
  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  /**
   * Returns all splits of the dataset.
   * <p>
   * Used to feed whole dataset into batch job.
   * </p>
   *
   * @return list of {@link com.continuuity.api.data.batch.Split}
   */
  @Override
  public List<Split> getSplits() {
    List<Split> splits = Lists.newArrayList();
    try {
      OrderedVersionedColumnarTable metaTable  = handle.getTable(streamMetaTable);
      byte [] startRow = StreamMeta.makeStreamMetaRowKey(streamTableName,startTime);
      byte [] stopRow = StreamMeta.makeStreamMetaRowKey(streamTableName,endTime);

      Scanner scanner = metaTable.scan(startRow,stopRow,readPointer);

      if (scanner == null) {
        return splits;
      }

      ImmutablePair<byte[], Map<byte[], byte[]>> startEntry = scanner.next();

      if (startEntry == null) {
        return splits;
      }

      ImmutablePair<byte[], Map<byte[],byte[]>> nextEntry;
      while ( (nextEntry=scanner.next()) !=null ) {

        byte [] startQueueEntryBytes  = startEntry.getSecond().get(StreamMeta.getOffsetColumn());
        byte [] nextQueueEntryBytes  = nextEntry.getSecond().get(StreamMeta.getOffsetColumn());

        if ( startQueueEntryBytes == null || nextQueueEntryBytes == null) {
          throw new RuntimeException(String.format("QueueEntry pointer not found in stream meta table for stream: %s",
                                                                                                     this.getName()));
        }

        QueueEntryPointer start = QueueEntryPointer.fromBytes(startQueueEntryBytes);
        QueueEntryPointer end = QueueEntryPointer.fromBytes(nextQueueEntryBytes);
        StreamInputSplit split = new StreamInputSplit(start,end);
        splits.add(split);

        startEntry = nextEntry;

      }
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
    return splits;
  }

  /**
   * Creates reader for the split of dataset.
   *
   * @param split split to create reader for.
   * @return instance of a {@link com.continuuity.api.data.batch.SplitReader}
   */
  @Override
  public SplitReader<byte[], QueueEntry> createSplitReader(Split split) {
    if(split instanceof StreamInputSplit) {
      return new StreamReader();
    } else {
      throw new RuntimeException("Only stream input split is expected");
    }
  }

  /**
   * Write to streams in batch. Not implemented yet
   * @param bytes  bytes key
   * @param streamEntry  QueueEntry
   * @throws OperationException
   */
  @Override
  public void write(byte[] bytes, QueueEntry streamEntry) throws OperationException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * This method is called at deployment time and must return the complete
   * specification that is needed to instantiate the data set at runtime
   * (@see #DataSet(DataSetSpecification)).
   *
   * @return a data set spec that has all meta data needed for runtime
   *         instantiation
   */
  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this).create();
  }

  /**
   * Iterator of QueueEntry with entries within start to end QueueEntryPointers
   * @param start QueueEntryPointer
   * @param end QueueEntryPointer
   * @return {@code Iterator} of QueueEntrie
   * @throws OperationException
   */
  public Iterator<QueueEntry> iterator(QueueEntryPointer start, QueueEntryPointer end) throws OperationException {
    byte [] name = Bytes.toBytes("streams");
    TTQueueTable table = this.handle.getStreamTable(name);
    return table.getIterator(this.streamTableName, start, end);
  }


  /**
   * Defines StreamInputSplit which contains Start and end queue entry pointers
   */
  public static final class StreamInputSplit extends Split {

    private final QueueEntryPointer startQueueEntryPointer;
    private final QueueEntryPointer endQueueEntryPointer;

    public StreamInputSplit(QueueEntryPointer start, QueueEntryPointer end) {
      this.startQueueEntryPointer = start;
      this.endQueueEntryPointer = end;
    }

    public QueueEntryPointer getStartQueueEntryPointer() {
      return startQueueEntryPointer;
    }

    public QueueEntryPointer getEndQueueEntryPointer() {
      return endQueueEntryPointer;
    }
  }


  public static final class StreamReader extends IteratorBasedSplitReader<byte[], QueueEntry> {

    public static final byte [] dummyKey = new byte[0];

    /**
     * Creates iterator to iterate through all records of a given split
     *
     * @param dataset dataset that owns a split
     * @param split   split to iterate through
     * @return an instance of {@link java.util.Iterator}
     * @throws com.continuuity.api.data.OperationException
     *          if there's an error during reading the split
     */
    @Override
    protected Iterator<QueueEntry> createIterator(BatchReadable dataset, Split split) throws OperationException {
      if(split instanceof StreamInputSplit) {
        StreamInputSplit streamInputSplit = (StreamInputSplit) split;
        return ((Stream)dataset).iterator(streamInputSplit.getStartQueueEntryPointer(),
                                          streamInputSplit.getEndQueueEntryPointer());
      } else {
        throw new RuntimeException("Only stream input split is expected");
      }
    }

    /**
     * Gets key from the given value provided by iterator. Note this function just returns a dummy value for now
     * since queue entry doesn't have a notion of a key
     *
     * @param streamEntry value to get key from
     * @return key
     */
    @Override
    protected byte[] getKey(QueueEntry streamEntry) {
      return dummyKey;
    }
  }

  /**
   * Utility functions for StreamMeta operations that are shared by Streams and StreamMetaOracle
   * - Creating rowKey for storing Stream offsets
   * - Get offset columns
   */
  public static class StreamMeta {


    private static final byte [] streamOffsetColumn = org.apache.hadoop.hbase.util.Bytes.toBytes("o") ;

    public static byte [] getOffsetColumn(){
      return streamOffsetColumn;
    }

    /**
     * Get Stream Meta key for the given stream and offset
     * @param streamName  name of the stream
     * @param offset  offset to be stored
     * @return byte array of stream meta rowkey
     */
   public static byte[] makeStreamMetaRowKey(byte[] streamName, long offset){
      return Bytes.add(streamName, Bytes.toBytes(offset));
    }
  }
}
