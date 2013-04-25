package com.continuuity.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.IteratorBasedSplitReader;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.internal.EntryPointer;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * Implementation of Stream as a dataset. Streams are currently backed by queues for data storage
 *
 */
public class Stream extends DataSet
                    implements BatchReadable<byte[], QueueEntry>, BatchWritable<byte[], QueueEntry> {
  //TODO: Use StreamEvent instead of QueueEntry - Using StreamEvent brings in app-fabric dependency to data-fabric
  private final byte [] streamMetaTable =  Bytes.toBytes("streamMeta"); //"streamMeta".getBytes(Charsets.UTF_8);
  private final byte [] streamOffsetColumn = org.apache.hadoop.hbase.util.Bytes.toBytes("o") ;
  private final byte [] streamTableName ;
  private long startPartition = Long.MIN_VALUE;
  private long endPartition = Long.MAX_VALUE;

  @Inject
  OVCTableHandle handle;

  public Stream(String name, OVCTableHandle handle) {
    super(name);
    this.streamTableName = Bytes.toBytes(String.format("stream://%s",name));
    this.handle = handle;
  }

  public void setStartPartition(long offset) {
    this.startPartition = offset;
  }

  public void setEndPartition(long offset) {
    this.endPartition = offset;
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

      List<byte[]> keys = metaTable.getKeysDirty(Integer.MAX_VALUE);
      List<Long> offsets  = Lists.newArrayList();
      for (byte [] key : keys) {
        long offset = StreamMeta.getTimeOffSet(key);
        if ( offset >= startPartition && offset <= endPartition) {
          offsets.add(offset);
        }
      }
      if (offsets.size() == 1) {
        StreamInputSplit split = new StreamInputSplit(offsets.get(0), Long.MAX_VALUE);
        splits.add(split);
      } else {
        for (int i = 0; i < offsets.size();i++){
          if ( i == offsets.size()-1){
            StreamInputSplit split = new StreamInputSplit(offsets.get(i), Long.MAX_VALUE);
            splits.add(split);
          } else {
            StreamInputSplit split = new StreamInputSplit(offsets.get(i),offsets.get(i+1));
            splits.add(split);
          }
        }
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
   * Iterator of QueueEntry with entries within startOffset to endOffset
   * @param startTime startOffset
   * @param endTime endOffset
   * @return {@code Iterator} of QueueEntries
   * @throws OperationException
   */
  public Iterator<QueueEntry> iterator(long startTime, long endTime) throws OperationException {

    OperationResult<byte[]> result  = handle.getTable(this.streamMetaTable)
                                                      .getCeilValueDirty(StreamMeta.makeStreamMetaRowKey(
                                                                            this.streamTableName,startTime),
                                                                          this.streamOffsetColumn) ;
    QueueEntryPointer begin = null;
    if ( !result.isEmpty()) {
      begin = QueueEntryPointer.fromBytes(result.getValue());
    }

    OperationResult<byte[]> result2  = handle.getTable(this.streamMetaTable)
                                                      .getCeilValueDirty(StreamMeta.makeStreamMetaRowKey(
                                                                           this.streamTableName,endTime),
                                                                         this.streamOffsetColumn);
    QueueEntryPointer end;
    if ( !result2.isEmpty()) {
      end = QueueEntryPointer.fromBytes(result.getValue());
    }  else {
      end = new EntryPointer(Long.MAX_VALUE,Long.MAX_VALUE);
    }

    byte [] name = Bytes.toBytes("streams");
    TTQueueTable table = this.handle.getStreamTable(name);
    return table.getIterator(this.streamTableName, begin, end);

  }


  /**
   * Defines StreamInputSplit which contains start and endtime
   */
  public static final class StreamInputSplit extends Split {
    private final long startTime;
    private final long endTime;

    public StreamInputSplit(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getEndTime() {
      return endTime;
    }
  }


  public static final class StreamReader extends IteratorBasedSplitReader<byte[], QueueEntry> {
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
        return ((Stream)dataset).iterator(streamInputSplit.getStartTime(),streamInputSplit.getEndTime());
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
      return new byte[0];
    }
  }

  /**
   * Utility functions for StreamMeta operations
   * - Creating rowKey for storing Stream offsets
   * - Get offset values from keys
   */
  public static class StreamMeta {

    private static final byte [] rowKeySeparator = Bytes.toBytes("--");

    /**
     * Get time offsets with the given streamMetaKey
     * @param streamMetaRowkey stream meta key
     * @return time offset
     */
    public static long getTimeOffSet(byte[] streamMetaRowkey) {
      Preconditions.checkNotNull(streamMetaRowkey);

      int index = Bytes.indexOf(streamMetaRowkey,rowKeySeparator);
      int offsetStartIndex = index + rowKeySeparator.length;

      Preconditions.checkArgument(index != -1, "Row key has invalid format");
      Preconditions.checkArgument(offsetStartIndex < streamMetaRowkey.length, "Row key has invalid format" );

      byte [] offset = Arrays.copyOfRange(streamMetaRowkey, offsetStartIndex, streamMetaRowkey.length );
      return Bytes.toLong(offset);
    }

    /**
     * Get Stream Meta key for the given stream and offset
     * @param streamName  name of the stream
     * @param offset  offset to be stored
     * @return byte array of stream meta rowkey
     */
   public static byte[] makeStreamMetaRowKey(byte[] streamName, long offset){
      return Bytes.add(streamName, rowKeySeparator, Bytes.toBytes(offset));
    }
  }
}
