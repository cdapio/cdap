package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.*;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.metrics2.api.CMetrics;
import com.continuuity.metrics2.collector.MetricType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The implementation of a thrift service for operation execution.
 * All operations arrive over the wire as Thrift objects. We must
 * convert ("unwrap") them into data fabric operations, execute using
 * the actual operations executor, and send the results back as
 * ("wrapped") Thrift objects.
 *
 * Why all this conversion (wrap/unwrap), and not define all Operations
 * themselves as Thrift objects?
 * <ul><li>
 *   All the non-thrift executors would have to use the Thrift objects
 * </li><li>
 *   Thrift's object model is too restrictive: it has only limited inheritance
 *   and no overloading
 * </li><li>
 *   Thrift objects are bare-bone, all they have are getters, setters, and
 *   basic object methods.
 * </li></ul>
 */
public class TOperationExecutorImpl
    extends ConverterUtils
    implements TOperationExecutor.Iface {

  private static final Logger Log =
      LoggerFactory.getLogger(TOperationExecutorImpl.class);

  /** the operation executor to use for all operations */
  private OperationExecutor opex;

  /** metrics client */
  private CMetrics metrics =  new CMetrics(MetricType.System);

  /** helper method to create a metrics helper */
  MetricsHelper newHelper(String meter, String histogram) {
    return new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, meter, histogram);
  }
  /** constructor requires the operation executor */
  public TOperationExecutorImpl(OperationExecutor opex) {
    this.opex = opex;
  }

  // Write operations. They all return a boolean, which is
  // always safe to return with Thrift.

  @Override
  public void write(TWrite tWrite) throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_WRITE_REQUESTS,
        Constants.METRIC_WRITE_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TWrite: " + tWrite);

    try {
      Write write = unwrap(tWrite);
      this.opex.execute(write);
      if (Log.isDebugEnabled()) Log.debug("Write successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("Write failed: " + e.getMessage());
      throw wrap(e);
    }

  }

  @Override
  public void delet(TDelete tDelete) throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_DELETE_REQUESTS,
        Constants.METRIC_DELETE_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TDelete: " + tDelete);

    try {
      Delete delete = unwrap(tDelete);
      this.opex.execute(delete);
      if (Log.isDebugEnabled()) Log.debug("Delete successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("Delete failed: " + e.getMessage());
      throw wrap(e);
    }
  }

  @Override
  public void increment(TIncrement tIncrement)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_INCREMENT_REQUESTS,
        Constants.METRIC_INCREMENT_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TIncrement: " + tIncrement);

    try {
      Increment increment = unwrap(tIncrement);
      this.opex.execute(increment);
      if (Log.isDebugEnabled()) Log.debug("Increment successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("Increment failed: " + e.getMessage());
      throw wrap(e);
    }
  }

  @Override
  public void compareAndSwap(TCompareAndSwap tCompareAndSwap)
      throws TException, TOperationException  {

    MetricsHelper helper = newHelper(
        Constants.METRIC_COMPAREANDSWAP_REQUESTS,
        Constants.METRIC_COMPAREANDSWAP_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TCompareAndSwap: " + tCompareAndSwap);

    try {
      CompareAndSwap compareAndSwap = unwrap(tCompareAndSwap);
      this.opex.execute(compareAndSwap);
      if (Log.isDebugEnabled()) Log.debug("CompareAndSwap successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("CompareAndSwap failed: " + e.getMessage());
      throw wrap(e);
    }
  }

  @Override
  public void queueEnqueue(TQueueEnqueue tQueueEnqueue)
      throws TException, TOperationException  {

    MetricsHelper helper = newHelper(
        Constants.METRIC_ENQUEUE_REQUESTS,
        Constants.METRIC_ENQUEUE_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TQueueEnqueue: " + tQueueEnqueue);

    try {
      QueueEnqueue queueEnqueue = unwrap(tQueueEnqueue);
      this.opex.execute(queueEnqueue);
      if (Log.isDebugEnabled()) Log.debug("Enqueue successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("Enqueue failed: " + e.getMessage());
      throw wrap(e);
    }
  }

  @Override
  public void queueAck(TQueueAck tQueueAck)
      throws TException, TOperationException  {

    MetricsHelper helper = newHelper(
        Constants.METRIC_ACK_REQUESTS,
        Constants.METRIC_ACK_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TQueueAck: " + tQueueAck);

    try {
      QueueAck queueAck = unwrap(tQueueAck);
      this.opex.execute(queueAck);
      if (Log.isDebugEnabled()) Log.debug("Ack successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("Ack failed: " + e.getMessage());
      throw wrap(e);
    }
  }

  // batch write, return a structure and never null, and is thus safe

  @Override
  public void batch(List<TWriteOperation> batch)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_BATCH_REQUESTS,
        Constants.METRIC_BATCH_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received Batch");
    List<WriteOperation> writes = new ArrayList<WriteOperation>(batch.size());
    for (TWriteOperation tWriteOp : batch) {
      WriteOperation writeOp;
      if (tWriteOp.isSetWrite())
        writeOp = unwrap(tWriteOp.getWrite());
      else if (tWriteOp.isSetDelet())
        writeOp = unwrap(tWriteOp.getDelet());
      else if (tWriteOp.isSetIncrement())
        writeOp = unwrap(tWriteOp.getIncrement());
      else if (tWriteOp.isSetCompareAndSwap())
        writeOp = unwrap(tWriteOp.getCompareAndSwap());
      else if (tWriteOp.isSetQueueEnqueue())
        writeOp = new QueueEnqueue(tWriteOp.getQueueEnqueue().getQueueName(),
            tWriteOp.getQueueEnqueue().getValue());
      else if (tWriteOp.isSetQueueAck())
        writeOp = unwrap(tWriteOp.getQueueAck());
      else {
        Log.error("Internal Error: Unkown TWriteOperation "
            + tWriteOp.toString() + " in batch. Skipping.");
        continue;
      }
      if (Log.isDebugEnabled())
        Log.debug("Operation in batch: " + writeOp);
      writes.add(writeOp);
    }

    try {
      this.opex.execute(writes);
      if (Log.isDebugEnabled()) Log.debug("Batch successful.");
      helper.success();

    } catch (OperationException e) {
      Log.debug("Batch failed: " + e.getMessage());
      helper.failure();
      throw wrap(e);
    }
  }

  // read operations. they may return null from the executor.
  // Because Thrift methods cannot return null, we must wrap their
  // results into a structure

  @Override
  public TOptionalBinary readKey(TReadKey tReadKey)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READKEY_REQUESTS,
        Constants.METRIC_READKEY_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TReadKey: " + tReadKey);

    try {
      ReadKey readKey = unwrap(tReadKey);
      OperationResult<byte[]> result = this.opex.execute(readKey);
      TOptionalBinary tResult = wrapBinary(result);
      if (Log.isDebugEnabled()) Log.debug("ReadKey successful.");
      helper.success();
      return tResult;

    } catch (OperationException e) {
      Log.debug("ReadKey failed: " + e.getMessage());
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public TOptionalBinaryMap read(TRead tRead)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READ_REQUESTS,
        Constants.METRIC_READ_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TRead: " + tRead);

    try {
      Read read = unwrap(tRead);
      OperationResult<Map<byte[], byte[]>> result = this.opex.execute(read);
      TOptionalBinaryMap tResult = wrapMap(result);
      if (Log.isDebugEnabled()) Log.debug("Read successful." );
      helper.success();
      return tResult;

    } catch (OperationException e) {
      Log.debug("Read failed: " + e.getMessage());
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public TOptionalBinaryList readAllKeys(TReadAllKeys tReadAllKeys)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READALLKEYS_REQUESTS,
        Constants.METRIC_READALLKEYS_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TReadAllKeys: " + tReadAllKeys);

    try {
      ReadAllKeys readAllKeys = unwrap(tReadAllKeys);
      OperationResult<List<byte[]>> result = this.opex.execute(readAllKeys);
      TOptionalBinaryList tResult = wrapList(result);
      if (Log.isDebugEnabled()) Log.debug("ReadAllKeys successful.");
      helper.success();
      return tResult;

    } catch (OperationException e) {
      Log.debug("ReadAllKeys failed: " + e.getMessage());
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public TOptionalBinaryMap
  readColumnRange(TReadColumnRange tReadColumnRange)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READCOLUMNRANGE_REQUESTS,
        Constants.METRIC_READCOLUMNRANGE_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TReadColumnRange: " + tReadColumnRange);

    try {
      ReadColumnRange readColumnRange = unwrap(tReadColumnRange);
      OperationResult<Map<byte[], byte[]>> result =
          this.opex.execute(readColumnRange);
      TOptionalBinaryMap tResult = wrapMap(result);
      if (Log.isDebugEnabled()) Log.debug("ReadColumnRange successful.");
      helper.success();
      return tResult;

    } catch (OperationException e) {
      Log.debug("ReadColumnRange failed: " + e.getMessage());
      helper.failure();
      throw wrap(e);
    }
  }

  // dequeue always return a structure, which does not need extra wrapping

  @Override
  public TDequeueResult dequeue(TQueueDequeue tQueueDequeue)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_DEQUEUE_REQUESTS,
        Constants.METRIC_DEQUEUE_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TQueueDequeue" + tQueueDequeue.toString());

    try {
      QueueDequeue queueDequeue = unwrap(tQueueDequeue);
      DequeueResult result = this.opex.execute(queueDequeue);
      if (Log.isDebugEnabled()) Log.debug("Dequeue successful.");
      TDequeueResult tResult = wrap(result);
      helper.success();
      return tResult;

    } catch (OperationException e) {
      Log.debug("Dequeue failed: " + e.getMessage());
      helper.failure();
      throw wrap(e);
    }
  }

  // getGroupId always returns a long and cannot be null

  @Override
  public long getGroupId(TGetGroupId tGetGroupId)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_GETGROUPID_REQUESTS,
        Constants.METRIC_GETGROUPID_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TGetGroupID: " + tGetGroupId);

    try {
      QueueAdmin.GetGroupID getGroupID = unwrap(tGetGroupId);
      long groupId = this.opex.execute(getGroupID);
      if (Log.isDebugEnabled()) Log.debug("GetGroupID successful: " + groupId);
      helper.success();
      return groupId;

    } catch (OperationException e) {
      Log.debug("GetGroupID failed: " + e.getMessage());
      helper.failure();
      throw wrap(e);
    }
  }

  // getQueueMeta can return null, if the queue does not exist

  @Override
  public TQueueMeta getQueueMeta(TGetQueueMeta tGetQueueMeta)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_GETQUEUEMETA_REQUESTS,
        Constants.METRIC_GETQUEUEMETA_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TGetQueueMeta: " + tGetQueueMeta);

    try {
      QueueAdmin.GetQueueMeta getQueueMeta = unwrap(tGetQueueMeta);
      OperationResult<QueueAdmin.QueueMeta> queueMeta =
          this.opex.execute(getQueueMeta);
      if (Log.isDebugEnabled()) Log.debug("GetQueueMeta successful: " +
          (queueMeta.isEmpty() ? "<empty>" : queueMeta.getValue()));
      TQueueMeta tQueueMeta =  wrap(queueMeta);
      helper.success();
      return tQueueMeta;

    } catch (OperationException e) {
      Log.debug("GetQueueMeta failed: " + e.getMessage());
      helper.failure();
      throw wrap(e);
    }
  }

  // clearFabric is safe as it returns nothing

  @Override
  public void clearFabric(TClearFabric tClearFabric)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_CLEARFABRIC_REQUESTS,
        Constants.METRIC_CLEARFABRIC_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TClearFabric: " + tClearFabric);

    try {
      ClearFabric clearFabric = unwrap(tClearFabric);
      this.opex.execute(clearFabric);
      if (Log.isDebugEnabled()) Log.debug("Clear successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("Clear failed: " + e.getMessage());
      throw wrap(e);
    }
  }
}
