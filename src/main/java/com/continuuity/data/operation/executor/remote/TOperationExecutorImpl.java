package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.*;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.common.metrics.MetricType;
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
  public void write(TOperationContext tcontext,
                    TWrite tWrite)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_WRITE_REQUESTS,
        Constants.METRIC_WRITE_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TWrite: " + tWrite);

    try {
      OperationContext context = unwrap(tcontext);
      Write write = unwrap(tWrite);
      this.opex.execute(context, write);
      if (Log.isDebugEnabled()) Log.debug("Write successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("Write failed: " + e.getMessage());
      throw wrap(e);
    }

  }

  @Override
  public void delet(TOperationContext tcontext,
                    TDelete tDelete) throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_DELETE_REQUESTS,
        Constants.METRIC_DELETE_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TDelete: " + tDelete);

    try {
      OperationContext context = unwrap(tcontext);
      Delete delete = unwrap(tDelete);
      this.opex.execute(context, delete);
      if (Log.isDebugEnabled()) Log.debug("Delete successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("Delete failed: " + e.getMessage());
      throw wrap(e);
    }
  }

  @Override
  public void increment(TOperationContext tcontext,
                        TIncrement tIncrement)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_INCREMENT_REQUESTS,
        Constants.METRIC_INCREMENT_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TIncrement: " + tIncrement);

    try {
      OperationContext context = unwrap(tcontext);
      Increment increment = unwrap(tIncrement);
      this.opex.execute(context, increment);
      if (Log.isDebugEnabled()) Log.debug("Increment successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("Increment failed: " + e.getMessage());
      throw wrap(e);
    }
  }

  @Override
  public void compareAndSwap(TOperationContext tcontext,
                             TCompareAndSwap tCompareAndSwap)
      throws TException, TOperationException  {

    MetricsHelper helper = newHelper(
        Constants.METRIC_COMPAREANDSWAP_REQUESTS,
        Constants.METRIC_COMPAREANDSWAP_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TCompareAndSwap: " + tCompareAndSwap);

    try {
      OperationContext context = unwrap(tcontext);
      CompareAndSwap compareAndSwap = unwrap(tCompareAndSwap);
      this.opex.execute(context, compareAndSwap);
      if (Log.isDebugEnabled()) Log.debug("CompareAndSwap successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("CompareAndSwap failed: " + e.getMessage());
      throw wrap(e);
    }
  }

  @Override
  public void queueEnqueue(TOperationContext tcontext,
                           TQueueEnqueue tQueueEnqueue)
      throws TException, TOperationException  {

    MetricsHelper helper = newHelper(
        Constants.METRIC_ENQUEUE_REQUESTS,
        Constants.METRIC_ENQUEUE_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TQueueEnqueue: " + tQueueEnqueue);

    try {
      OperationContext context = unwrap(tcontext);
      QueueEnqueue queueEnqueue = unwrap(tQueueEnqueue);
      this.opex.execute(context, queueEnqueue);
      if (Log.isDebugEnabled()) Log.debug("EnqueuePayload successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("EnqueuePayload failed: " + e.getMessage());
      throw wrap(e);
    }
  }

  @Override
  public void queueAck(TOperationContext tcontext,
                       TQueueAck tQueueAck)
      throws TException, TOperationException  {

    MetricsHelper helper = newHelper(
        Constants.METRIC_ACK_REQUESTS,
        Constants.METRIC_ACK_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TQueueAck: " + tQueueAck);

    try {
      OperationContext context = unwrap(tcontext);
      QueueAck queueAck = unwrap(tQueueAck);
      this.opex.execute(context, queueAck);
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
  public void batch(TOperationContext tcontext,
                    List<TWriteOperation> batch)
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
      OperationContext context = unwrap(tcontext);
      this.opex.execute(context, writes);
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
  public TOptionalBinary readKey(TOperationContext tcontext,
                                 TReadKey tReadKey)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READKEY_REQUESTS,
        Constants.METRIC_READKEY_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TReadKey: " + tReadKey);

    try {
      OperationContext context = unwrap(tcontext);
      ReadKey readKey = unwrap(tReadKey);
      OperationResult<byte[]> result = this.opex.execute(context, readKey);
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
  public TOptionalBinaryMap read(TOperationContext tcontext,
                                 TRead tRead)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READ_REQUESTS,
        Constants.METRIC_READ_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TRead: " + tRead);

    try {
      OperationContext context = unwrap(tcontext);
      Read read = unwrap(tRead);
      OperationResult<Map<byte[], byte[]>> result =
          this.opex.execute(context, read);
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
  public TOptionalBinaryList readAllKeys(TOperationContext tcontext,
                                         TReadAllKeys tReadAllKeys)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READALLKEYS_REQUESTS,
        Constants.METRIC_READALLKEYS_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TReadAllKeys: " + tReadAllKeys);

    try {
      OperationContext context = unwrap(tcontext);
      ReadAllKeys readAllKeys = unwrap(tReadAllKeys);
      OperationResult<List<byte[]>> result =
          this.opex.execute(context, readAllKeys);
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
  readColumnRange(TOperationContext tcontext,
                  TReadColumnRange tReadColumnRange)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READCOLUMNRANGE_REQUESTS,
        Constants.METRIC_READCOLUMNRANGE_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TReadColumnRange: " + tReadColumnRange);

    try {
      OperationContext context = unwrap(tcontext);
      ReadColumnRange readColumnRange = unwrap(tReadColumnRange);
      OperationResult<Map<byte[], byte[]>> result =
          this.opex.execute(context, readColumnRange);
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
  public TDequeueResult dequeue(TOperationContext tcontext,
                                TQueueDequeue tQueueDequeue)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_DEQUEUE_REQUESTS,
        Constants.METRIC_DEQUEUE_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TQueueDequeue" + tQueueDequeue.toString());

    try {
      OperationContext context = unwrap(tcontext);
      QueueDequeue queueDequeue = unwrap(tQueueDequeue);
      DequeueResult result = this.opex.execute(context, queueDequeue);
      if (Log.isDebugEnabled()) Log.debug("DequeuePayload successful.");
      TDequeueResult tResult = wrap(result);
      helper.success();
      return tResult;

    } catch (OperationException e) {
      Log.debug("DequeuePayload failed: " + e.getMessage());
      helper.failure();
      throw wrap(e);
    }
  }

  // getGroupId always returns a long and cannot be null

  @Override
  public long getGroupId(TOperationContext tcontext,
                         TGetGroupId tGetGroupId)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_GETGROUPID_REQUESTS,
        Constants.METRIC_GETGROUPID_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TGetGroupID: " + tGetGroupId);

    try {
      OperationContext context = unwrap(tcontext);
      QueueAdmin.GetGroupID getGroupID = unwrap(tGetGroupId);
      long groupId = this.opex.execute(context, getGroupID);
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
  public TQueueMeta getQueueMeta(TOperationContext tcontext,
                                 TGetQueueMeta tGetQueueMeta)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_GETQUEUEMETA_REQUESTS,
        Constants.METRIC_GETQUEUEMETA_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TGetQueueMeta: " + tGetQueueMeta);

    try {
      OperationContext context = unwrap(tcontext);
      QueueAdmin.GetQueueMeta getQueueMeta = unwrap(tGetQueueMeta);
      OperationResult<QueueAdmin.QueueMeta> queueMeta =
          this.opex.execute(context, getQueueMeta);
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
  public void clearFabric(TOperationContext tcontext,
                          TClearFabric tClearFabric)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_CLEARFABRIC_REQUESTS,
        Constants.METRIC_CLEARFABRIC_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received TClearFabric: " + tClearFabric);

    try {
      OperationContext context = unwrap(tcontext);
      ClearFabric clearFabric = unwrap(tClearFabric);
      this.opex.execute(context, clearFabric);
      if (Log.isDebugEnabled()) Log.debug("Clear successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.debug("Clear failed: " + e.getMessage());
      throw wrap(e);
    }
  }
}
