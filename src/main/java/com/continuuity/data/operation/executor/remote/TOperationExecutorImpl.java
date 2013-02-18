package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.TClearFabric;
import com.continuuity.data.operation.executor.remote.stubs.TDequeueResult;
import com.continuuity.data.operation.executor.remote.stubs.TGetGroupId;
import com.continuuity.data.operation.executor.remote.stubs.TGetQueueInfo;
import com.continuuity.data.operation.executor.remote.stubs.TOpenTable;
import com.continuuity.data.operation.executor.remote.stubs.TOperationContext;
import com.continuuity.data.operation.executor.remote.stubs.TOperationException;
import com.continuuity.data.operation.executor.remote.stubs.TOperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.TOptionalBinaryList;
import com.continuuity.data.operation.executor.remote.stubs.TOptionalBinaryMap;
import com.continuuity.data.operation.executor.remote.stubs.TQueueDequeue;
import com.continuuity.data.operation.executor.remote.stubs.TQueueInfo;
import com.continuuity.data.operation.executor.remote.stubs.TRead;
import com.continuuity.data.operation.executor.remote.stubs.TReadAllKeys;
import com.continuuity.data.operation.executor.remote.stubs.TReadColumnRange;
import com.continuuity.data.operation.executor.remote.stubs.TWriteOperation;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAdmin;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.continuuity.common.metrics.MetricsHelper.Status.NoData;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;
import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

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
  MetricsHelper newHelper(String method) {
    return new MetricsHelper(
        this.getClass(), this.metrics, "opex.service", method);
  }
  MetricsHelper newHelper(String method, byte[] scope) {
    MetricsHelper helper = newHelper(method);
    setScope(helper, scope);
    return helper;
  }
  MetricsHelper newHelper(String method, String scope) {
    MetricsHelper helper = newHelper(method);
    setScope(helper, scope);
    return helper;
  }
  void setScope(MetricsHelper helper, byte[] scope) {
    if (scope != null) helper.setScope(scope);
  }
  void setScope(MetricsHelper helper, String scope) {
    if (scope != null) helper.setScope(scope);
  }

  /** constructor requires the operation executor */
  public TOperationExecutorImpl(OperationExecutor opex) {
    this.opex = opex;
  }

  // batch write, return a structure and never null, and is thus safe

  @Override
  public void batch(TOperationContext tcontext,
                    List<TWriteOperation> batch)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper("batch");

    if (Log.isTraceEnabled())
      Log.trace("Received Batch");
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
      if (Log.isTraceEnabled())
        Log.trace("Operation in batch: " + writeOp);
      writes.add(writeOp);
    }

    try {
      OperationContext context = unwrap(tcontext);
      this.opex.commit(context, writes);
      if (Log.isTraceEnabled()) Log.trace("Batch successful.");
      helper.success();

    } catch (OperationException e) {
      Log.warn("Batch failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  // read operations. they may return null from the executor.
  // Because Thrift methods cannot return null, we must wrap their
  // results into a structure

  @Override
  public TOptionalBinaryMap read(TOperationContext tcontext,
                                 TRead tRead)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper("read", tRead.getTable());

    if (Log.isTraceEnabled())
      Log.trace("Received TRead: " + tRead);

    try {
      OperationContext context = unwrap(tcontext);
      Read read = unwrap(tRead);
      OperationResult<Map<byte[], byte[]>> result =
          this.opex.execute(context, read);
      TOptionalBinaryMap tResult = wrapMap(result);
      if (Log.isTraceEnabled()) Log.trace("Read successful.");

      helper.finish(result.isEmpty() ? NoData : Success);
      return tResult;

    } catch (OperationException e) {
      Log.warn("Read failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public TOptionalBinaryList readAllKeys(TOperationContext tcontext,
                                         TReadAllKeys tReadAllKeys)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper("listkeys", tReadAllKeys.getTable());

    if (Log.isTraceEnabled())
      Log.trace("Received TReadAllKeys: " + tReadAllKeys);

    try {
      OperationContext context = unwrap(tcontext);
      ReadAllKeys readAllKeys = unwrap(tReadAllKeys);
      OperationResult<List<byte[]>> result =
          this.opex.execute(context, readAllKeys);
      TOptionalBinaryList tResult = wrapList(result);
      if (Log.isTraceEnabled()) Log.trace("ReadAllKeys successful.");

      helper.finish(result.isEmpty() ? NoData : Success);
      return tResult;

    } catch (OperationException e) {
      Log.warn("ReadAllKeys failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public TOptionalBinaryMap
  readColumnRange(TOperationContext tcontext,
                  TReadColumnRange tReadColumnRange)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper("range", tReadColumnRange.getTable());

    if (Log.isTraceEnabled())
      Log.trace("Received TReadColumnRange: " + tReadColumnRange);

    try {
      OperationContext context = unwrap(tcontext);
      ReadColumnRange readColumnRange = unwrap(tReadColumnRange);
      OperationResult<Map<byte[], byte[]>> result =
          this.opex.execute(context, readColumnRange);
      TOptionalBinaryMap tResult = wrapMap(result);
      if (Log.isTraceEnabled()) Log.trace("ReadColumnRange successful.");

      helper.finish(result.isEmpty() ? NoData : Success);
      return tResult;

    } catch (OperationException e) {
      Log.warn("ReadColumnRange failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  // dequeue always return a structure, which does not need extra wrapping

  @Override
  public TDequeueResult dequeue(TOperationContext tcontext,
                                TQueueDequeue tQueueDequeue)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper("dequeue",tQueueDequeue.getQueueName());

    if (Log.isTraceEnabled())
      Log.trace("Received TQueueDequeue" + tQueueDequeue.toString());

    try {
      OperationContext context = unwrap(tcontext);
      QueueDequeue queueDequeue = unwrap(tQueueDequeue);
      DequeueResult result = this.opex.execute(context, queueDequeue);
      if (Log.isTraceEnabled()) {
        Log.trace("DequeuePayload successful with status {}",
                  result.getStatus().name());
      }
      TDequeueResult tResult = wrap(result);

      helper.finish(result.isEmpty() ? NoData : Success);
      return tResult;

    } catch (OperationException e) {
      Log.warn("DequeuePayload failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  // getGroupId always returns a long and cannot be null

  @Override
  public long getGroupId(TOperationContext tcontext,
                         TGetGroupId tGetGroupId)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper("getid", tGetGroupId.getQueueName());

    if (Log.isTraceEnabled())
      Log.trace("Received TGetGroupID: " + tGetGroupId);

    try {
      OperationContext context = unwrap(tcontext);
      QueueAdmin.GetGroupID getGroupID = unwrap(tGetGroupId);
      long groupId = this.opex.execute(context, getGroupID);
      if (Log.isTraceEnabled()) Log.trace("GetGroupID successful: " + groupId);
      helper.success();
      return groupId;

    } catch (OperationException e) {
      Log.warn("GetGroupID failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  // getQueueInfo can return null, if the queue does not exist

  @Override
  public TQueueInfo getQueueInfo(TOperationContext tcontext,
                                 TGetQueueInfo tGetQueueInfo)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper("info", tGetQueueInfo.getQueueName());

    if (Log.isTraceEnabled())
      Log.trace("Received TGetQueueInfo: " + tGetQueueInfo);

    try {
      OperationContext context = unwrap(tcontext);
      QueueAdmin.GetQueueInfo getQueueInfo = unwrap(tGetQueueInfo);
      OperationResult<QueueInfo> queueInfo =
          this.opex.execute(context, getQueueInfo);
      if (Log.isTraceEnabled()) Log.trace("GetQueueInfo successful: " +
          (queueInfo.isEmpty() ? "<empty>" : queueInfo.getValue()));
      TQueueInfo tQueueInfo =  wrap(queueInfo);

      helper.finish(queueInfo.isEmpty() ? NoData : Success);
      return tQueueInfo;

    } catch (OperationException e) {
      Log.warn("GetQueueInfo failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  // clearFabric is safe as it returns nothing

  @Override
  public void clearFabric(TOperationContext tcontext,
                          TClearFabric tClearFabric)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper("clear");

    if (Log.isTraceEnabled())
      Log.trace("Received TClearFabric: " + tClearFabric);

    try {
      OperationContext context = unwrap(tcontext);
      ClearFabric clearFabric = unwrap(tClearFabric);
      this.opex.execute(context, clearFabric);
      if (Log.isTraceEnabled()) Log.trace("Clear successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.warn("Clear failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      throw wrap(e);
    }
  }

  @Override
  public void openTable(TOperationContext tcontext,
                        TOpenTable tOpenTable)
      throws TException, TOperationException {

    MetricsHelper helper = newHelper("open", tOpenTable.getTable());

    if (Log.isTraceEnabled())
      Log.trace("Received TOpenTable: " + tOpenTable);

    try {
      OperationContext context = unwrap(tcontext);
      OpenTable openTable = unwrap(tOpenTable);
      this.opex.execute(context, openTable);
      if (Log.isTraceEnabled()) Log.trace("Open table successful.");
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.warn("Open table failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      throw wrap(e);
    }
  }

}
