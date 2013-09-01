package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.GetSplits;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.TruncateTable;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.remote.stubs.TClearFabric;
import com.continuuity.data.operation.executor.remote.stubs.TDequeueResult;
import com.continuuity.data.operation.executor.remote.stubs.TGetGroupId;
import com.continuuity.data.operation.executor.remote.stubs.TGetQueueInfo;
import com.continuuity.data.operation.executor.remote.stubs.TGetSplits;
import com.continuuity.data.operation.executor.remote.stubs.TIncrement;
import com.continuuity.data.operation.executor.remote.stubs.TKeyRange;
import com.continuuity.data.operation.executor.remote.stubs.TOpenTable;
import com.continuuity.data.operation.executor.remote.stubs.TOperationContext;
import com.continuuity.data.operation.executor.remote.stubs.TOperationException;
import com.continuuity.data.operation.executor.remote.stubs.TOperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.TOptionalBinaryList;
import com.continuuity.data.operation.executor.remote.stubs.TOptionalBinaryMap;
import com.continuuity.data.operation.executor.remote.stubs.TQueueConfigure;
import com.continuuity.data.operation.executor.remote.stubs.TQueueConfigureGroups;
import com.continuuity.data.operation.executor.remote.stubs.TQueueDequeue;
import com.continuuity.data.operation.executor.remote.stubs.TQueueDropInflight;
import com.continuuity.data.operation.executor.remote.stubs.TQueueInfo;
import com.continuuity.data.operation.executor.remote.stubs.TRead;
import com.continuuity.data.operation.executor.remote.stubs.TReadAllKeys;
import com.continuuity.data.operation.executor.remote.stubs.TReadColumnRange;
import com.continuuity.data.operation.executor.remote.stubs.TTransaction;
import com.continuuity.data.operation.executor.remote.stubs.TTransaction2;
import com.continuuity.data.operation.executor.remote.stubs.TTruncateTable;
import com.continuuity.data.operation.executor.remote.stubs.TWriteOperation;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.GetQueueInfo;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigureGroups;
import com.continuuity.data.operation.ttqueue.admin.QueueDropInflight;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.google.common.collect.Sets;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.continuuity.common.metrics.MetricsHelper.Status.NoData;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * The implementation of a thrift service for operation execution.
 * All operations arrive over the wire as Thrift objects. We must
 * convert ("unwrap") them into data fabric operations, execute using
 * the actual operations executor, and send the results back as
 * ("wrapped") Thrift objects.
 * <p/>
 * Why all this conversion (wrap/unwrap), and not define all Operations
 * themselves as Thrift objects?
 * <ul><li>
 * All the non-thrift executors would have to use the Thrift objects
 * </li><li>
 * Thrift's object model is too restrictive: it has only limited inheritance
 * and no overloading
 * </li><li>
 * Thrift objects are bare-bone, all they have are getters, setters, and
 * basic object methods.
 * </li></ul>
 */
public class TOperationExecutorImpl extends ConverterUtils implements TOperationExecutor.Iface {

  private static final Logger Log = LoggerFactory.getLogger(TOperationExecutorImpl.class);

  /**
   * the operation executor to use for all operations.
   */
  private OperationExecutor opex;

  /**
   * metrics client.
   */
  private CMetrics metrics = new CMetrics(MetricType.System);

  /**
   * helper method to create a metrics helper.
   */
  MetricsHelper newHelper(String method) {
    return new MetricsHelper(this.getClass(), this.metrics, "opex.service", method);
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
    if (scope != null) {
      helper.setScope(scope);
    }
  }

  void setScope(MetricsHelper helper, String scope) {
    if (scope != null) {
      helper.setScope(scope);
    }
  }

  /**
   * constructor requires the operation executor.
   */
  public TOperationExecutorImpl(OperationExecutor opex) {
    this.opex = opex;
  }

  // batch write, return a structure and never null, and is thus safe

  @Override
  public TTransaction start(TOperationContext tcontext, boolean trackChanges) throws TOperationException, TException {

    MetricsHelper helper = newHelper("startTransaction");

    if (Log.isTraceEnabled()) {
      Log.trace("Received startTransaction");
    }

    try {
      OperationContext context = unwrap(tcontext);
      Transaction result = this.opex.startTransaction(context, trackChanges);
      TTransaction ttx = wrap(result);
      if (Log.isTraceEnabled()) {
        Log.trace("Read successful.");
      }

      helper.finish(Success);
      return ttx;

    } catch (OperationException e) {
      Log.warn("startTransaction failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public void batch(TOperationContext tcontext, List<TWriteOperation> batch) throws TException, TOperationException {

    MetricsHelper helper = newHelper("batch");

    if (Log.isTraceEnabled()) {
      Log.trace("Received Batch");
    }

    try {
      OperationContext context = unwrap(tcontext);
      this.opex.commit(context, unwrapBatch(batch));
      if (Log.isTraceEnabled()) {
        Log.trace("Batch successful.");
      }
      helper.success();

    } catch (OperationException e) {
      Log.warn("Batch failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public TTransaction execute(TOperationContext tcontext, TTransaction ttx, List<TWriteOperation> batch)
    throws TOperationException, TException {

    MetricsHelper helper = newHelper("execute");

    if (Log.isTraceEnabled()) {
      Log.trace("Received Execute batch");
    }

    try {
      OperationContext context = unwrap(tcontext);
      Transaction tx = unwrap(ttx);
      tx = this.opex.execute(context, tx, unwrapBatch(batch));
      if (Log.isTraceEnabled()) {
        Log.trace("Execute batch successful.");
      }
      helper.success();

      return wrap(tx);

    } catch (OperationException e) {
      Log.warn("Execute batch failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public void finish(TOperationContext tcontext, TTransaction ttx, List<TWriteOperation> batch)
    throws TOperationException, TException {

    MetricsHelper helper = newHelper("finish");

    if (Log.isTraceEnabled()) {
      Log.trace("Received Commit with batch");
    }

    try {
      OperationContext context = unwrap(tcontext);
      Transaction tx = unwrap(ttx);
      this.opex.commit(context, tx, unwrapBatch(batch));
      if (Log.isTraceEnabled()) {
        Log.trace("Commit batch successful.");
      }
      helper.success();

    } catch (OperationException e) {
      Log.warn("Commit batch failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public void commit(TOperationContext tcontext, TTransaction ttx) throws TOperationException, TException {

    MetricsHelper helper = newHelper("commit");

    if (Log.isTraceEnabled()) {
      Log.trace("Received Commit");
    }

    try {
      OperationContext context = unwrap(tcontext);
      Transaction tx = unwrap(ttx);
      this.opex.commit(context, tx);
      if (Log.isTraceEnabled()) {
        Log.trace("Commit successful.");
      }
      helper.success();

    } catch (OperationException e) {
      Log.warn("Commit failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public void abort(TOperationContext tcontext, TTransaction ttx) throws TOperationException, TException {

    MetricsHelper helper = newHelper("abort");

    if (Log.isTraceEnabled()) {
      Log.trace("Received Abort");
    }

    try {
      OperationContext context = unwrap(tcontext);
      Transaction tx = unwrap(ttx);
      this.opex.abort(context, tx);
      if (Log.isTraceEnabled()) {
        Log.trace("Abort successful.");
      }
      helper.success();

    } catch (OperationException e) {
      Log.warn("Abort failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  // read operations. they may return null from the executor.
  // Because Thrift methods cannot return null, we must wrap their
  // results into a structure

  @Override
  public TOptionalBinaryMap read(TOperationContext tcontext, TRead tRead) throws TException, TOperationException {

    MetricsHelper helper = newHelper("read", tRead.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TRead: " + tRead);
    }

    try {
      OperationContext context = unwrap(tcontext);
      Read read = unwrap(tRead);
      OperationResult<Map<byte[], byte[]>> result = this.opex.execute(context, read);
      TOptionalBinaryMap tResult = wrapMap(result);
      if (Log.isTraceEnabled()) {
        Log.trace("Read successful.");
      }

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
  public TOptionalBinaryMap readTx(TOperationContext tcontext, TTransaction ttx, TRead tRead)
    throws TOperationException, TException {

    MetricsHelper helper = newHelper("readTx", tRead.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TRead: " + tRead);
    }

    try {
      OperationContext context = unwrap(tcontext);
      Transaction tx = unwrap(ttx);
      Read read = unwrap(tRead);
      OperationResult<Map<byte[], byte[]>> result = this.opex.execute(context, tx, read);
      TOptionalBinaryMap tResult = wrapMap(result);
      if (Log.isTraceEnabled()) {
        Log.trace("Read successful.");
      }

      helper.finish(result.isEmpty() ? NoData : Success);
      return tResult;

    } catch (OperationException e) {
      Log.warn("readTx failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public TOptionalBinaryList readAllKeys(TOperationContext tcontext, TReadAllKeys tReadAllKeys)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("listkeys", tReadAllKeys.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TReadAllKeys: " + tReadAllKeys);
    }

    try {
      OperationContext context = unwrap(tcontext);
      ReadAllKeys readAllKeys = unwrap(tReadAllKeys);
      OperationResult<List<byte[]>> result = this.opex.execute(context, readAllKeys);
      TOptionalBinaryList tResult = wrapList(result);
      if (Log.isTraceEnabled()) {
        Log.trace("ReadAllKeys successful.");
      }

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
  public TOptionalBinaryList readAllKeysTx(TOperationContext tcontext, TTransaction ttx, TReadAllKeys tReadAllKeys)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("listkeysTx", tReadAllKeys.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TReadAllKeys: " + tReadAllKeys);
    }

    try {
      OperationContext context = unwrap(tcontext);
      Transaction tx = unwrap(ttx);
      ReadAllKeys readAllKeys = unwrap(tReadAllKeys);
      OperationResult<List<byte[]>> result = this.opex.execute(context, tx, readAllKeys);
      TOptionalBinaryList tResult = wrapList(result);
      if (Log.isTraceEnabled()) {
        Log.trace("ReadAllKeys successful.");
      }

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
  public TOptionalBinaryMap readColumnRange(TOperationContext tcontext, TReadColumnRange tReadColumnRange)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("range", tReadColumnRange.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TReadColumnRange: " + tReadColumnRange);
    }

    try {
      OperationContext context = unwrap(tcontext);
      ReadColumnRange readColumnRange = unwrap(tReadColumnRange);
      OperationResult<Map<byte[], byte[]>> result = this.opex.execute(context, readColumnRange);
      TOptionalBinaryMap tResult = wrapMap(result);
      if (Log.isTraceEnabled()) {
        Log.trace("ReadColumnRange successful.");
      }

      helper.finish(result.isEmpty() ? NoData : Success);
      return tResult;

    } catch (OperationException e) {
      Log.warn("ReadColumnRange failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public TOptionalBinaryMap readColumnRangeTx(TOperationContext tcontext, TTransaction ttx,
                                              TReadColumnRange tReadColumnRange)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("rangeTx", tReadColumnRange.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TReadColumnRange: " + tReadColumnRange);
    }

    try {
      OperationContext context = unwrap(tcontext);
      Transaction tx = unwrap(ttx);
      ReadColumnRange readColumnRange = unwrap(tReadColumnRange);
      OperationResult<Map<byte[], byte[]>> result = this.opex.execute(context, tx, readColumnRange);
      TOptionalBinaryMap tResult = wrapMap(result);
      if (Log.isTraceEnabled()) {
        Log.trace("ReadColumnRange successful.");
      }

      helper.finish(result.isEmpty() ? NoData : Success);
      return tResult;

    } catch (OperationException e) {
      Log.warn("ReadColumnRange failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public Map<ByteBuffer, Long> increment(TOperationContext tcontext, TIncrement tIncrement)
    throws TOperationException, TException {

    MetricsHelper helper = newHelper("increment", tIncrement.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TIncrement: " + tIncrement);
    }

    try {
      OperationContext context = unwrap(tcontext);
      Increment increment = unwrap(tIncrement);
      Map<byte[], Long> result = this.opex.increment(context, increment);
      if (Log.isTraceEnabled()) {
        Log.trace("Increment successful.");
      }

      helper.finish(result.isEmpty() ? NoData : Success);
      return wrapLongMap(result);

    } catch (OperationException e) {
      Log.warn("Increment failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public Map<ByteBuffer, Long> incrementTx(TOperationContext tcontext, TTransaction ttx, TIncrement tIncrement)
    throws TOperationException, TException {

    MetricsHelper helper = newHelper("incrementTx", tIncrement.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TIncrement: " + tIncrement);
    }

    try {
      OperationContext context = unwrap(tcontext);
      Transaction tx = unwrap(ttx);
      Increment increment = unwrap(tIncrement);
      Map<byte[], Long> result = this.opex.increment(context, tx, increment);
      if (Log.isTraceEnabled()) {
        Log.trace("Increment successful.");
      }

      helper.finish(result.isEmpty() ? NoData : Success);
      return wrapLongMap(result);

    } catch (OperationException e) {
      Log.warn("Increment failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public List<TKeyRange> getSplits(TOperationContext tContext, TGetSplits tGetSplits)
    throws TOperationException, TException {
    MetricsHelper helper = newHelper("splits", tGetSplits.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TGetSplits: " + tGetSplits);
    }

    try {
      OperationContext context = unwrap(tContext);
      GetSplits getSplits = unwrap(tGetSplits);
      OperationResult<List<KeyRange>> result = this.opex.execute(context, getSplits);
      List<TKeyRange> tResult = wrap(result);
      if (Log.isTraceEnabled()) {
        Log.trace("GetSplits successful.");
      }

      helper.finish(result.isEmpty() ? NoData : Success);
      return tResult;

    } catch (OperationException e) {
      Log.warn("GetSplits failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  @Override
  public List<TKeyRange> getSplitsTx(TOperationContext tContext, TTransaction ttx, TGetSplits tGetSplits)
    throws TOperationException, TException {
    MetricsHelper helper = newHelper("splits", tGetSplits.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TGetSplits: " + tGetSplits);
    }

    try {
      OperationContext context = unwrap(tContext);
      Transaction tx = unwrap(ttx);
      GetSplits getSplits = unwrap(tGetSplits);
      OperationResult<List<KeyRange>> result = this.opex.execute(context, tx, getSplits);
      List<TKeyRange> tResult = wrap(result);
      if (Log.isTraceEnabled()) {
        Log.trace("GetSplits successful.");
      }

      helper.finish(result.isEmpty() ? NoData : Success);
      return tResult;

    } catch (OperationException e) {
      Log.warn("GetSplits failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      helper.failure();
      throw wrap(e);
    }
  }

  // dequeue always return a structure, which does not need extra wrapping

  @Override
  public TDequeueResult dequeue(TOperationContext tcontext, TQueueDequeue tQueueDequeue)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("dequeue", tQueueDequeue.getQueueName());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TQueueDequeue" + tQueueDequeue.toString());
    }

    try {
      OperationContext context = unwrap(tcontext);
      QueueDequeue queueDequeue = unwrap(tQueueDequeue);
      DequeueResult result = this.opex.execute(context, queueDequeue);
      if (Log.isTraceEnabled()) {
        Log.trace("DequeuePayload successful with status {}", result.getStatus().name());
      }
      TDequeueResult tResult = wrap(result, queueDequeue.getConsumer());

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
  public long getGroupId(TOperationContext tcontext, TGetGroupId tGetGroupId) throws TException, TOperationException {

    MetricsHelper helper = newHelper("getid", tGetGroupId.getQueueName());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TGetGroupID: " + tGetGroupId);
    }

    try {
      OperationContext context = unwrap(tcontext);
      GetGroupID getGroupID = unwrap(tGetGroupId);
      long groupId = this.opex.execute(context, getGroupID);
      if (Log.isTraceEnabled()) {
        Log.trace("GetGroupID successful: " + groupId);
      }
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
  public TQueueInfo getQueueInfo(TOperationContext tcontext, TGetQueueInfo tGetQueueInfo)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("info", tGetQueueInfo.getQueueName());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TGetQueueInfo: " + tGetQueueInfo);
    }

    try {
      OperationContext context = unwrap(tcontext);
      GetQueueInfo getQueueInfo = unwrap(tGetQueueInfo);
      OperationResult<QueueInfo> queueInfo = this.opex.execute(context, getQueueInfo);
      if (Log.isTraceEnabled()) {
        Log.trace("GetQueueInfo successful: " + (queueInfo.isEmpty() ? "<empty>" : queueInfo.getValue()));
      }
      TQueueInfo tQueueInfo = wrap(queueInfo);

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
  public void clearFabric(TOperationContext tcontext, TClearFabric tClearFabric)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("clear");

    if (Log.isTraceEnabled()) {
      Log.trace("Received TClearFabric: " + tClearFabric);
    }

    try {
      OperationContext context = unwrap(tcontext);
      ClearFabric clearFabric = unwrap(tClearFabric);
      this.opex.execute(context, clearFabric);
      if (Log.isTraceEnabled()) {
        Log.trace("Clear successful.");
      }
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.warn("Clear failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      throw wrap(e);
    }
  }

  // configureQueue is safe as it returns nothing

  @Override
  public void configureQueue(TOperationContext tcontext, TQueueConfigure tQueueConfigure)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("configure", tQueueConfigure.getQueueName());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TQueueConfigure: " + tQueueConfigure);
    }

    try {
      OperationContext context = unwrap(tcontext);
      QueueConfigure queueConfigure = unwrap(tQueueConfigure);
      this.opex.execute(context, queueConfigure);
      if (Log.isTraceEnabled()) {
        Log.trace("Queue configure successful.");
      }
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.warn("Queue configure failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      throw wrap(e);
    }
  }

  // configureQueueGroups is safe as it returns nothing

  @Override
  public void configureQueueGroups(TOperationContext tcontext,
                                   TQueueConfigureGroups tQueueConfigure)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("configureQueueGroups", tQueueConfigure.getQueueName());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TQueueConfigureGroups: " + tQueueConfigure);
    }

    try {
      OperationContext context = unwrap(tcontext);
      QueueConfigureGroups queueConfigure = unwrap(tQueueConfigure);
      this.opex.execute(context, queueConfigure);
      if (Log.isTraceEnabled()) {
        Log.trace("Queue configure groups successful.");
      }
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.warn("Queue configure groups failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      throw wrap(e);
    }
  }

  // queueDropInflight is safe as it returns nothing

  @Override
  public void queueDropInflight(TOperationContext tcontext,
                                TQueueDropInflight tOp)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("queueDropInflight", tOp.getQueueName());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TQueueDropInflight: " + tOp);
    }

    try {
      OperationContext context = unwrap(tcontext);
      QueueDropInflight op = unwrap(tOp);
      this.opex.execute(context, op);
      if (Log.isTraceEnabled()) {
        Log.trace("Queue drop inflight is successful.");
      }
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.warn("Queue drop inflight failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      throw wrap(e);
    }
  }

  @Override
  public void openTable(TOperationContext tcontext, TOpenTable tOpenTable) throws TException, TOperationException {

    MetricsHelper helper = newHelper("open", tOpenTable.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TOpenTable: " + tOpenTable);
    }

    try {
      OperationContext context = unwrap(tcontext);
      OpenTable openTable = unwrap(tOpenTable);
      this.opex.execute(context, openTable);
      if (Log.isTraceEnabled()) {
        Log.trace("Open table successful.");
      }
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.warn("Open table failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      throw wrap(e);
    }
  }

  @Override
  public void truncateTable(TOperationContext tcontext,
                            TTruncateTable tTruncateTable)
    throws TException, TOperationException {

    MetricsHelper helper = newHelper("truncate", tTruncateTable.getTable());

    if (Log.isTraceEnabled()) {
      Log.trace("Received TTruncateTable: " + tTruncateTable);
    }

    try {
      OperationContext context = unwrap(tcontext);
      TruncateTable truncateTable = unwrap(tTruncateTable);
      this.opex.execute(context, truncateTable);
      if (Log.isTraceEnabled()) {
        Log.trace("Truncate table successful.");
      }
      helper.success();

    } catch (OperationException e) {
      helper.failure();
      Log.warn("Truncate table failed: " + e.getMessage());
      Log.warn(StackTraceUtil.toStringStackTrace(e));
      throw wrap(e);
    }
  }

  // Temporary TxDs2 stuff

  @Override
  public TTransaction2 startTx() throws TOperationException, TException {
    try {
      return wrap(this.opex.start());
    } catch (OperationException e) {
      throw wrap(e);
    }
  }

  @Override
  public TTransaction2 startTxTimeout(int timeout) throws TOperationException, TException {
    try {
      return wrap(this.opex.start(timeout == -1 ? null : timeout));
    } catch (OperationException e) {
      throw wrap(e);
    }
  }

  @Override
  public boolean canCommitTx(TTransaction2 tx, Set<ByteBuffer> changes) throws TOperationException, TException {
    Set<byte[]> changeIds = Sets.newHashSet();
    for (ByteBuffer bb : changes) {
      changeIds.add(bb.array());
    }
    try {
      return this.opex.canCommit(unwrap(tx), changeIds);
    } catch (OperationException e) {
      throw wrap(e);
    }
  }

  @Override
  public boolean commitTx(TTransaction2 tx) throws TOperationException, TException {
    try {
      return this.opex.commit(unwrap(tx));
    } catch (OperationException e) {
      throw wrap(e);
    }
  }

  @Override
  public boolean abortTx(TTransaction2 tx) throws TOperationException, TException {
    try {
      return this.opex.abort(unwrap(tx));
    } catch (OperationException e) {
      throw wrap(e);
    }
  }
}
