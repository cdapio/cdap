package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.metrics.MetricsHelper;
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
import com.continuuity.data.operation.WriteOperation;
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
import com.continuuity.data.operation.executor.remote.stubs.TTruncateTable;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.GetQueueInfo;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigureGroups;
import com.continuuity.data.operation.ttqueue.admin.QueueDropInflight;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.continuuity.common.metrics.MetricsHelper.Status.NoData;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This class is a wrapper around the thrift opex client, it takes
 * Operations, converts them into thrift objects, calls the thrift
 * client, and converts the results back to data fabric classes.
 * This class also instruments the thrift calls with metrics.
 */
public class OperationExecutorClient extends ConverterUtils {

  private static final Logger Log = LoggerFactory.getLogger(OperationExecutorClient.class);
  private static final Function<byte[], ByteBuffer> BYTES_WRAPPER = new Function<byte[], ByteBuffer>() {
    @Override
    public ByteBuffer apply(byte[] input) {
      return ByteBuffer.wrap(input);
    }
  };

  /**
   * The thrift transport layer. We need this when we close the connection.
   */
  TTransport transport;

  /**
   * The actual thrift client.
   */
  TOperationExecutor.Client client;

  /**
   * The metrics collection client.
   */
  CMetrics metrics = new CMetrics(MetricType.System);

  /**
   * helper method to create a metrics helper.
   */
  MetricsHelper newHelper(String method) {
    return new MetricsHelper(this.getClass(), this.metrics, "opex.client", method);
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
   * Constructor from an existing, connected thrift transport.
   *
   * @param transport the thrift transport layer. It must already be comnnected
   */
  public OperationExecutorClient(TTransport transport) {
    this.transport = transport;
    // thrift protocol layer, we use binary because so does the service
    TProtocol protocol = new TBinaryProtocol(transport);
    // and create a thrift client
    this.client = new TOperationExecutor.Client(protocol);
  }

  /**
   * close this client. may be called multiple times
   */
  public void close() {
    if (this.transport.isOpen()) {
      this.transport.close();
    }
  }

  public Transaction startTransaction(OperationContext context, boolean trackChanges)
    throws OperationException, TException {

    MetricsHelper helper = newHelper("startTransaction");

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received StartTransaction");
      }
      TOperationContext tcontext = wrap(context);
      TTransaction ttx = client.start(tcontext, trackChanges);
      if (Log.isTraceEnabled()) {
        Log.trace("StartTransaction successful.");
      }
      Transaction tx = unwrap(ttx);
      helper.finish(Success);
      return tx;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(OperationContext context, List<WriteOperation> writes) throws OperationException, TException {

    MetricsHelper helper = newHelper("batch");

    if (Log.isTraceEnabled()) {
      Log.trace("Received Batch of " + writes.size() + "WriteOperations: ");
    }

    try {
      TOperationContext tcontext = wrap(context);

      if (Log.isTraceEnabled()) {
        Log.trace("Sending Batch.");
      }
      client.batch(tcontext, wrapBatch(writes));
      if (Log.isTraceEnabled()) {
        Log.trace("Batch successful.");
      }
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public Transaction execute(OperationContext context, Transaction transaction, List<WriteOperation> writes)
    throws OperationException, TException {

    MetricsHelper helper = newHelper("executeBatch");

    if (Log.isTraceEnabled()) {
      Log.trace("Received Batch of " + writes.size() + "WriteOperations: ");
    }

    try {
      TOperationContext tcontext = wrap(context);

      if (Log.isTraceEnabled()) {
        Log.trace("Sending Batch.");
      }
      TTransaction ttx = client.execute(tcontext, wrap(transaction), wrapBatch(writes));
      if (Log.isTraceEnabled()) {
        Log.trace("Batch successful.");
      }
      helper.success();
      return unwrap(ttx);

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void commit(OperationContext context, Transaction transaction, List<WriteOperation> writes)
    throws OperationException, TException {

    MetricsHelper helper = newHelper("commmitBatch");

    if (Log.isTraceEnabled()) {
      Log.trace("Received Batch of " + writes.size() + "WriteOperations: ");
    }

    try {
      TOperationContext tcontext = wrap(context);

      if (Log.isTraceEnabled()) {
        Log.trace("Committing Batch.");
      }
      client.finish(tcontext, wrap(transaction), wrapBatch(writes));
      if (Log.isTraceEnabled()) {
        Log.trace("Batch and commit successful.");
      }
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void commit(OperationContext context, Transaction transaction) throws OperationException, TException {

    MetricsHelper helper = newHelper("Commit");

    if (Log.isTraceEnabled()) {
      Log.trace("Received Commit");
    }

    try {
      TOperationContext tcontext = wrap(context);
      client.commit(tcontext, wrap(transaction));
      if (Log.isTraceEnabled()) {
        Log.trace("Commit successful.");
      }
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void abort(OperationContext context, Transaction transaction) throws OperationException, TException {

    MetricsHelper helper = newHelper("Abort");

    if (Log.isTraceEnabled()) {
      Log.trace("Received Abort");
    }

    try {
      TOperationContext tcontext = wrap(context);
      client.abort(tcontext, wrap(transaction));
      if (Log.isTraceEnabled()) {
        Log.trace("Abort successful.");
      }
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public DequeueResult execute(OperationContext context, QueueDequeue dequeue) throws TException, OperationException {

    MetricsHelper helper = newHelper("dequeue", dequeue.getKey());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + dequeue);
      }
      TOperationContext tcontext = wrap(context);
      TQueueDequeue tDequeue = wrap(dequeue);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tDequeue);
      }
      TDequeueResult tDequeueResult = client.dequeue(tcontext, tDequeue);
      if (Log.isTraceEnabled()) {
        Log.trace("TDequeue successful.");
      }
      DequeueResult dequeueResult = unwrap(tDequeueResult, dequeue.getConsumer());
      helper.finish(dequeueResult.isEmpty() ? NoData : Success);
      return dequeueResult;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public long execute(OperationContext context, GetGroupID getGroupId)
    throws TException, OperationException {

    MetricsHelper helper = newHelper("getid", getGroupId.getQueueName());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + getGroupId);
      }
      TOperationContext tcontext = wrap(context);
      TGetGroupId tGetGroupId = wrap(getGroupId);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tGetGroupId);
      }
      long result = client.getGroupId(tcontext, tGetGroupId);
      if (Log.isTraceEnabled()) {
        Log.trace("Result of TGetGroupId: " + result);
      }
      helper.success();
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public OperationResult<QueueInfo> execute(OperationContext context, GetQueueInfo getQueueInfo)
    throws TException, OperationException {

    MetricsHelper helper = newHelper("info", getQueueInfo.getQueueName());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + getQueueInfo);
      }
      TOperationContext tcontext = wrap(context);
      TGetQueueInfo tGetQueueInfo = wrap(getQueueInfo);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tGetQueueInfo);
      }
      TQueueInfo tQueueInfo = client.getQueueInfo(tcontext, tGetQueueInfo);
      if (Log.isTraceEnabled()) {
        Log.trace("TGetQueueInfo successful.");
      }
      OperationResult<QueueInfo> queueInfo = unwrap(tQueueInfo);

      helper.finish(queueInfo.isEmpty() ? NoData : Success);
      return queueInfo;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(OperationContext context, ClearFabric clearFabric) throws TException, OperationException {

    MetricsHelper helper = newHelper("clear");

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + clearFabric);
      }
      TOperationContext tContext = wrap(context);
      TClearFabric tClearFabric = wrap(clearFabric);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tClearFabric);
      }
      client.clearFabric(tContext, tClearFabric);
      if (Log.isTraceEnabled()) {
        Log.trace("ClearFabric successful.");
      }
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(OperationContext context, OpenTable openTable) throws TException, OperationException {

    MetricsHelper helper = newHelper("open", openTable.getTableName());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + openTable);
      }
      TOperationContext tContext = wrap(context);
      TOpenTable tOpenTable = wrap(openTable);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tOpenTable);
      }
      client.openTable(tContext, tOpenTable);
      if (Log.isTraceEnabled()) {
        Log.trace("OpenTable successful.");
      }
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(OperationContext context, TruncateTable truncateTable) throws TException, OperationException {

    MetricsHelper helper = newHelper("truncate", truncateTable.getTableName());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + truncateTable);
      }
      TOperationContext tContext = wrap(context);
      TTruncateTable tTruncateTable = wrap(truncateTable);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tTruncateTable);
      }
      client.truncateTable(tContext, tTruncateTable);
      if (Log.isTraceEnabled()) {
        Log.trace("TruncateTable successful.");
      }
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context, Read read)
    throws OperationException, TException {

    MetricsHelper helper = newHelper("read", read.getTable());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + read);
      }
      TOperationContext tcontext = wrap(context);
      TRead tRead = wrap(read);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending TRead." + tRead);
      }
      TOptionalBinaryMap tResult = client.read(tcontext, tRead);
      if (Log.isTraceEnabled()) {
        Log.trace("TRead successful.");
      }
      OperationResult<Map<byte[], byte[]>> result = unwrap(tResult);

      helper.finish(result.isEmpty() ? NoData : Success);
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context, Transaction transaction, Read read)
    throws OperationException, TException {


    MetricsHelper helper = newHelper("read", read.getTable());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + read);
      }
      TOperationContext tcontext = wrap(context);
      TRead tRead = wrap(read);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending TRead." + tRead);
      }
      TOptionalBinaryMap tResult = client.readTx(tcontext, wrap(transaction), tRead);
      if (Log.isTraceEnabled()) {
        Log.trace("TRead successful.");
      }
      OperationResult<Map<byte[], byte[]>> result = unwrap(tResult);

      helper.finish(result.isEmpty() ? NoData : Success);
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }


  public OperationResult<List<byte[]>> execute(OperationContext context, ReadAllKeys readKeys)
    throws OperationException, TException {

    MetricsHelper helper = newHelper("listkeys", readKeys.getTable());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + readKeys);
      }
      TOperationContext tcontext = wrap(context);
      TReadAllKeys tReadAllKeys = wrap(readKeys);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tReadAllKeys);
      }
      TOptionalBinaryList tResult = client.readAllKeys(tcontext, tReadAllKeys);
      if (Log.isTraceEnabled()) {
        Log.trace("TReadAllKeys successful.");
      }
      OperationResult<List<byte[]>> result = unwrap(tResult);

      helper.finish(result.isEmpty() ? NoData : Success);
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public OperationResult<List<byte[]>> execute(OperationContext context, Transaction transaction, ReadAllKeys readKeys)
    throws OperationException, TException {

    MetricsHelper helper = newHelper("listkeys", readKeys.getTable());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + readKeys);
      }
      TOperationContext tcontext = wrap(context);
      TReadAllKeys tReadAllKeys = wrap(readKeys);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tReadAllKeys);
      }
      TOptionalBinaryList tResult = client.readAllKeysTx(tcontext, wrap(transaction), tReadAllKeys);
      if (Log.isTraceEnabled()) {
        Log.trace("TReadAllKeys successful.");
      }
      OperationResult<List<byte[]>> result = unwrap(tResult);

      helper.finish(result.isEmpty() ? NoData : Success);
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public OperationResult<List<KeyRange>> execute(OperationContext context, GetSplits getSplits)
    throws OperationException, TException {

    MetricsHelper helper = newHelper("getsplits", getSplits.getTable());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + getSplits);
      }
      TOperationContext tcontext = wrap(context);
      TGetSplits tReadAllKeys = wrap(getSplits);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tReadAllKeys);
      }
      List<TKeyRange> tResult = client.getSplits(tcontext, tReadAllKeys);
      if (Log.isTraceEnabled()) {
        Log.trace("TReadAllKeys successful.");
      }
      OperationResult<List<KeyRange>> result = unwrap(tResult);

      helper.finish(result.isEmpty() ? NoData : Success);
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public OperationResult<List<KeyRange>> execute(OperationContext context, Transaction tx, GetSplits getSplits)
    throws OperationException, TException {

    MetricsHelper helper = newHelper("getsplits", getSplits.getTable());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + getSplits);
      }
      TOperationContext tcontext = wrap(context);
      TGetSplits tReadAllKeys = wrap(getSplits);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tReadAllKeys);
      }
      List<TKeyRange> tResult = client.getSplitsTx(tcontext, wrap(tx), tReadAllKeys);
      if (Log.isTraceEnabled()) {
        Log.trace("TReadAllKeys successful.");
      }
      OperationResult<List<KeyRange>> result = unwrap(tResult);

      helper.finish(result.isEmpty() ? NoData : Success);
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context, ReadColumnRange readColumnRange)
    throws TException, OperationException {

    MetricsHelper helper = newHelper("range", readColumnRange.getTable());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received ReadColumnRange.");
      }
      TOperationContext tcontext = wrap(context);
      TReadColumnRange tReadColumnRange = wrap(readColumnRange);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending TReadColumnRange.");
      }
      TOptionalBinaryMap tResult = client.readColumnRange(tcontext, tReadColumnRange);
      if (Log.isTraceEnabled()) {
        Log.trace("TReadColumnRange successful.");
      }
      OperationResult<Map<byte[], byte[]>> result = unwrap(tResult);

      helper.finish(result.isEmpty() ? NoData : Success);
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context, Transaction transaction,
                                                      ReadColumnRange readColumnRange)
    throws TException, OperationException {

    MetricsHelper helper = newHelper("range", readColumnRange.getTable());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received ReadColumnRange.");
      }
      TOperationContext tcontext = wrap(context);
      TReadColumnRange tReadColumnRange = wrap(readColumnRange);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending TReadColumnRange.");
      }
      TOptionalBinaryMap tResult = client.readColumnRangeTx(tcontext, wrap(transaction), tReadColumnRange);
      if (Log.isTraceEnabled()) {
        Log.trace("TReadColumnRange successful.");
      }
      OperationResult<Map<byte[], byte[]>> result = unwrap(tResult);

      helper.finish(result.isEmpty() ? NoData : Success);
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(OperationContext context, QueueConfigure configure)
    throws TException, OperationException {

    MetricsHelper helper = newHelper("configure", configure.getQueueName());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + configure);
      }
      TOperationContext tContext = wrap(context);
      TQueueConfigure tQueueConfigure = wrap(configure);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tQueueConfigure);
      }
      client.configureQueue(tContext, tQueueConfigure);
      if (Log.isTraceEnabled()) {
        Log.trace("QueueConfigure successful.");
      }
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(OperationContext context,
                      QueueConfigureGroups configure)
    throws TException, OperationException {

    MetricsHelper helper = newHelper("configureGroup", configure.getQueueName());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + configure);
      }
      TOperationContext tContext = wrap(context);
      TQueueConfigureGroups tQueueConfigure = wrap(configure);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tQueueConfigure);
      }
      client.configureQueueGroups(tContext, tQueueConfigure);
      if (Log.isTraceEnabled()) {
        Log.trace("QueueConfigureGroups successful.");
      }
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(OperationContext context,
                      QueueDropInflight op)
    throws TException, OperationException {

    MetricsHelper helper = newHelper("QueueDropInFlight", op.getQueueName());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received " + op);
      }
      TOperationContext tContext = wrap(context);
      TQueueDropInflight tOp = wrap(op);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending " + tOp);
      }
      client.queueDropInflight(tContext, tOp);
      if (Log.isTraceEnabled()) {
        Log.trace("QueueDropInFlight successful.");
      }
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public Map<byte[], Long> increment(OperationContext context, Transaction transaction, Increment increment)
    throws TException, OperationException {

    MetricsHelper helper = newHelper("increment", increment.getTable());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received Increment.");
      }
      TOperationContext tcontext = wrap(context);
      TIncrement tIncrement = wrap(increment);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending TIncrement.");
      }
      Map<ByteBuffer, Long> tResult = client.incrementTx(tcontext, wrap(transaction), tIncrement);
      if (Log.isTraceEnabled()) {
        Log.trace("TIncrement successful.");
      }
      Map<byte[], Long> result = unwrapLongMap(tResult);

      helper.finish(result.isEmpty() ? NoData : Success);
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public Map<byte[], Long> increment(OperationContext context, Increment increment)
    throws TException, OperationException {

    MetricsHelper helper = newHelper("increment", increment.getTable());

    try {
      if (Log.isTraceEnabled()) {
        Log.trace("Received Increment.");
      }
      TOperationContext tcontext = wrap(context);
      TIncrement tIncrement = wrap(increment);
      if (Log.isTraceEnabled()) {
        Log.trace("Sending TIncrement.");
      }
      Map<ByteBuffer, Long> tResult = client.increment(tcontext, tIncrement);
      if (Log.isTraceEnabled()) {
        Log.trace("TIncrement successful.");
      }
      Map<byte[], Long> result = unwrapLongMap(tResult);

      helper.finish(result.isEmpty() ? NoData : Success);
      return result;

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }


  public String getName() {
    return "remote-client";
  }

  public com.continuuity.data2.transaction.Transaction startLong() throws OperationException, TException {
    try {
      return unwrap(client.startLong());
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public com.continuuity.data2.transaction.Transaction startShort() throws OperationException, TException {
    try {
      return unwrap(client.startShort());
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public com.continuuity.data2.transaction.Transaction startShort(int timeout) throws OperationException, TException {
    try {
      return unwrap(client.startShortTimeout(timeout));
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public boolean canCommit(com.continuuity.data2.transaction.Transaction tx, Collection<byte[]> changeIds)
    throws OperationException, TException{
    try {
      return client.canCommitTx(wrap(tx), ImmutableSet.copyOf(Iterables.transform(changeIds, BYTES_WRAPPER)));
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public boolean commit(com.continuuity.data2.transaction.Transaction tx) throws OperationException, TException {
    try {
      return client.commitTx(wrap(tx));
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public boolean abort(com.continuuity.data2.transaction.Transaction tx) throws OperationException, TException {
    try {
      return client.abortTx(wrap(tx));
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

}
