package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.remote.stubs.*;
import com.continuuity.data.operation.ttqueue.*;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.continuuity.common.metrics.MetricsHelper.Status.NoData;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;
import static com.continuuity.data.operation.ttqueue.QueueAdmin.GetQueueInfo;
import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

/**
 * This class is a wrapper around the thrift opex client, it takes
 * Operations, converts them into thrift objects, calls the thrift
 * client, and converts the results back to data fabric classes.
 * This class also instruments the thrift calls with metrics.
 */
public class OperationExecutorClient extends ConverterUtils {

  private static final Logger Log =
      LoggerFactory.getLogger(OperationExecutorClient.class);

  /** The thrift transport layer. We need this when we close the connection */
  TTransport transport;

  /** The actual thrift client */
  TOperationExecutor.Client client;

  /** The metrics collection client */
  CMetrics metrics = new CMetrics(MetricType.System);

  /** helper method to create a metrics helper */
  MetricsHelper newHelper(String method) {
    return new MetricsHelper(
        this.getClass(), this.metrics, "opex.client", method);
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

  /**
   * Constructor from an existing, connected thrift transport
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

  /** close this client. may be called multiple times */
  public void close() {
    if (this.transport.isOpen())
      this.transport.close();
  }

  public void execute(OperationContext context,
                      List<WriteOperation> writes)
      throws OperationException, TException {

    MetricsHelper helper = newHelper("batch");

    if (Log.isTraceEnabled())
      Log.trace("Received Batch of " + writes.size() + "WriteOperations: ");

    TOperationContext tcontext = wrap(context);

    List<TWriteOperation> tWrites = Lists.newArrayList();
    for (WriteOperation writeOp : writes) {
      if (Log.isTraceEnabled())
        Log.trace("  WriteOperation: " + writeOp.toString());
      TWriteOperation tWriteOp = new TWriteOperation();
      if (writeOp instanceof Write)
        tWriteOp.setWrite(wrap((Write)writeOp));
      else if (writeOp instanceof Delete)
        tWriteOp.setDelet(wrap((Delete)writeOp));
      else if (writeOp instanceof Increment)
        tWriteOp.setIncrement(wrap((Increment) writeOp));
      else if (writeOp instanceof CompareAndSwap)
        tWriteOp.setCompareAndSwap(wrap((CompareAndSwap) writeOp));
      else if (writeOp instanceof QueueEnqueue)
        tWriteOp.setQueueEnqueue(wrap((QueueEnqueue) writeOp));
      else if (writeOp instanceof QueueAck)
        tWriteOp.setQueueAck(wrap((QueueAck) writeOp));
      else {
        Log.error("Internal Error: Received an unknown WriteOperation of class "
            + writeOp.getClass().getName() + ".");
        continue;
      }
      tWrites.add(tWriteOp);
    }
    try {
      if (Log.isTraceEnabled()) Log.trace("Sending Batch.");
      client.batch(tcontext, tWrites);
      if (Log.isTraceEnabled()) Log.trace("Batch successful.");
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public DequeueResult execute(OperationContext context,
                               QueueDequeue dequeue)
      throws TException, OperationException {

    MetricsHelper helper = newHelper("dequeue", dequeue.getKey());

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + dequeue);
      TOperationContext tcontext = wrap(context);
      TQueueDequeue tDequeue = wrap(dequeue);
      if (Log.isTraceEnabled()) Log.trace("Sending " + tDequeue);
      TDequeueResult tDequeueResult = client.dequeue(tcontext, tDequeue);
      if (Log.isTraceEnabled()) Log.trace("TDequeue successful.");
      DequeueResult dequeueResult = unwrap(tDequeueResult);

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

  public long execute(OperationContext context,
                      QueueAdmin.GetGroupID getGroupId)
      throws TException, OperationException {

    MetricsHelper helper = newHelper("getid", getGroupId.getQueueName());

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + getGroupId);
      TOperationContext tcontext = wrap(context);
      TGetGroupId tGetGroupId = wrap(getGroupId);
      if (Log.isTraceEnabled()) Log.trace("Sending " + tGetGroupId);
      long result = client.getGroupId(tcontext, tGetGroupId);
      if (Log.isTraceEnabled()) Log.trace("Result of TGetGroupId: " + result);
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

  public OperationResult<QueueInfo> execute(OperationContext context,
                                            GetQueueInfo getQueueInfo)
      throws TException, OperationException {

    MetricsHelper helper = newHelper("info", getQueueInfo.getQueueName());

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + getQueueInfo);
      TOperationContext tcontext = wrap(context);
      TGetQueueInfo tGetQueueInfo = wrap(getQueueInfo);
      if (Log.isTraceEnabled()) Log.trace("Sending " + tGetQueueInfo);
      TQueueInfo tQueueInfo = client.getQueueInfo(tcontext, tGetQueueInfo);
      if (Log.isTraceEnabled()) Log.trace("TGetQueueInfo successful.");
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

  public void execute(OperationContext context,
                      ClearFabric clearFabric)
      throws TException, OperationException {

    MetricsHelper helper = newHelper("clear");

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + clearFabric);
      TOperationContext tContext = wrap(context);
      TClearFabric tClearFabric = wrap(clearFabric);
      if (Log.isTraceEnabled()) Log.trace("Sending " + tClearFabric);
      client.clearFabric(tContext, tClearFabric);
      if (Log.isTraceEnabled()) Log.trace("ClearFabric successful.");
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(OperationContext context, OpenTable openTable)
      throws TException, OperationException {

    MetricsHelper helper = newHelper("open", openTable.getTableName());

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + openTable);
      TOperationContext tContext = wrap(context);
      TOpenTable tOpenTable = wrap(openTable);
      if (Log.isTraceEnabled()) Log.trace("Sending " + tOpenTable);
      client.openTable(tContext, tOpenTable);
      if (Log.isTraceEnabled()) Log.trace("OpenTable successful.");
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Read read)
      throws OperationException, TException {

    MetricsHelper helper = newHelper("read", read.getTable());

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + read);
      TOperationContext tcontext = wrap(context);
      TRead tRead = wrap(read);
      if (Log.isTraceEnabled()) Log.trace("Sending TRead." + tRead);
      TOptionalBinaryMap tResult = client.read(tcontext, tRead);
      if (Log.isTraceEnabled()) Log.trace("TRead successful.");
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

  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               ReadAllKeys readKeys)
      throws OperationException, TException {

    MetricsHelper helper = newHelper("listkeys", readKeys.getTable());

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + readKeys);
      TOperationContext tcontext = wrap(context);
      TReadAllKeys tReadAllKeys = wrap(readKeys);
      if (Log.isTraceEnabled()) Log.trace("Sending " + tReadAllKeys);
      TOptionalBinaryList tResult = client.readAllKeys(tcontext, tReadAllKeys);
      if (Log.isTraceEnabled()) Log.trace("TReadAllKeys successful.");
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

  public OperationResult<Map<byte[], byte[]>>
  execute(OperationContext context,
          ReadColumnRange readColumnRange)
      throws TException, OperationException {

    MetricsHelper helper = newHelper("range", readColumnRange.getTable());

    try {
      if (Log.isTraceEnabled()) Log.trace("Received ReadColumnRange.");
      TOperationContext tcontext = wrap(context);
      TReadColumnRange tReadColumnRange = wrap(readColumnRange);
      if (Log.isTraceEnabled()) Log.trace("Sending TReadColumnRange.");
      TOptionalBinaryMap tResult =
          client.readColumnRange(tcontext, tReadColumnRange);
      if (Log.isTraceEnabled()) Log.trace("TReadColumnRange successful.");
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

  public String getName() {
    return "remote-client";
  }

}
