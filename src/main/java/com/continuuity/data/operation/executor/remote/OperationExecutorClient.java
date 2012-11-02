package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.executor.remote.stubs.*;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

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
  MetricsHelper newHelper(String meter, String histogram) {
    return new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, meter, histogram);
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

    MetricsHelper helper = newHelper(
        Constants.METRIC_BATCH_REQUESTS,
        Constants.METRIC_BATCH_LATENCY);

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

    MetricsHelper helper = newHelper(
        Constants.METRIC_DEQUEUE_REQUESTS,
        Constants.METRIC_DEQUEUE_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + dequeue);
      TOperationContext tcontext = wrap(context);
      TQueueDequeue tDequeue = wrap(dequeue);
      if (Log.isTraceEnabled()) Log.trace("Sending " + tDequeue);
      TDequeueResult tDequeueResult = client.dequeue(tcontext, tDequeue);
      if (Log.isTraceEnabled()) Log.trace("TDequeue successful.");
      DequeueResult dequeueResult = unwrap(tDequeueResult);
      helper.success();
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

    MetricsHelper helper = newHelper(
        Constants.METRIC_GETGROUPID_REQUESTS,
        Constants.METRIC_GETGROUPID_LATENCY);

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

  public OperationResult<QueueAdmin.QueueMeta>
  execute(OperationContext context,
          QueueAdmin.GetQueueMeta getQueueMeta)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_GETQUEUEMETA_REQUESTS,
        Constants.METRIC_GETQUEUEMETA_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + getQueueMeta);
      TOperationContext tcontext = wrap(context);
      TGetQueueMeta tGetQueueMeta = wrap(getQueueMeta);
      if (Log.isTraceEnabled()) Log.trace("Sending " + tGetQueueMeta);
      TQueueMeta tQueueMeta = client.getQueueMeta(tcontext, tGetQueueMeta);
      if (Log.isTraceEnabled()) Log.trace("TGetQueueMeta successful.");
      OperationResult<QueueAdmin.QueueMeta> queueMeta = unwrap(tQueueMeta);
      helper.success();
      return queueMeta;

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

    MetricsHelper helper = newHelper(
        Constants.METRIC_CLEARFABRIC_REQUESTS,
        Constants.METRIC_CLEARFABRIC_LATENCY);

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

    MetricsHelper helper = newHelper(
        Constants.METRIC_OPENTABLE_REQUESTS,
        Constants.METRIC_OPENTABLE_LATENCY);

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

  public OperationResult<byte[]> execute(OperationContext context,
                                         ReadKey readKey)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READKEY_REQUESTS,
        Constants.METRIC_READKEY_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + readKey);
      TOperationContext tcontext = wrap(context);
      TReadKey tReadKey = wrap(readKey);
      if (Log.isTraceEnabled()) Log.trace("Sending TReadKey" + tReadKey);
      TOptionalBinary tResult = client.readKey(tcontext, tReadKey);
      if (Log.isTraceEnabled()) Log.trace("TReadKey successful.");
      OperationResult<byte[]> result = unwrap(tResult);
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

  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Read read)
      throws OperationException, TException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READ_REQUESTS,
        Constants.METRIC_READ_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + read);
      TOperationContext tcontext = wrap(context);
      TRead tRead = wrap(read);
      if (Log.isTraceEnabled()) Log.trace("Sending TRead." + tRead);
      TOptionalBinaryMap tResult = client.read(tcontext, tRead);
      if (Log.isTraceEnabled()) Log.trace("TRead successful.");
      OperationResult<Map<byte[], byte[]>> result = unwrap(tResult);
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

  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               ReadAllKeys readKeys)
      throws OperationException, TException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READALLKEYS_REQUESTS,
        Constants.METRIC_READALLKEYS_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + readKeys);
      TOperationContext tcontext = wrap(context);
      TReadAllKeys tReadAllKeys = wrap(readKeys);
      if (Log.isTraceEnabled()) Log.trace("Sending " + tReadAllKeys);
      TOptionalBinaryList tResult = client.readAllKeys(tcontext, tReadAllKeys);
      if (Log.isTraceEnabled()) Log.trace("TReadAllKeys successful.");
      OperationResult<List<byte[]>> result = unwrap(tResult);
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

  public OperationResult<Map<byte[], byte[]>>
  execute(OperationContext context,
          ReadColumnRange readColumnRange)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READCOLUMNRANGE_REQUESTS,
        Constants.METRIC_READCOLUMNRANGE_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received ReadColumnRange.");
      TOperationContext tcontext = wrap(context);
      TReadColumnRange tReadColumnRange = wrap(readColumnRange);
      if (Log.isTraceEnabled()) Log.trace("Sending TReadColumnRange.");
      TOptionalBinaryMap tResult =
          client.readColumnRange(tcontext, tReadColumnRange);
      if (Log.isTraceEnabled()) Log.trace("TReadColumnRange successful.");
      OperationResult<Map<byte[], byte[]>> result = unwrap(tResult);
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

  public void execute(OperationContext context,
                      Write write) throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_WRITE_REQUESTS,
        Constants.METRIC_WRITE_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received Write.");
      TOperationContext tcontext = wrap(context);
      TWrite tWrite = wrap(write);
      if (Log.isTraceEnabled()) Log.trace("Sending TWrite.");
      client.write(tcontext, tWrite);
      if (Log.isTraceEnabled()) Log.trace("TWrite successful.");
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
                      Delete delete) throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_DELETE_REQUESTS,
        Constants.METRIC_DELETE_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received Delete.");
      TOperationContext tcontext = wrap(context);
      TDelete tDelete = wrap(delete);
      if (Log.isTraceEnabled()) Log.trace("Sending TDelete.");
      client.delet(tcontext, tDelete);
      if (Log.isTraceEnabled()) Log.trace("TDelete successful.");
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
                      Increment increment)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_INCREMENT_REQUESTS,
        Constants.METRIC_INCREMENT_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received Increment.");
      TOperationContext tcontext = wrap(context);
      TIncrement tIncrement = wrap(increment);
      if (Log.isTraceEnabled()) Log.trace("Sending TIncrement.");
      client.increment(tcontext, tIncrement);
      if (Log.isTraceEnabled()) Log.trace("TIncrement successful.");
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
                      CompareAndSwap compareAndSwap)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_COMPAREANDSWAP_REQUESTS,
        Constants.METRIC_COMPAREANDSWAP_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received CompareAndSwap.");
      TOperationContext tcontext = wrap(context);
      TCompareAndSwap tCompareAndSwap = wrap(compareAndSwap);
      if (Log.isTraceEnabled()) Log.trace("Sending TCompareAndSwap.");
      client.compareAndSwap(tcontext, tCompareAndSwap);
      if (Log.isTraceEnabled()) Log.trace("TCompareAndSwap successful.");
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
                      QueueEnqueue enqueue)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_ENQUEUE_REQUESTS,
        Constants.METRIC_ENQUEUE_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received EnqueuePayload.");
      TOperationContext tcontext = wrap(context);
      TQueueEnqueue tQueueEnqueue = wrap(enqueue);
      if (Log.isTraceEnabled()) Log.trace("Sending TQueueEnqueue.");
      client.queueEnqueue(tcontext, tQueueEnqueue);
      if (Log.isTraceEnabled()) Log.trace("TQueueEnqueue successful.");
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
                      QueueAck ack)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_ACK_REQUESTS,
        Constants.METRIC_ACK_LATENCY);

    try {
      if (Log.isTraceEnabled()) Log.trace("Received " + ack);
      TOperationContext tcontext = wrap(context);
      TQueueAck tQueueAck = wrap(ack);
      if (Log.isTraceEnabled()) Log.trace("Sending " + tQueueAck);
      client.queueAck(tcontext, tQueueAck);
      if (Log.isTraceEnabled()) Log.trace("TQueueAck successful.");
      helper.success();

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
