package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.remote.stubs.*;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.metrics2.api.CMetrics;
import com.continuuity.metrics2.collector.MetricType;
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

  public void execute(List<WriteOperation> writes)
      throws OperationException, TException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_BATCH_REQUESTS,
        Constants.METRIC_BATCH_LATENCY);

    if (Log.isDebugEnabled())
      Log.debug("Received Batch of " + writes.size() + "WriteOperations: ");

    List<TWriteOperation> tWrites = Lists.newArrayList();
    for (WriteOperation writeOp : writes) {
      if (Log.isDebugEnabled())
        Log.debug("  WriteOperation: " + writeOp.toString());
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
      if (Log.isDebugEnabled()) Log.debug("Sending Batch.");
      client.batch(tWrites);
      if (Log.isDebugEnabled()) Log.debug("Batch successful.");
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public DequeueResult execute(QueueDequeue dequeue)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_DEQUEUE_REQUESTS,
        Constants.METRIC_DEQUEUE_LATENCY);

    try {
      if (Log.isDebugEnabled()) Log.debug("Received " + dequeue);
      TQueueDequeue tDequeue = wrap(dequeue);
      if (Log.isDebugEnabled()) Log.debug("Sending " + tDequeue);
      TDequeueResult tDequeueResult = client.dequeue(tDequeue);
      if (Log.isDebugEnabled()) Log.debug("TDequeue successful.");
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

  public long execute(QueueAdmin.GetGroupID getGroupId)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_GETGROUPID_REQUESTS,
        Constants.METRIC_GETGROUPID_LATENCY);

    try {
      if (Log.isDebugEnabled()) Log.debug("Received " + getGroupId);
      TGetGroupId tGetGroupId = wrap(getGroupId);
      if (Log.isDebugEnabled()) Log.debug("Sending " + tGetGroupId);
      long result = client.getGroupId(tGetGroupId);
      if (Log.isDebugEnabled()) Log.debug("Result of TGetGroupId: " + result);
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
  execute(QueueAdmin.GetQueueMeta getQueueMeta)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_GETQUEUEMETA_REQUESTS,
        Constants.METRIC_GETQUEUEMETA_LATENCY);

    if (Log.isDebugEnabled()) Log.debug("Received " + getQueueMeta);

    try {
      TGetQueueMeta tGetQueueMeta = wrap(getQueueMeta);
      if (Log.isDebugEnabled()) Log.debug("Sending " + tGetQueueMeta);
      TQueueMeta tQueueMeta = client.getQueueMeta(tGetQueueMeta);
      if (Log.isDebugEnabled()) Log.debug("TGetQueueMeta successful.");
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

  public void execute(ClearFabric clearFabric)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_CLEARFABRIC_REQUESTS,
        Constants.METRIC_CLEARFABRIC_LATENCY);

    if (Log.isDebugEnabled()) Log.debug("Received " + clearFabric);

    try {
      TClearFabric tClearFabric = wrap(clearFabric);
      if (Log.isDebugEnabled()) Log.debug("Sending " + tClearFabric);
      client.clearFabric(tClearFabric);
      if (Log.isDebugEnabled()) Log.debug("ClearFabric successful.");
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public OperationResult<byte[]> execute(ReadKey readKey)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READKEY_REQUESTS,
        Constants.METRIC_READKEY_LATENCY);

    if (Log.isDebugEnabled()) Log.debug("Received " + readKey);

    try {
      TReadKey tReadKey = wrap(readKey);
      if (Log.isDebugEnabled()) Log.debug("Sending TReadKey" + tReadKey);
      TOptionalBinary tResult = client.readKey(tReadKey);
      if (Log.isDebugEnabled()) Log.debug("TReadKey successful.");
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

  public OperationResult<Map<byte[], byte[]>> execute(Read read)
      throws OperationException, TException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READ_REQUESTS,
        Constants.METRIC_READ_LATENCY);

    if (Log.isDebugEnabled()) Log.debug("Received " + read);

    try {
      TRead tRead = wrap(read);
      if (Log.isDebugEnabled()) Log.debug("Sending TRead." + tRead);
      TOptionalBinaryMap tResult = client.read(tRead);
      if (Log.isDebugEnabled()) Log.debug("TRead successful.");
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

  public OperationResult<List<byte[]>> execute(ReadAllKeys readKeys)
      throws OperationException, TException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READALLKEYS_REQUESTS,
        Constants.METRIC_READALLKEYS_LATENCY);

    if (Log.isDebugEnabled()) Log.debug("Received " + readKeys);
    try {
      TReadAllKeys tReadAllKeys = wrap(readKeys);
      if (Log.isDebugEnabled()) Log.debug("Sending " + tReadAllKeys);
      TOptionalBinaryList tResult = client.readAllKeys(tReadAllKeys);
      if (Log.isDebugEnabled()) Log.debug("TReadAllKeys successful.");
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
  execute(ReadColumnRange readColumnRange)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_READCOLUMNRANGE_REQUESTS,
        Constants.METRIC_READCOLUMNRANGE_LATENCY);

    if (Log.isDebugEnabled()) Log.debug("Received ReadColumnRange.");

    try {
      TReadColumnRange tReadColumnRange = wrap(readColumnRange);
      if (Log.isDebugEnabled()) Log.debug("Sending TReadColumnRange.");
      TOptionalBinaryMap tResult = client.readColumnRange(tReadColumnRange);
      if (Log.isDebugEnabled()) Log.debug("TReadColumnRange successful.");
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

  public void execute(Write write) throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_WRITE_REQUESTS,
        Constants.METRIC_WRITE_LATENCY);

    try {
      if (Log.isDebugEnabled()) Log.debug("Received Write.");
      TWrite tWrite = wrap(write);
      if (Log.isDebugEnabled()) Log.debug("Sending TWrite.");
      client.write(tWrite);
      if (Log.isDebugEnabled()) Log.debug("TWrite successful.");
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(Delete delete) throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_DELETE_REQUESTS,
        Constants.METRIC_DELETE_LATENCY);

    try {
      if (Log.isDebugEnabled()) Log.debug("Received Delete.");
      TDelete tDelete = wrap(delete);
      if (Log.isDebugEnabled()) Log.debug("Sending TDelete.");
      client.delet(tDelete);
      if (Log.isDebugEnabled()) Log.debug("TDelete successful.");
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(Increment increment)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_INCREMENT_REQUESTS,
        Constants.METRIC_INCREMENT_LATENCY);

    try {
      if (Log.isDebugEnabled()) Log.debug("Received Increment.");
      TIncrement tIncrement = wrap(increment);
      if (Log.isDebugEnabled()) Log.debug("Sending TIncrement.");
      client.increment(tIncrement);
      if (Log.isDebugEnabled()) Log.debug("TIncrement successful.");
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(CompareAndSwap compareAndSwap)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_COMPAREANDSWAP_REQUESTS,
        Constants.METRIC_COMPAREANDSWAP_LATENCY);

    try {
      if (Log.isDebugEnabled()) Log.debug("Received CompareAndSwap.");
      TCompareAndSwap tCompareAndSwap = wrap(compareAndSwap);
      if (Log.isDebugEnabled()) Log.debug("Sending TCompareAndSwap.");
      client.compareAndSwap(tCompareAndSwap);
      if (Log.isDebugEnabled()) Log.debug("TCompareAndSwap successful.");
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(QueueEnqueue enqueue)
      throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_ENQUEUE_REQUESTS,
        Constants.METRIC_ENQUEUE_LATENCY);

    try {
      if (Log.isDebugEnabled()) Log.debug("Received Enqueue.");
      TQueueEnqueue tQueueEnqueue = wrap(enqueue);
      if (Log.isDebugEnabled()) Log.debug("Sending TQueueEnqueue.");
      client.queueEnqueue(tQueueEnqueue);
      if (Log.isDebugEnabled()) Log.debug("TQueueEnqueue successful.");
      helper.success();

    } catch (TOperationException te) {
      helper.failure();
      throw unwrap(te);

    } catch (TException te) {
      helper.failure();
      throw te;
    }
  }

  public void execute(QueueAck ack) throws TException, OperationException {

    MetricsHelper helper = newHelper(
        Constants.METRIC_ACK_REQUESTS,
        Constants.METRIC_ACK_LATENCY);

    try {
      if (Log.isDebugEnabled()) Log.debug("Received " + ack);
      TQueueAck tQueueAck = wrap(ack);
      if (Log.isDebugEnabled()) Log.debug("Sending " + tQueueAck);
      client.queueAck(tQueueAck);
      if (Log.isDebugEnabled()) Log.debug("TQueueAck successful.");
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
