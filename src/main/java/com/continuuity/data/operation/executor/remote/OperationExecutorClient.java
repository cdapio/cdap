package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.BatchOperationException;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.*;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.metrics2.api.CMetrics;
import com.continuuity.metrics2.collector.MetricType;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class OperationExecutorClient
    extends ConverterUtils implements OperationExecutor {

  private static final Logger Log =
      LoggerFactory.getLogger(OperationExecutorClient.class);

  TOperationExecutor.Client client;

  CMetrics metrics;

  public OperationExecutorClient(TOperationExecutor.Client client) {
    this.client = client;
    this.metrics = new CMetrics(MetricType.System);
  }

  @Override
  public BatchOperationResult execute(List<WriteOperation> writes)
      throws BatchOperationException {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_BATCH_REQUESTS);

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
      if (Log.isDebugEnabled())
        Log.debug("Sending Batch: " + Arrays.toString(writes.toArray()));

      TBatchOperationResult tResult = client.batch(tWrites);

      if (Log.isDebugEnabled())
        Log.debug("Result of Batch: " + tResult);

      BatchOperationResult result = unwrap(tResult);

      helper.finish(Constants.METRIC_SUCCESS, Constants.METRIC_BATCH_LATENCY);
      return result;

    } catch (Exception e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      throw new BatchOperationException(e.getMessage(), e);
    }
  }

  @Override
  public DequeueResult execute(QueueDequeue dequeue) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_DEQUEUE_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + dequeue);

      TQueueDequeue tDequeue = wrap(dequeue);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tDequeue);

      TDequeueResult tDequeueResult = client.dequeue(tDequeue);

      if (Log.isDebugEnabled())
        Log.debug("Result of TDequeue: " + tDequeueResult);

      DequeueResult dequeueResult = unwrap(tDequeueResult);
      helper.finish(Constants.METRIC_SUCCESS, Constants.METRIC_DEQUEUE_LATENCY);
      return dequeueResult;

    } catch (TException e) {
      String message = "Thrift Call for QueueDequeue failed for queue " +
          new String(dequeue.getKey()) + ": " + e.getMessage();
      Log.error(message);
      helper.finish(Constants.METRIC_FAILURE, null);
      return new DequeueResult(DequeueResult.DequeueStatus.FAILURE, message);
    }
  }

  @Override
  public long execute(QueueAdmin.GetGroupID getGroupId) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_GETGROUPID_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + getGroupId);

      TGetGroupId tGetGroupId = wrap(getGroupId);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tGetGroupId);

      long result = client.getGroupId(tGetGroupId);

      if (Log.isDebugEnabled())
        Log.debug("Result of TGetGroupId: " + result);

      helper.finish(Constants.METRIC_SUCCESS,
          Constants.METRIC_GETGROUPID_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for GetGroupId failed for queue " +
          new String(getGroupId.getQueueName()) + ": " + e.getMessage());
      return 0; // TODO execute() must be able to return an error
    }
  }

  @Override
  public QueueAdmin.QueueMeta execute(QueueAdmin.GetQueueMeta getQueueMeta) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_GETQUEUEMETA_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + getQueueMeta);

      TGetQueueMeta tGetQueueMeta = wrap(getQueueMeta);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tGetQueueMeta);

      TQueueMeta tQueueMeta = client.getQueueMeta(tGetQueueMeta);

      if (Log.isDebugEnabled())
        Log.debug("Result of TGetQueueMeta: " + tQueueMeta);

      QueueAdmin.QueueMeta queueMeta = unwrap(tQueueMeta);
      helper.finish(Constants.METRIC_SUCCESS,
          Constants.METRIC_GETQUEUEMETA_LATENCY);
      return queueMeta;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for GetQueueMeta failed for queue " +
          new String(getQueueMeta.getQueueName()) + ": " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  public void execute(ClearFabric clearFabric) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_CLEARFABRIC_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + clearFabric);

      TClearFabric tClearFabric = wrap(clearFabric);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tClearFabric);

      client.clearFabric(tClearFabric);

      helper.finish(Constants.METRIC_SUCCESS,
          Constants.METRIC_CLEARFABRIC_LATENCY);

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for ClearFabric failed with message: " +
          e.getMessage());
      // TODO execute() must be able to return an error
    }
  }

  @Override
  public byte[] execute(ReadKey readKey) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_READKEY_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + readKey);

      TReadKey tReadKey = wrap(readKey);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tReadKey);

      TOptionalBinary tResult = client.readKey(tReadKey);

      if (Log.isDebugEnabled())
        Log.debug("Result of TReadKey: " + tResult);

      byte[] result = unwrap(tResult);
      helper.finish(Constants.METRIC_SUCCESS, Constants.METRIC_READKEY_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for ReadKey for key '" +
          new String(readKey.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  public Map<byte[], byte[]> execute(Read read) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_READ_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + read);

      TRead tRead = wrap(read);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tRead);

      TOptionalBinaryMap tResult = client.read(tRead);

      if (Log.isDebugEnabled())
        Log.debug("Result of TRead: " + tResult);

      Map<byte[], byte[]> result = unwrap(tResult);
      helper.finish(Constants.METRIC_SUCCESS, Constants.METRIC_READ_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for Read for key '" +
          new String(read.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  public List<byte[]> execute(ReadAllKeys readKeys) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_READALLKEYS_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + readKeys);

      TReadAllKeys tReadAllKeys = wrap(readKeys);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tReadAllKeys);

      TOptionalBinaryList tResult = client.readAllKeys(tReadAllKeys);

      if (Log.isDebugEnabled())
        Log.debug("Result of TReadAllKeys: " + tResult);

      List<byte[]> result = unwrap(tResult);
      helper.finish(Constants.METRIC_SUCCESS,
          Constants.METRIC_READALLKEYS_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for ReadAllKeys(" + readKeys.getOffset() + ", " +
          readKeys.getLimit() + ") failed with message: " + e.getMessage());
      return new ArrayList<byte[]>(0);
      // TODO execute() must be able to return an error
    }
  }

  @Override
  public Map<byte[], byte[]> execute(ReadColumnRange readColumnRange) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_READCOLUMNRANGE_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + readColumnRange);

      TReadColumnRange tReadColumnRange = wrap(readColumnRange);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tReadColumnRange);

      TOptionalBinaryMap tResult = client.readColumnRange(tReadColumnRange);

      if (Log.isDebugEnabled())
        Log.debug("Result of TReadColumnRange: " + tResult);

      Map<byte[], byte[]> result = unwrap(tResult);
      helper.finish(Constants.METRIC_SUCCESS,
          Constants.METRIC_READCOLUMNRANGE_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for ReadColumnRange for key '" +
          new String(readColumnRange.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(Write write) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_WRITE_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + write);

      TWrite tWrite = wrap(write);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tWrite);

      boolean result = client.write(tWrite);

      if (Log.isDebugEnabled())
        Log.debug("Result of TWrite: " + result);

      helper.finish(Constants.METRIC_SUCCESS, Constants.METRIC_WRITE_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for Write for key '" + new String(write.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(Delete delete) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_DELETE_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + delete);

      TDelete tDelete = wrap(delete);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tDelete);

      boolean result = client.delet(tDelete);

      if (Log.isDebugEnabled())
        Log.debug("Result of TDelete: " + result);

      helper.finish(Constants.METRIC_SUCCESS, Constants.METRIC_DELETE_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for Delete for key '" + new String(delete.getKey())
          + "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(Increment increment) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_INCREMENT_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + increment);

      TIncrement tIncrement = wrap(increment);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tIncrement);

      boolean result = client.increment(tIncrement);

      if (Log.isDebugEnabled())
        Log.debug("Result of TIncrement: " + result);

      helper.finish(Constants.METRIC_SUCCESS,
          Constants.METRIC_INCREMENT_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for Increment for key '" +
          new String(increment.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(CompareAndSwap compareAndSwap) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_COMPAREANDSWAP_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + compareAndSwap);

      TCompareAndSwap tCompareAndSwap = wrap(compareAndSwap);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tCompareAndSwap);

      boolean result = client.compareAndSwap(tCompareAndSwap);

      if (Log.isDebugEnabled())
        Log.debug("Result of TCompareAndSwap: " + result);

      helper.finish(Constants.METRIC_SUCCESS,
          Constants.METRIC_COMPAREANDSWAP_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for CompareAndSwap for key '" +
          new String(compareAndSwap.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(QueueEnqueue enqueue) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_ENQUEUE_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + enqueue);

      TQueueEnqueue tQueueEnqueue = wrap(enqueue);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tQueueEnqueue);

      boolean result = client.queueEnqueue(tQueueEnqueue);

      if (Log.isDebugEnabled())
        Log.debug("Result of TQueueEnqueue: " + result);

      helper.finish(Constants.METRIC_SUCCESS,
          Constants.METRIC_ENQUEUE_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for QueueEnqueue for queue '" +
          new String(enqueue.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(QueueAck ack) {

    MetricsHelper helper = new MetricsHelper(this.metrics, this.getClass(),
        Constants.METRIC_REQUESTS, Constants.METRIC_ACK_REQUESTS);

    try {
      if (Log.isDebugEnabled())
        Log.debug("Received " + ack);

      TQueueAck tQueueAck = wrap(ack);

      if (Log.isDebugEnabled())
        Log.debug("Sending " + tQueueAck);

      boolean result = client.queueAck(tQueueAck);

      if (Log.isDebugEnabled())
        Log.debug("Result of TQueueAck: " + result);

      helper.finish(Constants.METRIC_SUCCESS, Constants.METRIC_ACK_LATENCY);
      return result;

    } catch (TException e) {
      helper.finish(Constants.METRIC_FAILURE, null);
      Log.error("Thrift Call for QueueAck for queue '" +
          new String(ack.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public String getName() {
    return "remote-client";
  }
}
