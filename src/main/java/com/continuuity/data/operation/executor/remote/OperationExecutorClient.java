package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.BatchOperationException;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.*;
import com.continuuity.data.operation.ttqueue.*;
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

  public OperationExecutorClient(TOperationExecutor.Client client) {
    this.client = client;
  }

  @Override
  synchronized
  public BatchOperationResult execute(List<WriteOperation> writes)
      throws BatchOperationException {
    Log.debug("Received Batch of " + writes.size() + "WriteOperations: ");
    List<TWriteOperation> tWrites = Lists.newArrayList();
    for (WriteOperation writeOp : writes) {
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
      Log.debug("Sending Batch: " + Arrays.toString(writes.toArray()));
      TBatchOperationResult result = client.batch(tWrites);
      Log.debug("Result of Batch: " + result);
      return new BatchOperationResult(result.isSuccess(), result.getMessage());
    } catch (TBatchOperationException e) {
      throw new BatchOperationException(e.getMessage(), e);
    } catch (TException e) {
      throw new BatchOperationException(e.getMessage(), e);
    }
  }

  @Override
  synchronized
  public DequeueResult execute(QueueDequeue dequeue) {
    try {
      Log.debug("Received " + dequeue);
      TQueueDequeue tDequeue = wrap(dequeue);
      Log.debug("Sending " + tDequeue);
      TDequeueResult tDequeueResult = client.dequeue(tDequeue);
      Log.debug("Result of TDequeue: " + tDequeueResult);
      return unwrap(tDequeueResult);
    } catch (TException e) {
      String message = "Thrift Call for QueueDequeue failed for queue " +
          new String(dequeue.getKey()) + ": " + e.getMessage();
      Log.error(message);
      return new DequeueResult(DequeueResult.DequeueStatus.FAILURE, message);
    }
  }

  @Override
  synchronized
  public long execute(QueueAdmin.GetGroupID getGroupId) {
    try {
      Log.debug("Received " + getGroupId);
      TGetGroupId tGetGroupId = wrap(getGroupId);
      Log.debug("Sending " + tGetGroupId);
      long result = client.getGroupId(tGetGroupId);
      Log.debug("Result of TGetGroupId: " + result);
      return result;
    } catch (TException e) {
      Log.error("Thrift Call for GetGroupId failed for queue " +
          new String(getGroupId.getQueueName()) + ": " + e.getMessage());
      return 0; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public QueueAdmin.QueueMeta execute(QueueAdmin.GetQueueMeta getQueueMeta) {
    try {
      Log.debug("Received " + getQueueMeta);
      TGetQueueMeta tGetQueueMeta = wrap(getQueueMeta);
      Log.debug("Sending " + tGetQueueMeta);
      TQueueMeta tQueueMeta = client.getQueueMeta(tGetQueueMeta);
      Log.debug("Result of TGetQueueMeta: " + tQueueMeta);
      return unwrap(tQueueMeta);
    } catch (TException e) {
      Log.error("Thrift Call for GetQueueMeta failed for queue " +
          new String(getQueueMeta.getQueueName()) + ": " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public void execute(ClearFabric clearFabric) {
    try {
      Log.debug("Received " + clearFabric);
      TClearFabric tClearFabric = wrap(clearFabric);
      Log.debug("Sending " + tClearFabric);
      client.clearFabric(tClearFabric);
    } catch (TException e) {
      Log.error("Thrift Call for ClearFabric failed with message: " +
          e.getMessage());
      // TODO execute() must be able to return an error
    }
  }

  @Override
  // synchronized
  public byte[] execute(ReadKey readKey) {
    try {
      Log.debug("Received " + readKey);
      TReadKey tReadKey = wrap(readKey);
      Log.debug("Sending " + tReadKey);
      TOptionalBinary result = client.readKey(tReadKey);
      Log.debug("Result of TReadKey: " + result);
      return unwrap(result);
    } catch (TException e) {
      Log.error("Thrift Call for ReadKey for key '" +
          new String(readKey.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public Map<byte[], byte[]> execute(Read read) {
    try {
      Log.debug("Received " + read);
      TRead tRead = wrap(read);
      Log.debug("Sending " + tRead);
      TOptionalBinaryMap result = client.read(tRead);
      Log.debug("Result of TRead: " + result);
      return unwrap(result);
    } catch (TException e) {
      Log.error("Thrift Call for Read for key '" +
          new String(read.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public List<byte[]> execute(ReadAllKeys readKeys) {
    try {
      Log.debug("Received " + readKeys);
      TReadAllKeys tReadAllKeys = wrap(readKeys);
      Log.debug("Sending " + tReadAllKeys);
      TOptionalBinaryList result = client.readAllKeys(tReadAllKeys);
      Log.debug("Result of TReadAllKeys: " + result);
      return unwrap(result);
    } catch (TException e) {
      Log.error("Thrift Call for ReadAllKeys(" + readKeys.getOffset() + ", " +
          readKeys.getLimit() + ") failed with message: " + e.getMessage());
      return new ArrayList<byte[]>(0);
      // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public Map<byte[], byte[]> execute(ReadColumnRange readColumnRange) {
    try {
      Log.debug("Received " + readColumnRange);
      TReadColumnRange tReadColumnRange = wrap(readColumnRange);
      Log.debug("Sending " + tReadColumnRange);
      TOptionalBinaryMap result = client.readColumnRange(tReadColumnRange);
      Log.debug("Result of TReadColumnRange: " + result);
      return unwrap(result);
    } catch (TException e) {
      Log.error("Thrift Call for ReadColumnRange for key '" +
          new String(readColumnRange.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  // synchronized
  public boolean execute(Write write) {
    try {
      Log.debug("Received " + write);
      TWrite tWrite = wrap(write);
      Log.debug("Sending " + tWrite);
      boolean result = client.write(tWrite);
      Log.debug("Result of TWrite: " + result);
      return result;
    } catch (TException e) {
      Log.error("Thrift Call for Write for key '" + new String(write.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(Delete delete) {
    try {
      Log.debug("Received " + delete);
      TDelete tDelete = wrap(delete);
      Log.debug("Sending " + tDelete);
      boolean result = client.delet(tDelete);
      Log.debug("Result of TDelete: " + result);
      return result;
    } catch (TException e) {
      Log.error("Thrift Call for Delete for key '" + new String(delete.getKey())
          + "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(Increment increment) {
    try {
      Log.debug("Received " + increment);
      TIncrement tIncrement = wrap(increment);
      Log.debug("Sending " + tIncrement);
      boolean result = client.increment(tIncrement);
      Log.debug("Result of TIncrement: " + result);
      return result;
    } catch (TException e) {
      Log.error("Thrift Call for Increment for key '" +
          new String(increment.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(CompareAndSwap compareAndSwap) {
    try {
      Log.debug("Received " + compareAndSwap);
      TCompareAndSwap tCompareAndSwap = wrap(compareAndSwap);
      Log.debug("Sending " + tCompareAndSwap);
      boolean result = client.compareAndSwap(tCompareAndSwap);
      Log.debug("Result of TCompareAndSwap: " + result);
      return result;
    } catch (TException e) {
      Log.error("Thrift Call for CompareAndSwap for key '" +
          new String(compareAndSwap.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(QueueEnqueue enqueue) {
    try {
      Log.debug("Received " + enqueue);
      TQueueEnqueue tQueueEnqueue = wrap(enqueue);
      Log.debug("Sending " + tQueueEnqueue);
      boolean result = client.queueEnqueue(tQueueEnqueue);
      Log.debug("Result of TQueueEnqueue: " + result);
      return result;
    } catch (TException e) {
      Log.error("Thrift Call for QueueEnqueue for queue '" +
          new String(enqueue.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(QueueAck ack) {
    try {
      Log.debug("Received " + ack);
      TQueueAck tQueueAck = wrap(ack);
      Log.debug("Sending " + tQueueAck);
      boolean result = client.queueAck(tQueueAck);
      Log.debug("Result of TQueueAck: " + result);
      return result;
    } catch (TException e) {
      Log.error("Thrift Call for QueueAck for queue '" +
          new String(ack.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }
}
