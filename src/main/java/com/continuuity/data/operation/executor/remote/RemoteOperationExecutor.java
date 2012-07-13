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
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RemoteOperationExecutor
    extends ConverterUtils
    implements OperationExecutor {

  private static final Logger Log =
      LoggerFactory.getLogger(RemoteOperationExecutor.class);

  TOperationExecutor.Client client;

  public RemoteOperationExecutor(String host, int port) throws Exception {
    TTransport transport = new TFramedTransport(new TSocket(host, port));
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new IOException("Unable to connect to service", e);
    }
    TProtocol protocol = new TBinaryProtocol(transport);
    client = new TOperationExecutor.Client(protocol);
  }

  @Override
  public BatchOperationResult execute(List<WriteOperation> writes)
      throws BatchOperationException {
    List<TWriteOperation> tWrites = Lists.newArrayList();
    for (WriteOperation writeOp : writes) {
      TWriteOperation tWriteOp = new TWriteOperation();
      if (writeOp instanceof Write)
        tWriteOp.setWrite(wrap((Write) writeOp));
      else if (writeOp instanceof Delete)
        tWriteOp.setDelet(wrap((Delete) writeOp));
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
      TBatchOperationResult result = client.batch(tWrites);
      return new BatchOperationResult(result.isSuccess(), result.getMessage());
    } catch (TBatchOperationException e) {
      throw new BatchOperationException(e.getMessage(), e);
    } catch (TException e) {
      throw new BatchOperationException(e.getMessage(), e);
    }
  }

  @Override
  public DequeueResult execute(QueueDequeue dequeue) {
    try {
      return unwrap(client.dequeue(wrap(dequeue)));
    } catch (TException e) {
      String message = "Thrift Call for QueueDequeue failed for queue " +
          new String(dequeue.getKey()) + ": " + e.getMessage();
      Log.error(message);
      return new DequeueResult(DequeueResult.DequeueStatus.FAILURE, message);
    }
  }

  @Override
  public long execute(QueueAdmin.GetGroupID getGroupId) {
    try {
      return client.getGroupId(
          new TGetGroupId(wrap(getGroupId.getQueueName())));
    } catch (TException e) {
      Log.error("Thrift Call for GetGroupId failed for queue " +
          new String(getGroupId.getQueueName()) + ": " + e.getMessage());
      return 0; // TODO execute() must be able to return an error
    }
  }

  @Override
  public QueueAdmin.QueueMeta execute(QueueAdmin.GetQueueMeta getQueueMeta) {
    try {
      return unwrap(client.getQueueMeta(wrap(getQueueMeta)));
    } catch (TException e) {
      Log.error("Thrift Call for GetQueueMeta failed for queue " +
          new String(getQueueMeta.getQueueName()) + ": " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  public void execute(ClearFabric clearFabric) {
    try {
      client.clearFabric(wrap(clearFabric));
    } catch (TException e) {
      Log.error("Thrift Call for ClearFabric failed with message: " +
          e.getMessage());
      // TODO execute() must be able to return an error
    }
  }

  @Override
  public byte[] execute(ReadKey readKey) {
    try {
      return unwrap(client.readKey(new TReadKey(wrap(readKey.getKey()))));
    } catch (TException e) {
      Log.error("Thrift Call for ReadKey for key '" +
          new String(readKey.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  public Map<byte[], byte[]> execute(Read read) {
    try {
      return unwrap(client.read(wrap(read)));
    } catch (TException e) {
      Log.error("Thrift Call for Read for key '" +
          new String(read.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  public List<byte[]> execute(ReadAllKeys readKeys) {
    try {
      return unwrap(client.readAllKeys(wrap(readKeys)));
    } catch (TException e) {
      Log.error("Thrift Call for ReadAllKeys(" + readKeys.getOffset() + ", " +
          readKeys.getLimit() + ") failed with message: " + e.getMessage());
      return new ArrayList<byte[]>(0);
      // TODO execute() must be able to return an error
    }
  }

  @Override
  public Map<byte[], byte[]> execute(ReadColumnRange readColumnRange) {
    try {
      return unwrap(client.readColumnRange(wrap(readColumnRange)));
    } catch (TException e) {
      Log.error("Thrift Call for ReadKey for key '" +
          new String(readColumnRange.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(Write write) {
    try {
      return client.write(wrap(write));
    } catch (TException e) {
      Log.error("Thrift Call for Write for key '" + new String(write.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(Delete delete) {
    try {
      return client.delet(wrap(delete));
    } catch (TException e) {
      Log.error("Thrift Call for Delete for key '" + new String(delete.getKey())
          + "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(Increment increment) {
    try {
      return client.increment(wrap(increment));
    } catch (TException e) {
      Log.error("Thrift Call for Increment for key '" +
          new String(increment.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(CompareAndSwap compareAndSwap) {
    try {
      return client.compareAndSwap(wrap(compareAndSwap));
    } catch (TException e) {
      Log.error("Thrift Call for CompareAndSwap for key '" +
          new String(compareAndSwap.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(QueueEnqueue enqueue) {
    try {
      return client.queueEnqueue(wrap(enqueue));
    } catch (TException e) {
      Log.error("Thrift Call for QueueEnqueue for queue '" +
          new String(enqueue.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  public boolean execute(QueueAck ack) {
    try {
      return client.queueAck(wrap(ack));
    } catch (TException e) {
      Log.error("Thrift Call for QueueAck for queue '" +
          new String(ack.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }
}
