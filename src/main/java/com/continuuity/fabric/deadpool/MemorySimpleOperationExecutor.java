package com.continuuity.fabric.deadpool;


import java.util.List;
import java.util.Map;

import com.continuuity.data.SyncReadTimeoutException;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OperationGenerator;
import com.continuuity.data.operation.OrderedRead;
import com.continuuity.data.operation.OrderedWrite;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadCounter;
import com.continuuity.data.operation.ReadModifyWrite;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.simple.SimpleOperationExecutor;
import com.continuuity.data.operation.queue.QueueAck;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePop;
import com.continuuity.data.operation.queue.QueuePush;
import com.continuuity.data.operation.type.WriteOperation;

public class MemorySimpleOperationExecutor implements SimpleOperationExecutor {

  private final MemorySimpleExecutor executor;

  public MemorySimpleOperationExecutor(MemorySimpleExecutor executor) {
    this.executor = executor;
  }

  // Batch of writes

  @Override
  public boolean execute(List<WriteOperation> writes) {
    for (WriteOperation write : writes) {
      if(!execute(write)) return false;
    }
    return true;
  }

  private boolean execute(WriteOperation write) {
    if (write instanceof Write) {
      if (!execute((Write)write)) return false;
    } else if (write instanceof OrderedWrite) {
      if (!execute((OrderedWrite)write)) return false;
    } else if (write instanceof ReadModifyWrite) {
      if (!execute((ReadModifyWrite)write)) return false;
    } else if (write instanceof Increment) {
      if (!execute((Increment)write)) return false;
    } else if (write instanceof CompareAndSwap) {
      if (!execute((CompareAndSwap)write)) return false;
    } else if (write instanceof QueuePush) {
      if (!execute((QueuePush)write)) return false;
    } else if (write instanceof QueueAck) {
      if (!execute((QueueAck)write)) return false;
    }
    return true;
  }

  // Single write queries

  @Override
  public boolean execute(Write write) {
    this.executor.writeRandom(write.getKey(), write.getValue());
    return true;
  }

  @Override
  public boolean execute(OrderedWrite write) {
    this.executor.writeOrdered(write.getKey(), write.getValue());
    return true;
  }

  @Override
  public boolean execute(ReadModifyWrite rmw) {
    this.executor.readModifyWrite(rmw.getKey(), rmw.getModifier());
    return true;
  }

  @Override
  public boolean execute(Increment inc) {
    long result = this.executor.increment(inc.getKey(), inc.getAmount());
    inc.setResult(result);
    OperationGenerator<Long> generator =
        inc.getPostIncrementOperationGenerator();
    if (generator != null) {
      WriteOperation writeOperation = generator.generateWriteOperation(result);
      if (writeOperation != null) {
        return execute(writeOperation);
      }
    }
    return true;
  }

  @Override
  public boolean execute(CompareAndSwap cas) {
    return this.executor.compareAndSwap(cas.getKey(),
        cas.getExpectedValue(), cas.getNewValue());
  }

  // Single read queries

  @Override
  public byte [] execute(Read read) throws SyncReadTimeoutException {
    byte [] result = this.executor.readRandom(read.getKey());
    read.setResult(result);
    return result;
  }

  @Override
  public long execute(ReadCounter readCounter)
  throws SyncReadTimeoutException {
    long result = this.executor.readCounter(readCounter.getKey());
    readCounter.setResult(result);
    return result;
  }

  @Override
  public Map<byte[], byte[]> execute(OrderedRead orderedRead) throws SyncReadTimeoutException {
    Map<byte[], byte[]> result = null;
    if (orderedRead.getEndKey() == null) {
      if (orderedRead.getLimit() <= 1) {
        result = this.executor.readOrdered(orderedRead.getStartKey());
      } else {
        result = this.executor.readOrdered(orderedRead.getStartKey(),
            orderedRead.getLimit());
      }
    } else {
      result = this.executor.readOrdered(orderedRead.getStartKey(),
          orderedRead.getEndKey());
    }
    orderedRead.setResult(result);
    return result;
  }

  // Queues

  @Override
  public boolean execute(QueuePush push) {
    return this.executor.queuePush(push.getQueueName(), push.getValue());
  }

  @Override
  public boolean execute(QueueAck ack) {
    return this.executor.queueAck(ack.getQueueName(), ack.getQueueEntry());
  }

  @Override
  public QueueEntry execute(QueuePop pop)
      throws SyncReadTimeoutException, InterruptedException {
    QueueEntry entry = this.executor.queuePop(pop.getQueueName(),
        pop.getConsumer(), pop.getPartitioner());
    pop.setResult(entry);
    return entry;
  }
}
