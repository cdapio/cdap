package com.continuuity.fabric.operations.memory;


import java.util.List;
import java.util.Map;

import com.continuuity.fabric.engine.memory.MemorySimpleExecutor;
import com.continuuity.fabric.operations.OperationGenerator;
import com.continuuity.fabric.operations.SimpleOperationExecutor;
import com.continuuity.fabric.operations.SyncReadTimeoutException;
import com.continuuity.fabric.operations.WriteOperation;
import com.continuuity.fabric.operations.impl.CompareAndSwap;
import com.continuuity.fabric.operations.impl.Increment;
import com.continuuity.fabric.operations.impl.OrderedRead;
import com.continuuity.fabric.operations.impl.OrderedWrite;
import com.continuuity.fabric.operations.impl.Read;
import com.continuuity.fabric.operations.impl.ReadCounter;
import com.continuuity.fabric.operations.impl.ReadModifyWrite;
import com.continuuity.fabric.operations.impl.Write;
import com.continuuity.fabric.operations.queues.QueueAck;
import com.continuuity.fabric.operations.queues.QueueEntry;
import com.continuuity.fabric.operations.queues.QueuePop;
import com.continuuity.fabric.operations.queues.QueuePush;

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
    } else if (write instanceof QueuePush) {
      if (!execute((QueuePush)write)) return false;
    } else if (write instanceof Increment) {
      if (!execute((Increment)write)) return false;
    } else if (write instanceof CompareAndSwap) {
      if (!execute((CompareAndSwap)write)) return false;
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
  public QueueEntry execute(QueuePop pop) throws SyncReadTimeoutException {
    QueueEntry entry = this.executor.queuePop(pop.getQueueName(),
        pop.getConsumer(), pop.getPartitioner());
    pop.setResult(entry);
    return entry;
  }

  @Override
  public boolean execute(QueueAck ack) {
    // TODO Auto-generated method stub
    return false;
  }
}
