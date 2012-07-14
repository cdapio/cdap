package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.WriteOperation;
import com.continuuity.data.operation.executor.BatchOperationException;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.*;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The implementation of a thrift service for operation execution.
 * All operations arrive over the wire as Thrift objects. We must
 * convert ("unwrap") them into data fabric operations, execute using
 * the actual operations executor, and send the results back as
 * ("wrapped") Thrift objects.
 *
 * Why all this conversion (wrap/unwrap), and not define all Operations
 * themselves as Thrift objects?
 * <ul><li>
 *   All the non-thrift executors would have to use the Thrift objects
 * </li><li>
 *   Thrift's object model is too restrictive: it has only limited inheritance
 *   and no overloading
 * </li><li>
 *   Thrift objects are bare-bone, all they have are getters, setters, and
 *   basic object methods.
 * </li></ul>
 */
public class TOperationExecutorImpl
    extends ConverterUtils
    implements TOperationExecutor.Iface {

  private static final Logger Log =
      LoggerFactory.getLogger(TOperationExecutorImpl.class);

  /** the operation executor to use for all operations */
  private OperationExecutor opex;

  /** constructor requires the operation executor */
  public TOperationExecutorImpl(OperationExecutor opex) {
    this.opex = opex;
  }

  // Write operations. They all return a boolean, which is
  // always safe to return with Thrift.

  @Override
  public boolean write(TWrite tWrite) throws TException {
    return this.opex.execute(unwrap(tWrite));
  }

  @Override
  public boolean delet(TDelete tDelete) throws TException {
    return this.opex.execute(unwrap(tDelete));
  }

  @Override
  public boolean increment(TIncrement tIncrement) throws TException {
    return this.opex.execute(unwrap(tIncrement));
  }

  @Override
  public boolean compareAndSwap(TCompareAndSwap tCompareAndSwap)
      throws TException {
    return this.opex.execute(unwrap(tCompareAndSwap));
  }

  @Override
  public boolean queueEnqueue(TQueueEnqueue tQueueEnqueue) throws TException {
    return this.opex.execute(unwrap(tQueueEnqueue));
  }

  @Override
  public boolean queueAck(TQueueAck tQueueAck) throws TException {
    return this.opex.execute(unwrap(tQueueAck));
  }

  // batch write, return a structure and never null, and is thus safe

  @Override
  public TBatchOperationResult batch(List<TWriteOperation> batch)
      throws TBatchOperationException, TException {
    List<WriteOperation> writes = new ArrayList<WriteOperation>(batch.size());
    for (TWriteOperation tWriteOp : batch) {
      WriteOperation writeOp;
      if (tWriteOp.isSetWrite())
        writeOp = unwrap(tWriteOp.getWrite());
      else if (tWriteOp.isSetDelet())
        writeOp = unwrap(tWriteOp.getDelet());
      else if (tWriteOp.isSetIncrement())
        writeOp = unwrap(tWriteOp.getIncrement());
      else if (tWriteOp.isSetCompareAndSwap())
        writeOp = unwrap(tWriteOp.getCompareAndSwap());
      else if (tWriteOp.isSetQueueEnqueue())
        writeOp = new QueueEnqueue(tWriteOp.getQueueEnqueue().getQueueName(),
            tWriteOp.getQueueEnqueue().getValue());
      else if (tWriteOp.isSetQueueAck())
        writeOp = unwrap(tWriteOp.getQueueAck());
      else {
        Log.error("Internal Error: Unkown TWriteOperation "
            + tWriteOp.toString() + " in batch. Skipping.");
        continue;
      }
      writes.add(writeOp);
    }
    BatchOperationResult result;
    try {
      result = this.opex.execute(writes);
    } catch (BatchOperationException e) {
      throw new TBatchOperationException(e.getMessage());
    }
    return new TBatchOperationResult(result.isSuccess(), result.getMessage());
  }

  // read operations. they may return null from the executor.
  // Because Thrift methods cannot return null, we must wrap their
  // results into a structure

  @Override
  public TOptionalBinary readKey(TReadKey tReadKey) throws TException {
    return wrapBinary(this.opex.execute(unwrap(tReadKey)));
  }

  @Override
  public TOptionalBinaryMap read(TRead tRead) throws TException {
    return wrapMap(this.opex.execute(unwrap(tRead)));
  }

  @Override
  public TOptionalBinaryList readAllKeys(TReadAllKeys tReadAllKeys)
      throws TException {
    return wrapList(this.opex.execute(unwrap(tReadAllKeys)));
  }

  @Override
  public TOptionalBinaryMap
  readColumnRange(TReadColumnRange tReadColumnRange) throws TException {
    return wrapMap(this.opex.execute(unwrap(tReadColumnRange)));
  }

  // dequeue always return a structure, which does not need extra wrapping

  @Override
  public TDequeueResult dequeue(TQueueDequeue tQueueDequeue) throws TException {
    return wrap(this.opex.execute(unwrap(tQueueDequeue)));
  }

  // getGroupId always returns a long and cannot be null

  @Override
  public long getGroupId(TGetGroupId tGetGroupId) throws TException {
    return this.opex.execute(unwrap(tGetGroupId));
  }

  // getQueueMeta can return null, if the queue does not exist

  @Override
  public TQueueMeta getQueueMeta(TGetQueueMeta tGetQueueMeta)
      throws TException {
    return wrap(this.opex.execute(unwrap(tGetQueueMeta)));
  }

  // clearFabric is safe as it returns nothing

  @Override
  public void clearFabric(TClearFabric tClearFabric) throws TException {
    this.opex.execute(unwrap(tClearFabric));
  }
}
