package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.table.ReadPointer;

/**
 *
 */
public abstract class TTQueueAbstractOnVCTable implements TTQueue {

  abstract QueueEntryPointer fetchNextEntryId();

  @Override
  public EnqueueResult enqueue(byte[] data, long writeVersion) throws OperationException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void invalidate(QueueEntryPointer entryPointer, long writeVersion) throws OperationException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer) throws OperationException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer) throws OperationException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void finalize(QueueEntryPointer entryPointer, QueueConsumer consumer, int totalNumGroups) throws
    OperationException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void unack(QueueEntryPointer entryPointer, QueueConsumer consumer) throws OperationException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public long getGroupID() throws OperationException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public QueueAdmin.QueueInfo getQueueInfo() throws OperationException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
