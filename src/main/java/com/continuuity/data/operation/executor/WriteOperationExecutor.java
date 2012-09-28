package com.continuuity.data.operation.executor;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;

public interface WriteOperationExecutor {

  // TODO document expected error codes

  /**
   * Performs a {@link Write} operation.
   * @param write the operation
   * @throws OperationException if execution failed
   */
  public void execute(OperationContext context,
                      Write write) throws OperationException;

  /**
   * Performs a {@link Delete} operation.
   * @param delete the operation
   * @throws OperationException if execution failed
   */
  public void execute(OperationContext context,
                      Delete delete) throws OperationException;

  /**
   * Performs an {@link Increment} operation.
   * @param inc the operation
   * @throws OperationException if execution failed
   */
  public void execute(OperationContext context,
                      Increment inc) throws OperationException;

  /**
   * Performs a {@link CompareAndSwap} operation.
   * @param cas the operation
   * @throws OperationException if execution failed
   */
  public void execute(OperationContext context,
                      CompareAndSwap cas) throws OperationException;
  
  /**
   * Performs a {@link QueueEnqueue} operation.
   * @param enqueue the operation
   * @throws OperationException if execution failed
   */
  public void execute(OperationContext context,
                      QueueEnqueue enqueue) throws OperationException;

  /**
   * Performs a {@link QueueAck} operation.
   * @param ack the operation
   * @throws OperationException if execution failed
   */
  public void execute(OperationContext context,
                      QueueAck ack) throws OperationException;
}
