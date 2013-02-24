package com.continuuity.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.ttqueue.QueueAck;

import java.nio.ByteBuffer;

/**
 *
 */
public interface InputDatum {

  void submitAck(TransactionAgent txAgent) throws OperationException;

  boolean needProcess();

  ByteBuffer getData();

  void incrementRetry();

  int getRetry();

  InputContext getInputContext();
}
