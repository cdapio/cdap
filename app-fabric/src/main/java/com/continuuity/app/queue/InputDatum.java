package com.continuuity.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.data.operation.executor.TransactionAgent;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 *
 */
public interface InputDatum {

  void submitAck(TransactionAgent txAgent) throws OperationException;

  boolean needProcess();

  Iterator<ByteBuffer> getData();

  void incrementRetry();

  int getRetry();

  InputContext getInputContext();
}
