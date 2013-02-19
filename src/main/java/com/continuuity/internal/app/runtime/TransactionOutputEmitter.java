package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.data.operation.executor.TransactionAgent;

/**
 *
 */
public interface TransactionOutputEmitter<T> extends OutputEmitter<T> {

  void submit(TransactionAgent agent) throws OperationException;

  void reset();
}
