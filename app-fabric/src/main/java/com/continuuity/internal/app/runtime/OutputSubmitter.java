package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.TransactionAgent;

/**
 * A OutputSubmitter is something that could submits operations to {@link TransactionAgent}.
 */
public interface OutputSubmitter {

  void submit(TransactionAgent agent) throws OperationException;

}
