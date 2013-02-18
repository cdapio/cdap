package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A {@link OutputSubmitter} that would delegate the {@link #submit(TransactionAgent)}
 * call to all the {@link OutputSubmitter} that it contains one by one.
 */
public final class MultiOutputSubmitter implements OutputSubmitter {

  private final List<OutputSubmitter> outputSubmitters;

  public MultiOutputSubmitter(Iterable<OutputSubmitter> outputSubmitters) {
    this.outputSubmitters = ImmutableList.copyOf(outputSubmitters);
  }

  @Override
  public void submit(TransactionAgent agent) throws OperationException {
    for (OutputSubmitter submitter : outputSubmitters) {
      submitter.submit(agent);
    }
  }
}
