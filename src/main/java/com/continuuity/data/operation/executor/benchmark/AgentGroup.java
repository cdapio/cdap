package com.continuuity.data.operation.executor.benchmark;

import com.continuuity.data.operation.executor.OperationExecutor;

public abstract class AgentGroup {

    public int getNumInstances() {
      return 1; // be default run 1 instance
    }

    public abstract String getName();
    public abstract Runnable getAgent(OperationExecutor opex, int instanceInGroup);
}
