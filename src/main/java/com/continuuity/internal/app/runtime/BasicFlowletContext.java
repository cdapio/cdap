package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.FlowletContext;

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class BasicFlowletContext implements FlowletContext {

  private final String name;
  private final AtomicInteger instanceCount;
  private final boolean asyncMode;

  public BasicFlowletContext(String name, int instanceCount) {
    this(name, instanceCount, false);
  }

  public BasicFlowletContext(String name, int instanceCount, boolean asyncMode) {
    this.name = name;
    this.instanceCount = new AtomicInteger(instanceCount);
    this.asyncMode = asyncMode;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount.get();
  }

  @Override
  public String getName() {
    return name;
  }

  public void setInstanceCount(int count) {
    instanceCount.set(count);
  }

  public boolean isAsyncMode() {
    return asyncMode;
  }
}
