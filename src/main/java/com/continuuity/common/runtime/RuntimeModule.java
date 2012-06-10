package com.continuuity.common.runtime;

import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 *
 */
public abstract class RuntimeModule {
  public abstract Module getInMemory();
  public abstract Module getSingleNode();
  public abstract Module getDistributed();
}
