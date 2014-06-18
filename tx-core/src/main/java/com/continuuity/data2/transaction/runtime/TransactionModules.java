package com.continuuity.data2.transaction.runtime;

import com.google.inject.Module;

/**
 * Provides access to Google Guice modules for in-memory, single-node, and distributed operation.
 */
public class TransactionModules {
  public TransactionModules() {
  }

  public Module getInMemoryModules() {
    return new TransactionInMemoryModule();
  }

  public Module getSingleNodeModules() {
    return new TransactionLocalModule();
  }

  public Module getDistributedModules() {
    return new TransactionDistributedModule();
  }
}
