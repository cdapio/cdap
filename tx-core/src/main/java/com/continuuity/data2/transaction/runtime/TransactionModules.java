package com.continuuity.data2.transaction.runtime;

import com.google.inject.Module;

/**
 *
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
