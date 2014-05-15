/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * Test the guice module overrides for the {@link TransactionServiceTwillRunnable}.
 */
public class TransactionServiceGuiceTest {

  @Test
  public void testGuiceInjector() {
    Injector injector = TransactionServiceTwillRunnable.createGuiceInjector(CConfiguration.create(),
                                                                            new Configuration());
    // get one tx manager
    InMemoryTransactionManager txManager1 = injector.getInstance(InMemoryTransactionManager.class);
    // get a second tx manager
    InMemoryTransactionManager txManager2 = injector.getInstance(InMemoryTransactionManager.class);
    // these should be two separate instances
    assertFalse(txManager1 == txManager2);
  }
}
