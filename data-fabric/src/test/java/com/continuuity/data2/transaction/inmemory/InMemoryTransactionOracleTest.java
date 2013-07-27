package com.continuuity.data2.transaction.inmemory;

import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TransactionSystemTest;
import org.junit.After;
import org.junit.Before;

/**
 *
 */
public class InMemoryTransactionOracleTest extends TransactionSystemTest {
  @Override
  protected TransactionSystemClient getClient() {
    return new InMemoryTxSystemClient();
  }

  @Before
  public void before() {
    InMemoryTransactionOracle.reset();
  }

  @After
  public void after() {
    InMemoryTransactionOracle.reset();
  }
}
