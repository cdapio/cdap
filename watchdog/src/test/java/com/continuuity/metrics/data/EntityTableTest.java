/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.OmidTransactionException;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.TransactionResult;
import com.continuuity.data.operation.executor.omid.Undo;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.test.hbase.HBaseTestBase;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class EntityTableTest {

  private static OVCTableHandle tableHandle;

  @Test
  public void testGetId() throws OperationException {
    EntityTable entityTable = new EntityTable(tableHandle.getTable(Bytes.toBytes("testGetId")));

    // Make sure it is created sequentially
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // It should get the same value (from cache)
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // Construct another entityTable, it should load from storage.
    entityTable = new EntityTable(tableHandle.getTable(Bytes.toBytes("testGetId")));
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // ID for different type should have ID starts from 1 again.
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("flow", "flow" + i));
    }
  }

  @Test
  public void testGetName() throws OperationException {
    EntityTable entityTable = new EntityTable(tableHandle.getTable(Bytes.toBytes("testGetName")));

    // Create some entities.
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // Reverse lookup
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals("app" + i, entityTable.getName(i, "app"));
    }
  }


  @BeforeClass
  public static void init() throws Exception {
    HBaseTestBase.startHBase();
    Injector injector = Guice.createInjector(new ConfigModule(CConfiguration.create(),
                                                              HBaseTestBase.getConfiguration()),
                                             new MetricModule());

    tableHandle = injector.getInstance(OVCTableHandle.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    HBaseTestBase.stopHBase();
  }

  private static final class MetricModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(OVCTableHandle.class).to(HBaseFilterableOVCTableHandle.class);
      bind(TransactionOracle.class).to(NoopTransactionOracle.class).in(Scopes.SINGLETON);
    }
  }

  private static final class NoopTransactionOracle implements TransactionOracle {

    @Override
    public Transaction startTransaction(boolean trackChanges) {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void validateTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void addToTransaction(Transaction tx, List<Undo> undos) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public TransactionResult commitTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public TransactionResult abortTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void removeTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public ReadPointer getReadPointer() {
      throw new UnsupportedOperationException("Not supported");
    }
  }
}
