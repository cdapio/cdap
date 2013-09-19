/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.OmidTransactionException;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.TransactionResult;
import com.continuuity.data.operation.executor.omid.Undo;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
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

  private static DataSetAccessor accessor;

  protected MetricsTable getTable(String name) throws Exception {
    accessor.getDataSetManager(MetricsTable.class, DataSetAccessor.Namespace.SYSTEM).create(name);
    return accessor.getDataSetClient(name, MetricsTable.class, DataSetAccessor.Namespace.SYSTEM);
  }

  @Test
  public void testGetId() throws Exception {
    EntityTable entityTable = new EntityTable(getTable("testGetId"));

    // Make sure it is created sequentially
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // It should get the same value (from cache)
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // Construct another entityTable, it should load from storage.
    entityTable = new EntityTable(getTable("testGetId"));
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // ID for different type should have ID starts from 1 again.
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("flow", "flow" + i));
    }
  }

  @Test
  public void testGetName() throws Exception {
    EntityTable entityTable = new EntityTable(getTable("testGetName"));

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
                                             new DataFabricDistributedModule(HBaseTestBase.getConfiguration()),
                                             new LocationRuntimeModule().getDistributedModules());

    accessor = injector.getInstance(DataSetAccessor.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    HBaseTestBase.stopHBase();
  }

  private static final class MetricModule extends AbstractModule {

    @Override
    protected void configure() {
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
