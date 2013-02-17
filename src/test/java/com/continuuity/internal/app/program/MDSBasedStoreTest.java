/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.WordCountApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Status;
import com.continuuity.app.program.Store;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class MDSBasedStoreTest {
  private static MDSBasedStore store;

  @BeforeClass
  public static void beforeClass() {
    final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules());
    OperationExecutor executor = injector.getInstance(OperationExecutor.class);
    MetadataService.Iface metadataService = injector.getInstance(com.continuuity.metadata.MetadataService.class);
    MetaDataStore metaDataStore = new SerializingMetaDataStore(executor);
    store = new MDSBasedStore(metaDataStore, metadataService);
  }

  @Test
  public void testMetaDataServiceInjection() {
    final Injector injector =
      Guice.createInjector(
                            new AbstractModule() {
                              @Override
                              protected void configure() {
                                bind(OperationExecutor.class).to(NoOperationExecutor.class);
                                bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
                                bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
                              }
                            }
      );

    MDSBasedStore store = injector.getInstance(MDSBasedStore.class);
    Assert.assertNotNull(store.getMetaDataService());
  }

  @Test
  public void testLogProgramRunHistory() throws OperationException {
    // record finished flow
    Store.ProgramId programId = new Store.ProgramId("account1", "application1", "flow1");
    store.setStart(programId, "run1", 20);
    store.setEnd(programId, "run1", 29, Status.FAILED);

    // record another finished flow
    store.setStart(programId, "run2", 10);
    store.setEnd(programId, "run2", 19, Status.SUCCEEDED);

    // record not finished flow
    store.setStart(programId, "run3", 50);

    // record run of different program
    Store.ProgramId programId2 = new Store.ProgramId("account1", "application1", "flow2");
    store.setStart(programId2, "run4", 100);
    store.setEnd(programId2, "run4", 109, Status.SUCCEEDED);

    // we should probably be better with "get" method in MDSBasedStore interface to do that, but we don't have one
    List<RunRecord> history = store.getRunHistory(programId);

    // only finished runs should be returned
    Assert.assertEquals(2, history.size());
    // records should be sorted by start time
    RunRecord run = history.get(0);
    Assert.assertEquals(10, run.getStartTs());
    Assert.assertEquals(19, run.getStopTs());
    Assert.assertEquals(Status.SUCCEEDED, run.getEndStatus());

    run = history.get(1);
    Assert.assertEquals(20, run.getStartTs());
    Assert.assertEquals(29, run.getStopTs());
    Assert.assertEquals(Status.FAILED, run.getEndStatus());
  }

  @Test
  public void testAddGetApplication() throws OperationException {
    ApplicationSpecification spec = new WordCountApp().configure();
    Store.ApplicationId id = new Store.ApplicationId("account1", "application1");
    store.addApplication(id, spec);

    ApplicationSpecification stored = store.getApplication(id);

    // should be enough to make sure it is stored
    Assert.assertEquals(1, stored.getDataSets().size());
    Assert.assertEquals(spec.getFlows().get("WordCountFlow").getClassName(),
                        stored.getFlows().get("WordCountFlow").getClassName());
  }

}
