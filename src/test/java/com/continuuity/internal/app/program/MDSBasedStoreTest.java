/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.WordCountApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Status;
import com.continuuity.app.program.Store;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Application;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.Flow;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.metadata.thrift.Query;
import com.continuuity.metadata.thrift.Stream;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class MDSBasedStoreTest {
  private static MDSBasedStore store;
  private static MetadataService.Iface metadataService;

  @BeforeClass
  public static void beforeClass() {
    final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                                   new StoreModule4Test());

    metadataService = injector.getInstance(MetadataService.Iface.class);
    store = injector.getInstance(MDSBasedStore.class);
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
    Id.Program programId = Id.Program.from("account1", "application1", "flow1");
    store.setStart(programId, "run1", 20);
    store.setEnd(programId, "run1", 29, Status.FAILED);

    // record another finished flow
    store.setStart(programId, "run2", 10);
    store.setEnd(programId, "run2", 19, Status.SUCCEEDED);

    // record not finished flow
    store.setStart(programId, "run3", 50);

    // record run of different program
    Id.Program programId2 = Id.Program.from("account1", "application1", "flow2");
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
  public void testAddGetApplication() throws Exception {
    ApplicationSpecification spec = new WordCountApp().configure();
    Id.Application id = new Id.Application(new Id.Account("account1"), "application1");
    store.addApplication(id, spec);

    ApplicationSpecification stored = store.getApplication(id);

    // should be enough to make sure it is stored
    Assert.assertEquals(1, stored.getDataSets().size());
    Assert.assertEquals(spec.getFlows().get("WordCountFlow").getClassName(),
                        stored.getFlows().get("WordCountFlow").getClassName());

    // Checking that resources were registered in metadataService (UI still uses this)
    // app
    Application app = metadataService.getApplication(new Account("account1"), new Application("application1"));
    Assert.assertNotNull(app);
    Assert.assertEquals("WordCountApp", app.getName());

    // flow
    Assert.assertEquals(1, metadataService.getFlows("account1").size());
    Flow flow = metadataService.getFlow("account1", "application1", "WordCountFlow");
    Assert.assertNotNull(flow);
    Assert.assertEquals(1, flow.getDatasets().size());
    Assert.assertEquals(1, flow.getStreams().size());
    Assert.assertEquals("WordCountFlow", flow.getName());

    // procedure
    Assert.assertEquals(1, metadataService.getQueries(new Account("account1")).size());
    Query query = metadataService.getQuery(new Account("account1"), new Query("WordFrequency", "application1"));
    Assert.assertNotNull(query);
    // TODO: uncomment when datasets are added to procedureSpec
//    Assert.assertEquals(1, query.getDatasets().size());
    Assert.assertEquals("WordFrequency", query.getName());

    // streams
    Assert.assertEquals(1, metadataService.getStreams(new Account("account1")).size());
    Stream stream = metadataService.getStream(new Account("account1"), new Stream("text"));
    Assert.assertNotNull(stream);
    Assert.assertEquals("text", stream.getName());

    // datasets
    Assert.assertEquals(1, metadataService.getDatasets(new Account("account1")).size());
    Dataset dataset = metadataService.getDataset(new Account("account1"), new Dataset("mydataset"));
    Assert.assertNotNull(dataset);
    Assert.assertEquals("mydataset", dataset.getName());
    Assert.assertEquals(KeyValueTable.class.getName(), dataset.getType());
  }

}
