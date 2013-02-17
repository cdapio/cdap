/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.program.Status;
import com.continuuity.app.program.Store;
import com.continuuity.data.metadata.MetaDataEntry;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;

public class MDSBasedStoreTest {
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
  public void testLogProgramRun() throws OperationException {
    final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules());
    OperationExecutor executor = injector.getInstance(OperationExecutor.class);
    MetadataService.Iface metadataService = injector.getInstance(com.continuuity.metadata.MetadataService.class);
    MetaDataStore metaDataStore = new SerializingMetaDataStore(executor);
    MDSBasedStore store = new MDSBasedStore(metaDataStore, metadataService);
    long startTs = System.currentTimeMillis();
    store.logProgramStart("account1", "application1", "flow1", "run1", startTs);
    long endTs = startTs + 10;
    store.setEnd(new Store.ProgramId("account1", "application1", "flow1"), "run1", endTs, Status.FAILED);

    // we should probably be better with "get" method in MDSBasedStore interface to do that, but we don't have one
    MetaDataEntry logged = metaDataStore.get(new OperationContext("account1"), "account1", "application1",
                                             MDSBasedStore.ENTRY_TYPE_PROGRAM_RUN, "flow1");
    Assert.assertNotNull(logged);
    Assert.assertEquals(startTs, (long) Long.valueOf(logged.getTextField(MDSBasedStore.FIELD_PROGRAM_RUN_START_TS)));
    Assert.assertEquals(endTs, (long) Long.valueOf(logged.getTextField(MDSBasedStore.FIELD_PROGRAM_RUN_END_TS)));
    Assert.assertEquals(Status.FAILED,
                        Status.valueOf(logged.getTextField(MDSBasedStore.FIELD_PROGRAM_RUN_END_STATE)));
  }

}
