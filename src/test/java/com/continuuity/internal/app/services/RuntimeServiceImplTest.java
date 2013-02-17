/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.WordCountApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.Status;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.FlowRunRecord;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.program.MDSBasedStore;
import com.continuuity.internal.app.program.StoreModule4Test;
import com.continuuity.internal.app.services.legacy.ConnectionDefinition;
import com.continuuity.internal.app.services.legacy.FlowDefinitionImpl;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

public class RuntimeServiceImplTest {
  private static MDSBasedStore store;
  private static RuntimeServiceImpl runtimeService;

  @BeforeClass
  public static void beforeClass() {
    final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                                   new StoreModule4Test(), new ServicesModule4Test());

    store = injector.getInstance(MDSBasedStore.class);
    runtimeService = injector.getInstance(RuntimeServiceImpl.class);
  }

  // TODO: remove @Ignore when srcType is added to Queues
  @Test
  @Ignore
  public void testGetFlowDefinition() throws Exception {
    ApplicationSpecification spec = new WordCountApp().configure();
    Id.Application appId = new Id.Application(new Id.Account("account1"), "application1");
    store.addApplication(appId, spec);

    FlowIdentifier flowId = new FlowIdentifier("account1", "application1", "WordCountFlow", 0);
    String flowDefJson = runtimeService.getFlowDefinition(flowId);
    FlowDefinitionImpl flowDef = new Gson().fromJson(flowDefJson, FlowDefinitionImpl.class);

    Assert.assertEquals(3, flowDef.getFlowlets().size());
    Assert.assertEquals(1, flowDef.getFlowStreams().size());

    // checking connections (most important stuff)
    Assert.assertEquals(3, flowDef.getConnections().size());
    int[] connectionFound = new int[3];
    for (ConnectionDefinition conn : flowDef.getConnections()) {
      if (conn.getFrom().isFlowStream()) {
        connectionFound[0] = 1;
        Assert.assertEquals("text", conn.getFrom().getStream());
      } else {
        if ("Tokenizer".equals(conn.getFrom().getFlowlet())) {
          connectionFound[1] = 1;
          Assert.assertEquals("CountByField", conn.getTo().getFlowlet());
        } else if ("StreamSucker".equals(conn.getFrom().getFlowlet())) {
          connectionFound[2] = 1;
          Assert.assertEquals("Tokenizer", conn.getTo().getFlowlet());
        }
      }
    }

    Assert.assertArrayEquals(new int[]{1, 1, 1}, connectionFound);
  }

  @Test
  public void testGetFlowHistory() throws Exception {
    // record finished flow
    Id.Program programId = new Id.Program(new Id.Application(new Id.Account("account1"), "application1"), "flow1");
    store.setStart(programId, "run1", 20);
    store.setEnd(programId, "run1", 29, Status.FAILED);

    FlowIdentifier flowId = new FlowIdentifier("account1", "application1", "flow1", 0);
    List<FlowRunRecord> history = runtimeService.getFlowHistory(flowId);
    Assert.assertEquals(1, history.size());
    FlowRunRecord record = history.get(0);
    Assert.assertEquals(20, record.getStartTime());
    Assert.assertEquals(29, record.getEndTime());
    Assert.assertEquals(Status.FAILED, Status.valueOf(record.getEndStatus()));
  }
}