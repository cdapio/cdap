/*
 * Copyright © 2016-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.api.workflow.WorkflowActionNode;
import io.cdap.cdap.api.workflow.WorkflowNode;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.DefaultLineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.Lineage;
import io.cdap.cdap.data2.metadata.lineage.LineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.LineageTable;
import io.cdap.cdap.data2.metadata.lineage.Relation;
import io.cdap.cdap.data2.metadata.writer.BasicLineageWriter;
import io.cdap.cdap.data2.metadata.writer.LineageWriter;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.twill.api.RunId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests lineage computation.
 */
public class LineageAdminTest extends AppFabricTestBase {
  // Define data
  private final DatasetId dataset1 = new DatasetId("default", "dataset1");
  private final DatasetId dataset2 = new DatasetId("default", "dataset2");
  private final DatasetId dataset3 = new DatasetId("default", "dataset3");
  private final DatasetId dataset4 = new DatasetId("default", "dataset4");
  private final DatasetId dataset5 = new DatasetId("default", "dataset5");
  private final DatasetId dataset6 = new DatasetId("default", "dataset6");
  private final DatasetId dataset7 = new DatasetId("default", "dataset7");

  // Define programs and runs
  private final ProgramId program1 = new ProgramId("default", "app1", ProgramType.MAPREDUCE, "mr1");
  private final ProgramRunId run1 = program1.run(RunIds.generate(10000).getId());

  private final ProgramId program2 = new ProgramId("default", "app2", ProgramType.SPARK, "spark2");
  private final ProgramRunId run2 = program2.run(RunIds.generate(900).getId());

  private final ProgramId program3 = new ProgramId("default", "app3", ProgramType.WORKER, "worker3");
  private final ProgramRunId run3 = program3.run(RunIds.generate(800).getId());

  private final ProgramId program4 = new ProgramId("default", "app4", ProgramType.SERVICE, "service4");
  private final ProgramRunId run4 = program4.run(RunIds.generate(800).getId());

  private final ProgramId program5 = new ProgramId("default", "app5", ProgramType.SERVICE, "service5");
  private final ProgramRunId run5 = program5.run(RunIds.generate(700).getId());

  private final ProgramId program6 = new ProgramId("default", "app6", ProgramType.WORKFLOW, "workflow6");
  private int sourceId;

  @After
  public void cleanup() throws Exception {
    deleteNamespace(NamespaceId.DEFAULT.getNamespace());
    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    TransactionRunners.run(transactionRunner, context -> {
      LineageTable table = LineageTable.create(context);
      table.deleteAll();
    });
  }

  @Test
  public void testSimpleLineage() {
    // Lineage for D3 -> P2 -> D2 -> P1 -> D1
    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    LineageStoreReader lineageReader = new DefaultLineageStoreReader(transactionRunner);
    LineageWriter lineageWriter = new BasicLineageWriter(transactionRunner);

    Store store = getInjector().getInstance(Store.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageReader, store);

    // Add accesses for D3 -> P2 -> D2 -> P1 -> D1 <-> P3
    // We need to use current time here as metadata store stores access time using current time
    ProgramRunId run1 = program1.run(RunIds.generate(System.currentTimeMillis()).getId());
    ProgramRunId run2 = program2.run(RunIds.generate(System.currentTimeMillis()).getId());
    ProgramRunId run3 = program3.run(RunIds.generate(System.currentTimeMillis()).getId());

    addRuns(store, run1, run2, run3);
    // It is okay to use current time here since access time is ignore during assertions
    lineageWriter.addAccess(run1, dataset1, AccessType.UNKNOWN);
    lineageWriter.addAccess(run1, dataset1, AccessType.WRITE);
    lineageWriter.addAccess(run1, dataset2, AccessType.READ);

    lineageWriter.addAccess(run2, dataset2, AccessType.WRITE);
    lineageWriter.addAccess(run2, dataset3, AccessType.READ);

    lineageWriter.addAccess(run3, dataset1, AccessType.UNKNOWN, null);

    // The UNKNOWN access type will get filtered out if there is READ/WRITE. It will be preserved if it is the
    // only access type
    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1)),
        new Relation(dataset2, program2, AccessType.WRITE, twillRunId(run2)),
        new Relation(dataset3, program2, AccessType.READ, twillRunId(run2)),
        new Relation(dataset1, program3, AccessType.UNKNOWN, twillRunId(run3))
      )
    );

    // Lineage for D1
    Assert.assertEquals(expectedLineage,
                        lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000, 100));

    // Lineage for D2
    Assert.assertEquals(expectedLineage,
                        lineageAdmin.computeLineage(dataset2, 500, System.currentTimeMillis() + 10000, 100));

    // Lineage for D1 for one level should be D2 -> P1 -> D1 <-> P3
    Lineage oneLevelLineage = lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000, 1);

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1)),
        new Relation(dataset1, program3, AccessType.UNKNOWN, twillRunId(run3))
      ),
      oneLevelLineage.getRelations());

    // Assert that in a different namespace both lineage and metadata should be empty
    NamespaceId customNamespace = new NamespaceId("custom_namespace");
    DatasetId customDataset1 = customNamespace.dataset(dataset1.getEntityName());
    Assert.assertEquals(new Lineage(ImmutableSet.of()),
                        lineageAdmin.computeLineage(customDataset1, 500,
                                                    System.currentTimeMillis() + 10000, 100));
  }

  @Test
  public void testSimpleLoopLineage() {
    // Lineage for D1 -> P1 -> D2 -> P2 -> D3 -> P3 -> D4
    //             |                 |
    //             |                 V
    //             |<-----------------
    //

    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    LineageStoreReader lineageReader = new DefaultLineageStoreReader(transactionRunner);
    LineageWriter lineageWriter = new BasicLineageWriter(transactionRunner);

    Store store = getInjector().getInstance(Store.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageReader, store);

    // Add access
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageWriter.addAccess(run1, dataset1, AccessType.READ);
    lineageWriter.addAccess(run1, dataset2, AccessType.WRITE);

    lineageWriter.addAccess(run2, dataset2, AccessType.READ);
    lineageWriter.addAccess(run2, dataset1, AccessType.WRITE);
    lineageWriter.addAccess(run2, dataset3, AccessType.WRITE);

    lineageWriter.addAccess(run3, dataset3, AccessType.READ, null);
    lineageWriter.addAccess(run3, dataset4, AccessType.WRITE, null);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset2, program1, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1)),
        new Relation(dataset1, program2, AccessType.WRITE, twillRunId(run2)),
        new Relation(dataset2, program2, AccessType.READ, twillRunId(run2)),
        new Relation(dataset3, program2, AccessType.WRITE, twillRunId(run2)),
        new Relation(dataset4, program3, AccessType.WRITE, twillRunId(run3)),
        new Relation(dataset3, program3, AccessType.READ, twillRunId(run3))
      )
    );

    // Lineage for D1
    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset1, 500, 20000, 100));

    // Lineage for D2
    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset2, 500, 20000, 100));

    // Lineage for D1 for one level D1 -> P1 -> D2 -> P2 -> D3
    //                              |                 |
    //                              |                 V
    //                              |<-----------------
    //
    Lineage oneLevelLineage = lineageAdmin.computeLineage(dataset1, 500, 20000, 1);

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset2, program1, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1)),
        new Relation(dataset1, program2, AccessType.WRITE, twillRunId(run2)),
        new Relation(dataset2, program2, AccessType.READ, twillRunId(run2)),
        new Relation(dataset3, program2, AccessType.WRITE, twillRunId(run2))
      ),
      oneLevelLineage.getRelations());
  }

  @Test
  public void testDirectCycle() {
    // Lineage for:
    //
    // D1 <-> P1
    //
    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    LineageStoreReader lineageReader = new DefaultLineageStoreReader(transactionRunner);
    LineageWriter lineageWriter = new BasicLineageWriter(transactionRunner);

    Store store = getInjector().getInstance(Store.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageReader, store);

    // Add accesses
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageWriter.addAccess(run1, dataset1, AccessType.READ);
    lineageWriter.addAccess(run1, dataset1, AccessType.WRITE);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1))
        )
    );

    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset1, 500, 20000, 100));
  }

  @Test
  public void testDirectCycleTwoRuns() {
    // Lineage for:
    //
    // D1 -> P1 (run1)
    //
    // D1 <- P1 (run2)
    //
    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    LineageStoreReader lineageReader =
      new DefaultLineageStoreReader(transactionRunner);
    LineageWriter lineageWriter = new BasicLineageWriter(transactionRunner);

    Store store = getInjector().getInstance(Store.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageReader, store);

    // Add accesses
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageWriter.addAccess(run1, dataset1, AccessType.READ);
    // Write is in a different run
    lineageWriter.addAccess(new ProgramRunId(run1.getNamespace(), run1.getApplication(), run1.getParent().getType(),
                                            run1.getProgram(), run2.getEntityName()), dataset1, AccessType.WRITE);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1)),
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run2))
      )
    );

    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset1, 500, 20000, 100));
  }

  @Test
  public void testBranchLineage() {
    // Lineage for:
    //
    //       ->D4        -> D5 -> P3 -> D6
    //       |           |
    //       |           |
    // D1 -> P1 -> D2 -> P2 -> D3
    //       |     |           |
    //       |     |           |
    // S1 -->|     ---------------> P4 -> D7

    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    LineageStoreReader lineageReader =
      new DefaultLineageStoreReader(transactionRunner);
    LineageWriter lineageWriter = new BasicLineageWriter(transactionRunner);

    Store store = getInjector().getInstance(Store.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageReader, store);

    // Add accesses
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageWriter.addAccess(run1, dataset1, AccessType.READ);
    lineageWriter.addAccess(run1, dataset2, AccessType.WRITE);
    lineageWriter.addAccess(run1, dataset4, AccessType.WRITE);

    lineageWriter.addAccess(run2, dataset2, AccessType.READ);
    lineageWriter.addAccess(run2, dataset3, AccessType.WRITE);
    lineageWriter.addAccess(run2, dataset5, AccessType.WRITE);

    lineageWriter.addAccess(run3, dataset5, AccessType.READ, null);
    lineageWriter.addAccess(run3, dataset6, AccessType.WRITE, null);

    lineageWriter.addAccess(run4, dataset2, AccessType.READ, null);
    lineageWriter.addAccess(run4, dataset3, AccessType.READ, null);
    lineageWriter.addAccess(run4, dataset7, AccessType.WRITE, null);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1)),
        new Relation(dataset2, program1, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset4, program1, AccessType.WRITE, twillRunId(run1)),

        new Relation(dataset2, program2, AccessType.READ, twillRunId(run2)),
        new Relation(dataset3, program2, AccessType.WRITE, twillRunId(run2)),
        new Relation(dataset5, program2, AccessType.WRITE, twillRunId(run2)),

        new Relation(dataset5, program3, AccessType.READ, twillRunId(run3)),
        new Relation(dataset6, program3, AccessType.WRITE, twillRunId(run3)),

        new Relation(dataset2, program4, AccessType.READ, twillRunId(run4)),
        new Relation(dataset3, program4, AccessType.READ, twillRunId(run4)),
        new Relation(dataset7, program4, AccessType.WRITE, twillRunId(run4))
      )
    );

    // Lineage for D7
    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset7, 500, 20000, 100));
    // Lineage for D6
    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset6, 500, 20000, 100));
    // Lineage for D3
    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset3, 500, 20000, 100));
  }

  @Test
  public void testBranchLoopLineage() {
    // Lineage for:
    //
    //  |-------------------------------------|
    //  |                                     |
    //  |                                     |
    //  |    -> D4       -> D5 -> P3 -> D6 -> P5
    //  |    |           |                    ^
    //  V    |           |                    |
    // D1 -> P1 -> D2 -> P2 -> D3 ----------->|
    //       |     |           |
    //       |     |           |
    // S1 -->|     ---------------> P4 -> D7

    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    LineageStoreReader lineageReader =
      new DefaultLineageStoreReader(transactionRunner);
    LineageWriter lineageWriter = new BasicLineageWriter(transactionRunner);

    Store store = getInjector().getInstance(Store.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageReader, store);

    // Add accesses
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageWriter.addAccess(run1, dataset1, AccessType.READ);
    lineageWriter.addAccess(run1, dataset2, AccessType.WRITE);
    lineageWriter.addAccess(run1, dataset4, AccessType.WRITE);

    lineageWriter.addAccess(run2, dataset2, AccessType.READ);
    lineageWriter.addAccess(run2, dataset3, AccessType.WRITE);
    lineageWriter.addAccess(run2, dataset5, AccessType.WRITE);

    lineageWriter.addAccess(run3, dataset5, AccessType.READ, null);
    lineageWriter.addAccess(run3, dataset6, AccessType.WRITE, null);

    lineageWriter.addAccess(run4, dataset2, AccessType.READ, null);
    lineageWriter.addAccess(run4, dataset3, AccessType.READ, null);
    lineageWriter.addAccess(run4, dataset7, AccessType.WRITE, null);

    lineageWriter.addAccess(run5, dataset3, AccessType.READ, null);
    lineageWriter.addAccess(run5, dataset6, AccessType.READ, null);
    lineageWriter.addAccess(run5, dataset1, AccessType.WRITE, null);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1)),
        new Relation(dataset2, program1, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset4, program1, AccessType.WRITE, twillRunId(run1)),

        new Relation(dataset2, program2, AccessType.READ, twillRunId(run2)),
        new Relation(dataset3, program2, AccessType.WRITE, twillRunId(run2)),
        new Relation(dataset5, program2, AccessType.WRITE, twillRunId(run2)),

        new Relation(dataset5, program3, AccessType.READ, twillRunId(run3)),
        new Relation(dataset6, program3, AccessType.WRITE, twillRunId(run3)),

        new Relation(dataset2, program4, AccessType.READ, twillRunId(run4)),
        new Relation(dataset3, program4, AccessType.READ, twillRunId(run4)),
        new Relation(dataset7, program4, AccessType.WRITE, twillRunId(run4)),

        new Relation(dataset3, program5, AccessType.READ, twillRunId(run5)),
        new Relation(dataset6, program5, AccessType.READ, twillRunId(run5)),
        new Relation(dataset1, program5, AccessType.WRITE, twillRunId(run5))
      )
    );

    // Lineage for D1
    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset1, 500, 20000, 100));
    // Lineage for D5
    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset5, 500, 20000, 100));
    // Lineage for D7
    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset7, 500, 20000, 100));

    // Lineage for D5 for one level
    //                   -> D5 -> P3 -> D6
    //                   |
    //                   |
    //             D2 -> P2 -> D3
    Lineage oneLevelLineage = lineageAdmin.computeLineage(dataset5, 500, 20000, 1);

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset2, program2, AccessType.READ, twillRunId(run2)),
        new Relation(dataset3, program2, AccessType.WRITE, twillRunId(run2)),
        new Relation(dataset5, program2, AccessType.WRITE, twillRunId(run2)),

        new Relation(dataset5, program3, AccessType.READ, twillRunId(run3)),
        new Relation(dataset6, program3, AccessType.WRITE, twillRunId(run3))
      ),
      oneLevelLineage.getRelations()
    );
  }


  @Test
  public void testWorkflowLineage() {

    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    LineageStoreReader lineageReader =
      new DefaultLineageStoreReader(transactionRunner);
    LineageWriter lineageWriter = new BasicLineageWriter(transactionRunner);

    ApplicationId testApp = NamespaceId.DEFAULT.app("testApp");
    ProgramId workflowId = testApp.workflow("wf1");
    // if the spark and mr job are inner jobs of workflow, they should be in the same app
    ProgramId mrId = testApp.mr("mr1");
    ProgramId sparkId = testApp.mr("spark1");
    ImmutableList<WorkflowNode> nodes = ImmutableList.of(
      new WorkflowActionNode("mr1", new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, "mr1")),
      new WorkflowActionNode("spark1", new ScheduleProgramInfo(SchedulableProgramType.SPARK, "spark1")));
    WorkflowSpecification wfSpec =
      new WorkflowSpecification("test", "wf1", "", Collections.emptyMap(),
                                nodes,
                                Collections.emptyMap(), Collections.emptyMap());
    ApplicationSpecification appSpec =
      new DefaultApplicationSpecification("testApp", "dummy app", null,
                                          NamespaceId.DEFAULT.artifact("testArtifact",
                                                                       "1.0").toApiArtifactId(),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          ImmutableMap.of(workflowId.getProgram(), wfSpec),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          ProjectInfo.getVersion().toString());

    Store store = getInjector().getInstance(Store.class);
    store.addApplication(testApp, appSpec);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageReader, store);

    // Add accesses for D3 -> P2 -> D2 -> P1 -> D1 <-> P3
    //                                           |
    //                                           |-> P5,
    // P1 and P2 are inner programs of the workflow
    // We need to use current time here as metadata store stores access time using current time
    ProgramRunId run1 = mrId.run(RunIds.generate(System.currentTimeMillis()).getId());
    ProgramRunId run2 = sparkId.run(RunIds.generate(System.currentTimeMillis()).getId());
    ProgramRunId run3 = program3.run(RunIds.generate(System.currentTimeMillis()).getId());

    ProgramRunId workflow = workflowId.run(RunIds.generate(System.currentTimeMillis()).getId());

    ProgramRunId run5 = program5.run(RunIds.generate(System.currentTimeMillis()).getId());

    addRuns(store, workflow);
    // only mr and spark can be inner programs
    addWorkflowRuns(store, workflow.getProgram(), workflow.getRun(), run1, run2);
    addRuns(store, run3);
    addRuns(store, run5);

    // It is okay to use current time here since access time is ignore during assertions
    lineageWriter.addAccess(run1, dataset1, AccessType.WRITE);
    lineageWriter.addAccess(run1, dataset2, AccessType.READ);

    lineageWriter.addAccess(run2, dataset2, AccessType.WRITE);
    lineageWriter.addAccess(run2, dataset3, AccessType.READ);

    lineageWriter.addAccess(run3, dataset1, AccessType.UNKNOWN, null);

    lineageWriter.addAccess(run5, dataset1, AccessType.READ, null);


    // The UNKNOWN access type will get filtered out if there is READ/WRITE. It will be preserved if it is the
    // only access type
    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, workflowId, AccessType.WRITE, twillRunId(workflow)),
        new Relation(dataset2, workflowId, AccessType.READ, twillRunId(workflow)),
        new Relation(dataset2, workflowId, AccessType.WRITE, twillRunId(workflow)),
        new Relation(dataset3, workflowId, AccessType.READ, twillRunId(workflow)),
        new Relation(dataset1, program3, AccessType.UNKNOWN, twillRunId(run3)),
        new Relation(dataset1, program5, AccessType.READ, twillRunId(run5))
      )
    );

    Lineage resultLineage = lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000,
                                                        100, "workflow");
    // Lineage for D1
    Assert.assertEquals(expectedLineage, resultLineage);


    resultLineage = lineageAdmin.computeLineage(dataset2, 500, System.currentTimeMillis() + 10000,
                                                100, "workflow");
    // Lineage for D2
    Assert.assertEquals(expectedLineage, resultLineage);


    // Lineage for D1 for one level should be D2 -> P1 -> D1 <-> P3
    Lineage oneLevelLineage = lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000,
                                                          1, "workflow");

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, workflowId, AccessType.WRITE, twillRunId(workflow)),
        new Relation(dataset2, workflowId, AccessType.READ, twillRunId(workflow)),
        new Relation(dataset1, program5, AccessType.READ, twillRunId(run5)),
        new Relation(dataset1, program3, AccessType.UNKNOWN, twillRunId(run3))
      ),
      oneLevelLineage.getRelations());

    // Run tests without workflow parameter
    expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, mrId, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset2, mrId, AccessType.READ, twillRunId(run1)),
        new Relation(dataset2, sparkId, AccessType.WRITE, twillRunId(run2)),
        new Relation(dataset3, sparkId, AccessType.READ, twillRunId(run2)),
        new Relation(dataset1, program3, AccessType.UNKNOWN, twillRunId(run3)),
        new Relation(dataset1, program5, AccessType.READ, twillRunId(run5))
      )
    );

    resultLineage = lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000,
                                                        100, null);
    // Lineage for D1
    Assert.assertEquals(expectedLineage, resultLineage);

    resultLineage = lineageAdmin.computeLineage(dataset2, 500, System.currentTimeMillis() + 10000,
                                                100, null);
    // Lineage for D2
    Assert.assertEquals(expectedLineage, resultLineage);


    // Lineage for D1 for one level should be D2 -> P1 -> D1 <-> P3
    oneLevelLineage = lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000,
                                                          1, null);

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, mrId, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset2, mrId, AccessType.READ, twillRunId(run1)),
        new Relation(dataset1, program5, AccessType.READ, twillRunId(run5)),
        new Relation(dataset1, program3, AccessType.UNKNOWN, twillRunId(run3))
      ),
      oneLevelLineage.getRelations());
    
    // Assert that in a different namespace both lineage and metadata should be empty
    NamespaceId customNamespace = new NamespaceId("custom_namespace");
    DatasetId customDataset1 = customNamespace.dataset(dataset1.getEntityName());
    Assert.assertEquals(new Lineage(ImmutableSet.of()),
                        lineageAdmin.computeLineage(customDataset1, 500,
                                                    System.currentTimeMillis() + 10000, 100));
  }

  @Test
  public void testScanRange() {
    Set<RunId> runIds = ImmutableSet.of(
      RunIds.generate(500),
      RunIds.generate(400),
      RunIds.generate(600),
      RunIds.generate(200),
      RunIds.generate(700),
      RunIds.generate(100)
    );

    LineageAdmin.ScanRangeWithFilter scanRange = LineageAdmin.getScanRange(runIds);
    Assert.assertEquals(100, scanRange.getStart());
    Assert.assertEquals(701, scanRange.getEnd());

    scanRange = LineageAdmin.getScanRange(ImmutableSet.of());
    Assert.assertEquals(0, scanRange.getStart());
    Assert.assertEquals(0, scanRange.getEnd());

    scanRange = LineageAdmin.getScanRange(ImmutableSet.of(RunIds.generate(100)));
    Assert.assertEquals(100, scanRange.getStart());
    Assert.assertEquals(101, scanRange.getEnd());
  }

  @Test
  public void testLocalDatasetsInWorkflow() throws Exception {
    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    LineageStoreReader lineageReader =
      new DefaultLineageStoreReader(transactionRunner);
    LineageWriter lineageWriter = new BasicLineageWriter(transactionRunner);

    ApplicationId testApp = NamespaceId.DEFAULT.app("testLocalDatasets");
    ProgramId workflowId = testApp.workflow("wf1");
    // if the spark and mr job are inner jobs of workflow, they should be in the same app
    ProgramId mrId1 = testApp.mr("mr1");
    ProgramId mrId2 = testApp.mr("mr2");
    ProgramId sparkId = testApp.spark("spark1");
    ImmutableList<WorkflowNode> nodes = ImmutableList.of(
      new WorkflowActionNode("mr1", new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, "mr1")),
      new WorkflowActionNode("mr2", new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, "mr2")),
      new WorkflowActionNode("spark1", new ScheduleProgramInfo(SchedulableProgramType.SPARK, "spark1")));
    WorkflowSpecification wfSpec =
      new WorkflowSpecification("test", "wf1", "", Collections.emptyMap(),
                                nodes,
                                Collections.emptyMap(), Collections.emptyMap());
    ApplicationSpecification appSpec =
      new DefaultApplicationSpecification("testLocalDatasets", "dummy app", null,
                                          NamespaceId.DEFAULT.artifact("testArtifact",
                                                                       "1.0").toApiArtifactId(),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          ImmutableMap.of(workflowId.getProgram(), wfSpec),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          ProjectInfo.getVersion().toString());

    Store store = getInjector().getInstance(Store.class);
    store.addApplication(testApp, appSpec);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageReader, store);

    // Add accesses for D1 -|
    //                      |-> MR1 -> LOCAL1 -> MR2 -> LOCAL2 -> SPARK -> D3
    //                  D2 -|
    // P1 and P2 are inner programs of the workflow
    // We need to use current time here as metadata store stores access time using current time
    ProgramRunId mr1Run = mrId1.run(RunIds.generate(System.currentTimeMillis()).getId());
    ProgramRunId mr2Run = mrId2.run((RunIds.generate(System.currentTimeMillis()).getId()));
    ProgramRunId sparkRun = sparkId.run(RunIds.generate(System.currentTimeMillis()).getId());
    ProgramRunId workflow = workflowId.run(RunIds.generate(System.currentTimeMillis()).getId());

    // local datasets always end with workflow run id
    DatasetId localDataset1 = NamespaceId.DEFAULT.dataset("localDataset1" + workflow.getRun());
    DatasetId localDataset2 = NamespaceId.DEFAULT.dataset("localDataset2" + workflow.getRun());

    addRuns(store, workflow);
    // only mr and spark can be inner programs
    addWorkflowRuns(store, workflow.getProgram(), workflow.getRun(), mr1Run, mr2Run, sparkRun);

    lineageWriter.addAccess(mr1Run, dataset1, AccessType.READ);
    lineageWriter.addAccess(mr1Run, dataset2, AccessType.READ);
    lineageWriter.addAccess(mr1Run, localDataset1, AccessType.WRITE);

    lineageWriter.addAccess(mr2Run, localDataset1, AccessType.READ);
    lineageWriter.addAccess(mr2Run, localDataset2, AccessType.WRITE);

    lineageWriter.addAccess(sparkRun, localDataset2, AccessType.READ);
    lineageWriter.addAccess(sparkRun, dataset3, AccessType.WRITE);

    // compute the lineage without roll up, the local datasets and inner program should not roll up
    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, mrId1, AccessType.READ, twillRunId(mr1Run)),
        new Relation(dataset2, mrId1, AccessType.READ, twillRunId(mr1Run)),
        new Relation(localDataset1, mrId1, AccessType.WRITE, twillRunId(mr1Run)),
        new Relation(localDataset1, mrId2, AccessType.READ, twillRunId(mr2Run)),
        new Relation(localDataset2, mrId2, AccessType.WRITE, twillRunId(mr2Run)),
        new Relation(localDataset2, sparkId, AccessType.READ, twillRunId(sparkRun)),
        new Relation(dataset3, sparkId, AccessType.WRITE, twillRunId(sparkRun))));
    Lineage resultLineage = lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000,
                                                        100, null);
    // Lineage for D1
    Assert.assertEquals(expectedLineage, resultLineage);
    // D3 should have same lineage for all levels
    resultLineage = lineageAdmin.computeLineage(dataset3, 500, System.currentTimeMillis() + 10000,
                                                100, null);
    Assert.assertEquals(expectedLineage, resultLineage);

    // if only query for one level with no roll up, the roll up should not happen and the inner program and local
    // dataset should get returned
    expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset3, sparkId, AccessType.WRITE, twillRunId(sparkRun)),
        new Relation(localDataset2, sparkId, AccessType.READ, twillRunId(sparkRun))));
    resultLineage = lineageAdmin.computeLineage(dataset3, 500, System.currentTimeMillis() + 10000,
                                                1, null);
    Assert.assertEquals(expectedLineage, resultLineage);

    // query for roll up the workflow, all the inner program and local datasets should not be in the result,
    // the entire workflow information should get returned
    expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, workflowId, AccessType.READ, twillRunId(workflow)),
        new Relation(dataset2, workflowId, AccessType.READ, twillRunId(workflow)),
        new Relation(dataset3, workflowId, AccessType.WRITE, twillRunId(workflow))));
    // D1, D2, D3 should give same result
    resultLineage = lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000,
                                                1, "workflow");
    Assert.assertEquals(expectedLineage, resultLineage);
    resultLineage = lineageAdmin.computeLineage(dataset2, 500, System.currentTimeMillis() + 10000,
                                                1, "workflow");
    Assert.assertEquals(expectedLineage, resultLineage);
    resultLineage = lineageAdmin.computeLineage(dataset3, 500, System.currentTimeMillis() + 10000,
                                                1, "workflow");
    Assert.assertEquals(expectedLineage, resultLineage);
  }

  private void setStartAndRunning(Store store, ProgramId id, String pid, ArtifactId artifactId) {
    setStartAndRunning(store, id, pid, ImmutableMap.of(), ImmutableMap.of(), artifactId);
  }

  private void setStartAndRunning(Store store, ProgramId id, String pid, Map<String, String> runtimeArgs,
                                  Map<String, String> systemArgs, ArtifactId artifactId) {
    if (!systemArgs.containsKey(SystemArguments.PROFILE_NAME)) {
      systemArgs = ImmutableMap.<String, String>builder()
        .putAll(systemArgs)
        .put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName())
        .build();
    }
    long startTime = RunIds.getTime(pid, TimeUnit.SECONDS);
    store.setProvisioning(id.run(pid), runtimeArgs, systemArgs,
                          AppFabricTestHelper.createSourceId(++sourceId), artifactId);
    store.setProvisioned(id.run(pid), 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(id.run(pid), null, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
    store.setRunning(id.run(pid), startTime + 1, null, AppFabricTestHelper.createSourceId(++sourceId));
  }

  private void addRuns(Store store, ProgramRunId... runs) {
    for (ProgramRunId run : runs) {
      ArtifactId artifactId = run.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();
      setStartAndRunning(store, run.getParent(), run.getEntityName(), artifactId);
    }
  }

  /** Adds runs which have workflows associated with them
   *
   * @param store store instance
   * @param workflowName name of the workflow
   * @param workflowRunId run ID associated with all program runs
   * @param runs list of runs to be added
   */
  private void addWorkflowRuns(Store store, String workflowName, String workflowRunId, ProgramRunId... runs) {
    Map<String, String> workflowIDMap = new HashMap<>();
    Map<String, String> emptyMap = ImmutableMap.of();
    workflowIDMap.put(ProgramOptionConstants.WORKFLOW_NAME, workflowName);
    workflowIDMap.put(ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId);
    workflowIDMap.put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    for (ProgramRunId run : runs) {
      workflowIDMap.put(ProgramOptionConstants.WORKFLOW_NODE_ID, run.getProgram());
      ArtifactId artifactId = run.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();
      store.setProvisioning(run, emptyMap, workflowIDMap, AppFabricTestHelper.createSourceId(++sourceId), artifactId);
      store.setProvisioned(run, 0, AppFabricTestHelper.createSourceId(++sourceId));
      store.setStart(run, null, workflowIDMap, AppFabricTestHelper.createSourceId(++sourceId));
      store.setRunning(run, RunIds.getTime(run.getRun(), TimeUnit.SECONDS) + 1, null,
                       AppFabricTestHelper.createSourceId(++sourceId));
    }
  }

  private RunId twillRunId(ProgramRunId run) {
    return RunIds.fromString(run.getEntityName());
  }
}
