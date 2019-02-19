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

package co.cask.cdap.metadata;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.DefaultLineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.LineageTable;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.data2.metadata.writer.BasicLineageWriter;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
    LineageWriter lineageWriter = new BasicLineageWriter(getDatasetFramework(), getTxClient(), transactionRunner);

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
    LineageWriter lineageWriter = new BasicLineageWriter(getDatasetFramework(), getTxClient(), transactionRunner);

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
        new Relation(dataset4, program3, AccessType.WRITE, twillRunId(run3), emptySet()),
        new Relation(dataset3, program3, AccessType.READ, twillRunId(run3), emptySet())
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
    LineageWriter lineageWriter = new BasicLineageWriter(getDatasetFramework(), getTxClient(), transactionRunner);

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
    LineageWriter lineageWriter = new BasicLineageWriter(getDatasetFramework(), getTxClient(), transactionRunner);

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
    LineageWriter lineageWriter = new BasicLineageWriter(getDatasetFramework(), getTxClient(), transactionRunner);

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

        new Relation(dataset5, program3, AccessType.READ, twillRunId(run3), emptySet()),
        new Relation(dataset6, program3, AccessType.WRITE, twillRunId(run3), emptySet()),

        new Relation(dataset2, program4, AccessType.READ, twillRunId(run4), emptySet()),
        new Relation(dataset3, program4, AccessType.READ, twillRunId(run4), emptySet()),
        new Relation(dataset7, program4, AccessType.WRITE, twillRunId(run4), emptySet())
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
    LineageWriter lineageWriter = new BasicLineageWriter(getDatasetFramework(), getTxClient(), transactionRunner);

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

        new Relation(dataset5, program3, AccessType.READ, twillRunId(run3), emptySet()),
        new Relation(dataset6, program3, AccessType.WRITE, twillRunId(run3), emptySet()),

        new Relation(dataset2, program4, AccessType.READ, twillRunId(run4), emptySet()),
        new Relation(dataset3, program4, AccessType.READ, twillRunId(run4), emptySet()),
        new Relation(dataset7, program4, AccessType.WRITE, twillRunId(run4), emptySet()),

        new Relation(dataset3, program5, AccessType.READ, twillRunId(run5), emptySet()),
        new Relation(dataset6, program5, AccessType.READ, twillRunId(run5), emptySet()),
        new Relation(dataset1, program5, AccessType.WRITE, twillRunId(run5), emptySet())
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

        new Relation(dataset5, program3, AccessType.READ, twillRunId(run3), emptySet()),
        new Relation(dataset6, program3, AccessType.WRITE, twillRunId(run3), emptySet())
      ),
      oneLevelLineage.getRelations()
    );
  }


  @Test
  public void testWorkflowLineage() {
    // Lineage for D3 -> P2 -> D2 -> P1 -> D1

    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    LineageStoreReader lineageReader =
      new DefaultLineageStoreReader(transactionRunner);
    LineageWriter lineageWriter = new BasicLineageWriter(getDatasetFramework(), getTxClient(), transactionRunner);

    Store store = getInjector().getInstance(Store.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageReader, store);

    // Add accesses for D3 -> P2 -> D2 -> P1 -> D1 <-> P3
    // We need to use current time here as metadata store stores access time using current time
    ProgramRunId run1 = program1.run(RunIds.generate(System.currentTimeMillis()).getId());
    ProgramRunId run2 = program2.run(RunIds.generate(System.currentTimeMillis()).getId());
    ProgramRunId run3 = program3.run(RunIds.generate(System.currentTimeMillis()).getId());

    ProgramRunId workflow = program6.run(RunIds.generate(System.currentTimeMillis()).getId());

    ProgramRunId run5 = program5.run(RunIds.generate(System.currentTimeMillis()).getId());

    addWorkflowRuns(store, workflow.getProgram(), workflow.getRun(), run1, run2, run3);
    addRuns(store, workflow);
    addRuns(store, run5);

    // It is okay to use current time here since access time is ignore during assertions
    lineageWriter.addAccess(run1, dataset1, AccessType.WRITE);
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
        new Relation(dataset1, program6, AccessType.WRITE, twillRunId(workflow)),
        new Relation(dataset2, program6, AccessType.READ, twillRunId(workflow)),
        new Relation(dataset2, program6, AccessType.WRITE, twillRunId(workflow)),
        new Relation(dataset3, program6, AccessType.READ, twillRunId(workflow)),
        new Relation(dataset1, program6, AccessType.UNKNOWN, twillRunId(workflow)),
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
        new Relation(dataset1, program6, AccessType.WRITE, twillRunId(workflow)),
        new Relation(dataset2, program6, AccessType.READ, twillRunId(workflow)),
        new Relation(dataset1, program5, AccessType.READ, twillRunId(run5)),
        new Relation(dataset1, program6, AccessType.UNKNOWN, twillRunId(workflow))
      ),
      oneLevelLineage.getRelations());

    // Run tests without workflow parameter
    expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1)),
        new Relation(dataset2, program2, AccessType.WRITE, twillRunId(run2)),
        new Relation(dataset3, program2, AccessType.READ, twillRunId(run2)),
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
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1)),
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
   * @param runs list ofo runs to be added
   */
  private void addWorkflowRuns(Store store, String workflowName, String workflowRunId, ProgramRunId... runs) {
    Map<String, String> workflowIDMap = new HashMap<>();
    Map<String, String> emptyMap = ImmutableMap.of();
    workflowIDMap.put(ProgramOptionConstants.WORKFLOW_NAME, workflowName);
    workflowIDMap.put(ProgramOptionConstants.WORKFLOW_NODE_ID, "workflowNodeId");
    workflowIDMap.put(ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId);
    workflowIDMap.put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    for (ProgramRunId run : runs) {
      ArtifactId artifactId = run.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();
      store.setProvisioning(run, emptyMap, workflowIDMap, AppFabricTestHelper.createSourceId(++sourceId), artifactId);
      store.setProvisioned(run, 0, AppFabricTestHelper.createSourceId(++sourceId));
      store.setStart(run, null, workflowIDMap, AppFabricTestHelper.createSourceId(++sourceId));
      store.setRunning(run, RunIds.getTime(run.getRun(), TimeUnit.SECONDS) + 1, null,
                       AppFabricTestHelper.createSourceId(++sourceId));
    }
  }

  private static Set<NamespacedEntityId> emptySet() {
    return Collections.emptySet();
  }

  private RunId twillRunId(ProgramRunId run) {
    return RunIds.fromString(run.getEntityName());
  }

  private DatasetFramework getDatasetFramework() {
    return getInjector().getInstance(DatasetFramework.class);
  }
}
