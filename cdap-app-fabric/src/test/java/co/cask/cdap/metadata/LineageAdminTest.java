/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageStore;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tephra.TransactionExecutorFactory;
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
  private final StreamId stream1 = new StreamId("default", "stream1");
  private final DatasetId dataset1 = new DatasetId("default", "dataset1");
  private final DatasetId dataset2 = new DatasetId("default", "dataset2");
  private final DatasetId dataset3 = new DatasetId("default", "dataset3");
  private final DatasetId dataset4 = new DatasetId("default", "dataset4");
  private final DatasetId dataset5 = new DatasetId("default", "dataset5");
  private final DatasetId dataset6 = new DatasetId("default", "dataset6");
  private final DatasetId dataset7 = new DatasetId("default", "dataset7");

  // Define programs and runs
  private final ProgramId program1 = new ProgramId("default", "app1", ProgramType.FLOW, "flow1");
  private final FlowletId flowlet1 = program1.flowlet("flowlet1");
  private final ProgramRunId run1 = program1.run(RunIds.generate(10000).getId());

  private final ProgramId program2 = new ProgramId("default", "app2", ProgramType.FLOW, "flow2");
  private final FlowletId flowlet2 = program2.flowlet("flowlet2");
  private final ProgramRunId run2 = program2.run(RunIds.generate(900).getId());

  private final ProgramId program3 = new ProgramId("default", "app3", ProgramType.WORKER, "worker3");
  private final ProgramRunId run3 = program3.run(RunIds.generate(800).getId());

  private final ProgramId program4 = new ProgramId("default", "app4", ProgramType.SERVICE, "service4");
  private final ProgramRunId run4 = program4.run(RunIds.generate(800).getId());

  private final ProgramId program5 = new ProgramId("default", "app5", ProgramType.SERVICE, "service5");
  private final ProgramRunId run5 = program5.run(RunIds.generate(700).getId());

  private final ProgramId program6 = new ProgramId("default", "app6", ProgramType.WORKFLOW, "workflow6");
  private final ProgramRunId run6 = program6.run(RunIds.generate(700).getId());

  @After
  public void cleanup() throws Exception {
    deleteNamespace(NamespaceId.DEFAULT.getNamespace());
  }

  @Test
  public void testSimpleLineage() throws Exception {
    // Lineage for D3 -> P2 -> D2 -> P1 -> D1

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), getDatasetFramework(),
                                                 NamespaceId.DEFAULT.dataset("testSimpleLineage"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityExistenceVerifier());

    // Define metadata
    MetadataRecord run1AppMeta = new MetadataRecord(program1.getParent(), MetadataScope.USER,
                                                    toMap("pk1", "pk1"), toSet("pt1"));
    MetadataRecord run1ProgramMeta = new MetadataRecord(program1, MetadataScope.USER,
                                                        toMap("pk1", "pk1"), toSet("pt1"));
    MetadataRecord run1Data1Meta = new MetadataRecord(dataset1, MetadataScope.USER,
                                                      toMap("dk1", "dk1"), toSet("dt1"));
    MetadataRecord run1Data2Meta = new MetadataRecord(dataset2, MetadataScope.USER,
                                                      toMap("dk2", "dk2"), toSet("dt2"));

    // Add metadata
    metadataStore.setProperties(MetadataScope.USER, program1.getParent(), run1AppMeta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, program1.getParent(), run1AppMeta.getTags().toArray(new String[0]));
    metadataStore.setProperties(MetadataScope.USER, program1, run1ProgramMeta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, program1, run1ProgramMeta.getTags().toArray(new String[0]));
    metadataStore.setProperties(MetadataScope.USER, dataset1, run1Data1Meta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, dataset1, run1Data1Meta.getTags().toArray(new String[0]));
    metadataStore.setProperties(MetadataScope.USER, dataset2, run1Data2Meta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, dataset2, run1Data2Meta.getTags().toArray(new String[0]));

    // Add accesses for D3 -> P2 -> D2 -> P1 -> D1 <-> P3
    // We need to use current time here as metadata store stores access time using current time
    ProgramRunId run1 = program1.run(RunIds.generate(System.currentTimeMillis()).getId());
    ProgramRunId run2 = program2.run(RunIds.generate(System.currentTimeMillis()).getId());
    ProgramRunId run3 = program3.run(RunIds.generate(System.currentTimeMillis()).getId());

    addRuns(store, run1, run2, run3);
    // It is okay to use current time here since access time is ignore during assertions
    lineageStore.addAccess(run1, dataset1, AccessType.UNKNOWN, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.READ, System.currentTimeMillis(), flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.WRITE, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.READ, System.currentTimeMillis(), flowlet2);

    lineageStore.addAccess(run3, dataset1, AccessType.UNKNOWN, System.currentTimeMillis());

    // The UNKNOWN access type will get filtered out if there is READ/WRITE. It will be preserved if it is the
    // only access type
    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.READ, twillRunId(run2), toSet(flowlet2)),
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
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program3, AccessType.UNKNOWN, twillRunId(run3))
      ),
      oneLevelLineage.getRelations());

    // Assert metadata
    Assert.assertEquals(toSet(run1AppMeta, run1ProgramMeta, run1Data1Meta, run1Data2Meta),
                        lineageAdmin.getMetadataForRun(run1));

    // Assert that in a different namespace both lineage and metadata should be empty
    NamespaceId customNamespace = new NamespaceId("custom_namespace");
    DatasetId customDataset1 = customNamespace.dataset(dataset1.getEntityName());
    ProgramRunId customRun1 = customNamespace.app(program1.getApplication()).program(program1.getType(),
                               program1.getEntityName()).run(run1.getEntityName());
    Assert.assertEquals(new Lineage(ImmutableSet.<Relation>of()),
                        lineageAdmin.computeLineage(customDataset1, 500,
                                                    System.currentTimeMillis() + 10000, 100));
    Assert.assertEquals(ImmutableSet.<MetadataRecord>of(), lineageAdmin.getMetadataForRun(customRun1));
  }

  @Test
  public void testSimpleLoopLineage() throws Exception {
    // Lineage for D1 -> P1 -> D2 -> P2 -> D3 -> P3 -> D4
    //             |                 |
    //             |                 V
    //             |<-----------------
    //

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), getDatasetFramework(),
                                                 NamespaceId.DEFAULT.dataset("testSimpleLoopLineage"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityExistenceVerifier());


    // Add access
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageStore.addAccess(run1, dataset1, AccessType.READ, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.WRITE, System.currentTimeMillis(), flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.READ, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset1, AccessType.WRITE, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.WRITE, System.currentTimeMillis(), flowlet2);

    lineageStore.addAccess(run3, dataset3, AccessType.READ, System.currentTimeMillis());
    lineageStore.addAccess(run3, dataset4, AccessType.WRITE, System.currentTimeMillis());

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset2, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset2, program2, AccessType.READ, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),
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
        new Relation(dataset2, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset2, program2, AccessType.READ, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2))
      ),
      oneLevelLineage.getRelations());
  }

  @Test
  public void testDirectCycle() throws Exception {
    // Lineage for:
    //
    // D1 <-> P1
    //
    LineageStore lineageStore = new LineageStore(getTxExecFactory(), getDatasetFramework(),
                                                 NamespaceId.DEFAULT.dataset("testDirectCycle"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityExistenceVerifier());

    // Add accesses
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageStore.addAccess(run1, dataset1, AccessType.READ, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, System.currentTimeMillis(), flowlet1);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1))
        )
    );

    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset1, 500, 20000, 100));
  }

  @Test
  public void testDirectCycleTwoRuns() throws Exception {
    // Lineage for:
    //
    // D1 -> P1 (run1)
    //
    // D1 <- P1 (run2)
    //
    LineageStore lineageStore = new LineageStore(getTxExecFactory(), getDatasetFramework(),
                                                 NamespaceId.DEFAULT.dataset("testDirectCycleTwoRuns"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityExistenceVerifier());

    // Add accesses
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageStore.addAccess(run1, dataset1, AccessType.READ, System.currentTimeMillis(), flowlet1);
    // Write is in a different run
    lineageStore.addAccess(new ProgramRunId(run1.getNamespace(), run1.getApplication(), run1.getParent().getType(),
                                            run1.getProgram(), run2.getEntityName()), dataset1, AccessType.WRITE,
                           System.currentTimeMillis(), flowlet1);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run2), toSet(flowlet1))
      )
    );

    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(dataset1, 500, 20000, 100));
  }

  @Test
  public void testBranchLineage() throws Exception {
    // Lineage for:
    //
    //       ->D4        -> D5 -> P3 -> D6
    //       |           |
    //       |           |
    // D1 -> P1 -> D2 -> P2 -> D3
    //       |     |           |
    //       |     |           |
    // S1 -->|     ---------------> P4 -> D7

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), getDatasetFramework(),
                                                 NamespaceId.DEFAULT.dataset("testBranchLineage"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityExistenceVerifier());

    // Add accesses
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageStore.addAccess(run1, stream1, AccessType.READ, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset1, AccessType.READ, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.WRITE, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset4, AccessType.WRITE, System.currentTimeMillis(), flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.READ, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.WRITE, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset5, AccessType.WRITE, System.currentTimeMillis(), flowlet2);

    lineageStore.addAccess(run3, dataset5, AccessType.READ, System.currentTimeMillis());
    lineageStore.addAccess(run3, dataset6, AccessType.WRITE, System.currentTimeMillis());

    lineageStore.addAccess(run4, dataset2, AccessType.READ, System.currentTimeMillis());
    lineageStore.addAccess(run4, dataset3, AccessType.READ, System.currentTimeMillis());
    lineageStore.addAccess(run4, dataset7, AccessType.WRITE, System.currentTimeMillis());

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(stream1, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset4, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),

        new Relation(dataset2, program2, AccessType.READ, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset5, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),

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
  public void testBranchLoopLineage() throws Exception {
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

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), getDatasetFramework(),
                                                 NamespaceId.DEFAULT.dataset("testBranchLoopLineage"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityExistenceVerifier());

    // Add accesses
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageStore.addAccess(run1, stream1, AccessType.READ, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset1, AccessType.READ, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.WRITE, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset4, AccessType.WRITE, System.currentTimeMillis(), flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.READ, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.WRITE, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset5, AccessType.WRITE, System.currentTimeMillis(), flowlet2);

    lineageStore.addAccess(run3, dataset5, AccessType.READ, System.currentTimeMillis());
    lineageStore.addAccess(run3, dataset6, AccessType.WRITE, System.currentTimeMillis());

    lineageStore.addAccess(run4, dataset2, AccessType.READ, System.currentTimeMillis());
    lineageStore.addAccess(run4, dataset3, AccessType.READ, System.currentTimeMillis());
    lineageStore.addAccess(run4, dataset7, AccessType.WRITE, System.currentTimeMillis());

    lineageStore.addAccess(run5, dataset3, AccessType.READ, System.currentTimeMillis());
    lineageStore.addAccess(run5, dataset6, AccessType.READ, System.currentTimeMillis());
    lineageStore.addAccess(run5, dataset1, AccessType.WRITE, System.currentTimeMillis());

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(stream1, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset4, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),

        new Relation(dataset2, program2, AccessType.READ, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset5, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),

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
    // Lineage for S1
    Assert.assertEquals(expectedLineage, lineageAdmin.computeLineage(stream1, 500, 20000, 100));

    // Lineage for D5 for one level
    //                   -> D5 -> P3 -> D6
    //                   |
    //                   |
    //             D2 -> P2 -> D3
    Lineage oneLevelLineage = lineageAdmin.computeLineage(dataset5, 500, 20000, 1);

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset2, program2, AccessType.READ, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset5, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),

        new Relation(dataset5, program3, AccessType.READ, twillRunId(run3), emptySet()),
        new Relation(dataset6, program3, AccessType.WRITE, twillRunId(run3), emptySet())
      ),
      oneLevelLineage.getRelations()
    );

    // Lineage for S1 for one level
    //
    //       -> D4
    //       |
    //       |
    // D1 -> P1 -> D2
    //       |
    //       |
    // S1 -->|

    oneLevelLineage = lineageAdmin.computeLineage(stream1, 500, 20000, 1);
    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(stream1, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset4, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1))
      ),
      oneLevelLineage.getRelations()
    );
  }


  @Test
  public void testWorkflowLineage() throws Exception {
    // Lineage for D3 -> P2 -> D2 -> P1 -> D1

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), getDatasetFramework(),
                                                 NamespaceId.DEFAULT.dataset("testWorkflowLineage"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityExistenceVerifier());

    // Define metadata
    MetadataRecord run1AppMeta = new MetadataRecord(program1.getParent(), MetadataScope.USER,
                                                    toMap("pk1", "pk1"), toSet("pt1"));
    MetadataRecord run1ProgramMeta = new MetadataRecord(program1, MetadataScope.USER,
                                                        toMap("pk1", "pk1"), toSet("pt1"));
    MetadataRecord run1Data1Meta = new MetadataRecord(dataset1, MetadataScope.USER,
                                                      toMap("dk1", "dk1"), toSet("dt1"));
    MetadataRecord run1Data2Meta = new MetadataRecord(dataset2, MetadataScope.USER,
                                                      toMap("dk2", "dk2"), toSet("dt2"));

    // Add metadata
    metadataStore.setProperties(MetadataScope.USER, program1.getParent(), run1AppMeta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, program1.getParent(), run1AppMeta.getTags().toArray(new String[0]));
    metadataStore.setProperties(MetadataScope.USER, program1, run1ProgramMeta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, program1, run1ProgramMeta.getTags().toArray(new String[0]));
    metadataStore.setProperties(MetadataScope.USER, dataset1, run1Data1Meta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, dataset1, run1Data1Meta.getTags().toArray(new String[0]));
    metadataStore.setProperties(MetadataScope.USER, dataset2, run1Data2Meta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, dataset2, run1Data2Meta.getTags().toArray(new String[0]));

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
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.READ, System.currentTimeMillis(), flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.WRITE, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.READ, System.currentTimeMillis(), flowlet2);

    lineageStore.addAccess(run3, dataset1, AccessType.UNKNOWN, System.currentTimeMillis());

    lineageStore.addAccess(run5, dataset1, AccessType.READ, System.currentTimeMillis());


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
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.READ, twillRunId(run2), toSet(flowlet2)),
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
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset1, program5, AccessType.READ, twillRunId(run5)),
        new Relation(dataset1, program3, AccessType.UNKNOWN, twillRunId(run3))
      ),
      oneLevelLineage.getRelations());
    
    // Assert metadata
    Assert.assertEquals(toSet(run1AppMeta, run1ProgramMeta, run1Data1Meta, run1Data2Meta),
                        lineageAdmin.getMetadataForRun(run1));

    // Assert that in a different namespace both lineage and metadata should be empty
    NamespaceId customNamespace = new NamespaceId("custom_namespace");
    DatasetId customDataset1 = customNamespace.dataset(dataset1.getEntityName());
    ProgramRunId customRun1 = customNamespace.app(
      program1.getApplication()).program(program1.getType(),
                                         program1.getEntityName()).run(run1.getEntityName()
    );
    Assert.assertEquals(new Lineage(ImmutableSet.<Relation>of()),
                        lineageAdmin.computeLineage(customDataset1, 500,
                                                    System.currentTimeMillis() + 10000, 100));
    Assert.assertEquals(ImmutableSet.<MetadataRecord>of(), lineageAdmin.getMetadataForRun(customRun1));
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

    scanRange = LineageAdmin.getScanRange(ImmutableSet.<RunId>of());
    Assert.assertEquals(0, scanRange.getStart());
    Assert.assertEquals(0, scanRange.getEnd());

    scanRange = LineageAdmin.getScanRange(ImmutableSet.of(RunIds.generate(100)));
    Assert.assertEquals(100, scanRange.getStart());
    Assert.assertEquals(101, scanRange.getEnd());
  }

  private void addRuns(Store store, ProgramRunId... runs) {
    for (ProgramRunId run : runs) {
      store.setStart(run.getParent(), run.getEntityName(), RunIds.getTime(
        RunIds.fromString(run.getEntityName()), TimeUnit.SECONDS));
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
    for (ProgramRunId run : runs) {
      store.setStart(run.getParent(), run.getEntityName(), RunIds.getTime(
        RunIds.fromString(run.getEntityName()), TimeUnit.SECONDS), null,
                     emptyMap, workflowIDMap);
    }
  }

  @SafeVarargs
  private static <T> Set<T> toSet(T... elements) {
    return ImmutableSet.copyOf(elements);
  }

  private Map<String, String> toMap(String key, String value) {
    return ImmutableMap.of(key, value);
  }

  private static Set<NamespacedEntityId> emptySet() {
    return Collections.emptySet();
  }

  private RunId twillRunId(ProgramRunId run) {
    return RunIds.fromString(run.getEntityName());
  }

  private TransactionExecutorFactory getTxExecFactory() {
    return getInjector().getInstance(TransactionExecutorFactory.class);
  }

  private DatasetFramework getDatasetFramework() {
    return getInjector().getInstance(DatasetFramework.class);
  }

  private static final class NoOpEntityExistenceVerifier implements EntityExistenceVerifier {
    @Override
    public void ensureExists(EntityId entityId) throws NotFoundException {
      // no-op
    }
  }
}
