/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageStore;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests lineage computation.
 */
public class LineageAdminTest extends MetadataTestBase {
  // Define data
  private final Id.Stream stream1 = Id.Stream.from("default", "stream1");
  private final Id.DatasetInstance dataset1 = Id.DatasetInstance.from("default", "dataset1");
  private final Id.DatasetInstance dataset2 = Id.DatasetInstance.from("default", "dataset2");
  private final Id.DatasetInstance dataset3 = Id.DatasetInstance.from("default", "dataset3");
  private final Id.DatasetInstance dataset4 = Id.DatasetInstance.from("default", "dataset4");
  private final Id.DatasetInstance dataset5 = Id.DatasetInstance.from("default", "dataset5");
  private final Id.DatasetInstance dataset6 = Id.DatasetInstance.from("default", "dataset6");
  private final Id.DatasetInstance dataset7 = Id.DatasetInstance.from("default", "dataset7");

  // Define programs and runs
  private final Id.Program program1 = Id.Program.from("default", "app1", ProgramType.FLOW, "flow1");
  private final Id.Flow.Flowlet flowlet1 =
    Id.Flow.Flowlet.from(program1.getApplication(), program1.getId(), "flowlet1");
  private final Id.Run run1 = new Id.Run(program1, RunIds.generate(10000).getId());

  private final Id.Program program2 = Id.Program.from("default", "app2", ProgramType.FLOW, "flow2");
  private final Id.Flow.Flowlet flowlet2 =
    Id.Flow.Flowlet.from(program2.getApplication(), program2.getId(), "flowlet2");
  private final Id.Run run2 = new Id.Run(program2, RunIds.generate(900).getId());

  private final Id.Program program3 = Id.Worker.from("default", "app3", ProgramType.WORKER, "worker3");
  private final Id.Run run3 = new Id.Run(program3, RunIds.generate(800).getId());

  private final Id.Program program4 = Id.Program.from("default", "app4", ProgramType.SERVICE, "service4");
  private final Id.Run run4 = new Id.Run(program4, RunIds.generate(800).getId());

  private final Id.Program program5 = Id.Program.from("default", "app5", ProgramType.SERVICE, "service5");
  private final Id.Run run5 = new Id.Run(program5, RunIds.generate(700).getId());

  @After
  public void cleanup() throws Exception {
    deleteNamespace(NamespaceId.DEFAULT.getNamespace());
  }

  @Test
  public void testSimpleLineage() throws Exception {
    // Lineage for D3 -> P2 -> D2 -> P1 -> D1

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), getDatasetFramework(),
                                                 Id.DatasetInstance.from("default", "testSimpleLineage"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityValidator());

    // Define metadata
    MetadataRecord run1AppMeta = new MetadataRecord(program1.getApplication(), MetadataScope.USER,
                                                    toMap("pk1", "pk1"), toSet("pt1"));
    MetadataRecord run1ProgramMeta = new MetadataRecord(program1, MetadataScope.USER,
                                                        toMap("pk1", "pk1"), toSet("pt1"));
    MetadataRecord run1Data1Meta = new MetadataRecord(dataset1, MetadataScope.USER,
                                                      toMap("dk1", "dk1"), toSet("dt1"));
    MetadataRecord run1Data2Meta = new MetadataRecord(dataset2, MetadataScope.USER,
                                                      toMap("dk2", "dk2"), toSet("dt2"));

    // Add metadata
    metadataStore.setProperties(MetadataScope.USER, program1.getApplication(), run1AppMeta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, program1.getApplication(), run1AppMeta.getTags().toArray(new String[0]));
    metadataStore.setProperties(MetadataScope.USER, program1, run1ProgramMeta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, program1, run1ProgramMeta.getTags().toArray(new String[0]));
    metadataStore.setProperties(MetadataScope.USER, dataset1, run1Data1Meta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, dataset1, run1Data1Meta.getTags().toArray(new String[0]));
    metadataStore.setProperties(MetadataScope.USER, dataset2, run1Data2Meta.getProperties());
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    metadataStore.addTags(MetadataScope.USER, dataset2, run1Data2Meta.getTags().toArray(new String[0]));
    TimeUnit.MILLISECONDS.sleep(1);

    // Add accesses for D3 -> P2 -> D2 -> P1 -> D1
    // We need to use current time here as metadata store stores access time using current time
    Id.Run run1 = new Id.Run(program1, RunIds.generate(System.currentTimeMillis()).getId());
    Id.Run run2 = new Id.Run(program2, RunIds.generate(System.currentTimeMillis()).getId());
    addRuns(store, run1, run2);
    // It is okay to use current time here since access time is ignore during assertions
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.READ, System.currentTimeMillis(), flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.WRITE, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.READ, System.currentTimeMillis(), flowlet2);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.READ, twillRunId(run2), toSet(flowlet2))
      )
    );

    // Lineage for D1
    Assert.assertEquals(expectedLineage,
                        lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000, 100));

    // Lineage for D2
    Assert.assertEquals(expectedLineage,
                        lineageAdmin.computeLineage(dataset2, 500, System.currentTimeMillis() + 10000, 100));

    // Lineage for D1 for one level should be D2 -> P1 -> D1
    Lineage oneLevelLineage = lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000, 1);

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1))
      ),
      oneLevelLineage.getRelations());

    // Assert metadata
    Assert.assertEquals(toSet(run1AppMeta, run1ProgramMeta, run1Data1Meta, run1Data2Meta),
                        lineageAdmin.getMetadataForRun(run1));

    // Assert that in a different namespace both lineage and metadata should be empty
    Id.Namespace customNamespace = Id.Namespace.from("custom_namespace");
    Id.DatasetInstance customDataset1 = Id.DatasetInstance.from(customNamespace, dataset1.getId());
    Id.Run customRun1 =
      new Id.Run(Id.Program.from(customNamespace, program1.getApplicationId(), program1.getType(), program1.getId()),
                 run1.getId());
    Assert.assertEquals(new Lineage(ImmutableSet.<Relation>of()),
                        lineageAdmin.computeLineage(customDataset1, 500, System.currentTimeMillis() + 10000, 100));
    Assert.assertEquals(ImmutableSet.<MetadataRecord>of(), lineageAdmin.getMetadataForRun(customRun1));
  }

  @Test
  public void testLineageSimplifying() throws Exception {
    // tests that if there's a READ and a WRITE, it gets served as a READ_WRITE
    // also tests that UNKNOWN is ignored when a READ_WRITE is served

    // Lineage for D3 -> P2 -> D2 -> P1 -> D1
    //             |     |
    //             |     V
    //             |<-----

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), getDatasetFramework(),
                                                 Id.DatasetInstance.from("default", "testSimpleLineage"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityValidator());

    // Add accesses for D3 -> P2 -> D2 -> P1 -> D1
    // We need to use current time here as metadata store stores access time using current time
    Id.Run run1 = new Id.Run(program1, RunIds.generate(System.currentTimeMillis()).getId());
    Id.Run run2 = new Id.Run(program2, RunIds.generate(System.currentTimeMillis()).getId());
    addRuns(store, run1, run2);
    // It is okay to use current time here since access time is ignore during assertions
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.READ, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.UNKNOWN, System.currentTimeMillis(), flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.WRITE, System.currentTimeMillis(), flowlet2);
    // a READ, WRITE, and UNKNOWN will be served as a READ_WRITE
    lineageStore.addAccess(run2, dataset3, AccessType.READ, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.WRITE, System.currentTimeMillis(), flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.UNKNOWN, System.currentTimeMillis(), flowlet2);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.UNKNOWN, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program2, AccessType.WRITE, twillRunId(run2), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.READ_WRITE, twillRunId(run2), toSet(flowlet2))
      )
    );

    // Lineage for D1
    Assert.assertEquals(expectedLineage,
                        lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000, 100));

    // Lineage for D2
    Assert.assertEquals(expectedLineage,
                        lineageAdmin.computeLineage(dataset2, 500, System.currentTimeMillis() + 10000, 100));

    // Lineage for D1 for one level should be D2 -> P1 -> D1
    Lineage oneLevelLineage = lineageAdmin.computeLineage(dataset1, 500, System.currentTimeMillis() + 10000, 1);

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, twillRunId(run1), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.UNKNOWN, twillRunId(run1), toSet(flowlet1))
      ),
      oneLevelLineage.getRelations());
  }

  @Test
  public void testSimpleLoopLineage() throws Exception {
    // Lineage for D1 -> P1 -> D2 -> P2 -> D3 -> P3 -> D4
    //             |                 |
    //             |                 V
    //             |<-----------------
    //

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), getDatasetFramework(),
                                                 Id.DatasetInstance.from("default", "testSimpleLoopLineage"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityValidator());


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
                                                 Id.DatasetInstance.from("default", "testDirectCycle"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityValidator());

    // Add accesses
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageStore.addAccess(run1, dataset1, AccessType.READ, System.currentTimeMillis(), flowlet1);
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, System.currentTimeMillis(), flowlet1);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(new Relation(dataset1, program1, AccessType.READ_WRITE, twillRunId(run1), toSet(flowlet1)))
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
                                                 Id.DatasetInstance.from("default", "testDirectCycleTwoRuns"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityValidator());

    // Add accesses
    addRuns(store, run1, run2, run3, run4, run5);
    // It is okay to use current time here since access time is ignore during assertions
    lineageStore.addAccess(run1, dataset1, AccessType.READ, System.currentTimeMillis(), flowlet1);
    // Write is in a different run
    lineageStore.addAccess(new Id.Run(run1.getProgram(), run2.getId()), dataset1, AccessType.WRITE,
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
                                                 Id.DatasetInstance.from("default", "testBranchLineage"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityValidator());

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
                                                 Id.DatasetInstance.from("default", "testBranchLoopLineage"));
    Store store = getInjector().getInstance(Store.class);
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    LineageAdmin lineageAdmin = new LineageAdmin(lineageStore, store, metadataStore, new NoOpEntityValidator());

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

  private void addRuns(Store store, Id.Run... runs) {
    for (Id.Run run : runs) {
      store.setStart(run.getProgram(), run.getId(), RunIds.getTime(RunIds.fromString(run.getId()), TimeUnit.SECONDS));
    }
  }

  @SafeVarargs
  private static <T> Set<T> toSet(T... elements) {
    return ImmutableSet.copyOf(elements);
  }

  private Map<String, String> toMap(String key, String value) {
    return ImmutableMap.of(key, value);
  }

  private static Set<Id.NamespacedId> emptySet() {
    return Collections.emptySet();
  }

  private RunId twillRunId(Id.Run run) {
    return RunIds.fromString(run.getId());
  }

  private TransactionExecutorFactory getTxExecFactory() {
    return getInjector().getInstance(TransactionExecutorFactory.class);
  }

  private DatasetFramework getDatasetFramework() {
    return getInjector().getInstance(DatasetFramework.class);
  }

  private static final class NoOpEntityValidator extends EntityValidator {
    public NoOpEntityValidator() {
      // This entity validator does not do any validation.
      // Hence it is okay to pass in null as constructor arguments.
      super(null, null, null, null, null);
    }

    @Override
    public void ensureEntityExists(Id.NamespacedId entityId) throws NotFoundException {
      // no-op
    }

    @Override
    public void ensureRunExists(Id.Run run) throws NotFoundException {
      // no-op
    }
  }
}
