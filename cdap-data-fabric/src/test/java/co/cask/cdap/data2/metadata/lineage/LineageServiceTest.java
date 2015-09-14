/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Tests lineage computation.
 */
public class LineageServiceTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final Set<MetadataRecord> EMPTY_METADATA = ImmutableSet.of();

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

  @Test
  public void testSimpleLineage() throws Exception {
    // Lineage for D3 -> P2 -> D2 -> P1 -> D1

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), dsFrameworkUtil.getFramework(),
                                                 Id.DatasetInstance.from("default", "testSimpleLineage"));
    LineageService lineageService = new LineageService(lineageStore);

    // Define metadata
    MetadataRecord run1Meta = new MetadataRecord(program1, toMap("pk1", "pk1"), toSet("pt1"));
    Set<MetadataRecord> run1data1 = toSet(run1Meta,
                                          new MetadataRecord(dataset1, toMap("dk1", "dk1"), toSet("dt1")));
    Set<MetadataRecord> run1data2 = toSet(run1Meta,
                                          new MetadataRecord(dataset2, toMap("dk2", "dk2"), toSet("dt2")));

    // Add accesses for D3 -> P2 -> D2 -> P1 -> D1
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, run1data1, flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.READ, run1data2, flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.WRITE, EMPTY_METADATA, flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.READ, EMPTY_METADATA, flowlet2);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2))
      )
    );

    // Lineage for D1
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset1, 500, 20000, 100));

    // Lineage for D2
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset2, 500, 20000, 100));

    // Lineage for D1 for one level should be D2 -> P1 -> D1
    Lineage oneLevelLineage = lineageService.computeLineage(dataset1, 500, 20000, 1);

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1))
      ),
      oneLevelLineage.getRelations());

    // Assert metadata
    Assert.assertEquals(toSet(run1data1, run1data2), lineageStore.getAccesses(run1));
  }

  @Test
  public void testSimpleLoopLineage() throws Exception {
    // Lineage for D1 -> P1 -> D2 -> P2 -> D3 -> P3 -> D4
    //             |                 |
    //             |                 V
    //             |<-----------------
    //

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), dsFrameworkUtil.getFramework(),
                                                 Id.DatasetInstance.from("default", "testSimpleLoopLineage"));
    LineageService lineageService = new LineageService(lineageStore);

    // Add access
    lineageStore.addAccess(run1, dataset1, AccessType.READ, EMPTY_METADATA, flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.WRITE, EMPTY_METADATA, flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.READ, EMPTY_METADATA, flowlet2);
    lineageStore.addAccess(run2, dataset1, AccessType.WRITE, EMPTY_METADATA, flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.WRITE, EMPTY_METADATA, flowlet2);

    lineageStore.addAccess(run3, dataset3, AccessType.READ, EMPTY_METADATA);
    lineageStore.addAccess(run3, dataset4, AccessType.WRITE, EMPTY_METADATA);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset2, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset2, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset4, program3, AccessType.WRITE, toSet(twillRunId(run3)), emptySet()),
        new Relation(dataset3, program3, AccessType.READ, toSet(twillRunId(run3)), emptySet())
      )
    );

    // Lineage for D1
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset1, 500, 20000, 100));

    // Lineage for D2
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset2, 500, 20000, 100));

    // Lineage for D1 for one level D1 -> P1 -> D2 -> P2 -> D3
    //                              |                 |
    //                              |                 V
    //                              |<-----------------
    //
    Lineage oneLevelLineage = lineageService.computeLineage(dataset1, 500, 20000, 1);

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset2, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset2, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2))
      ),
      oneLevelLineage.getRelations());
  }

  @Test
  public void testDirectCycle() throws Exception {
    // Lineage for:
    //
    // D1 <-> P1
    //
    LineageStore lineageStore = new LineageStore(getTxExecFactory(), dsFrameworkUtil.getFramework(),
                                                 Id.DatasetInstance.from("default", "testDirectCycle"));
    LineageService lineageService = new LineageService(lineageStore);

    // Add accesses
    lineageStore.addAccess(run1, dataset1, AccessType.READ, EMPTY_METADATA, flowlet1);
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, EMPTY_METADATA, flowlet1);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1))
      )
    );

    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset1, 500, 20000, 100));
  }

  @Test
  public void testDirectCycleTwoRuns() throws Exception {
    // Lineage for:
    //
    // D1 -> P1 (run1)
    //
    // D1 <- P1 (run2)
    //
    LineageStore lineageStore = new LineageStore(getTxExecFactory(), dsFrameworkUtil.getFramework(),
                                                 Id.DatasetInstance.from("default", "testDirectCycleTwoRuns"));
    LineageService lineageService = new LineageService(lineageStore);

    // Add accesses
    lineageStore.addAccess(run1, dataset1, AccessType.READ, EMPTY_METADATA, flowlet1);
    // Write is in a different run
    lineageStore.addAccess(new Id.Run(run1.getProgram(), run2.getId()), dataset1, AccessType.WRITE,
                           EMPTY_METADATA, flowlet1);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet1))
      )
    );

    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset1, 500, 20000, 100));
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

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), dsFrameworkUtil.getFramework(),
                                                 Id.DatasetInstance.from("default", "testBranchLineage"));
    LineageService lineageService = new LineageService(lineageStore);

    // Add accesses
    lineageStore.addAccess(run1, stream1, AccessType.READ, EMPTY_METADATA, flowlet1);
    lineageStore.addAccess(run1, dataset1, AccessType.READ, EMPTY_METADATA, flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.WRITE, EMPTY_METADATA, flowlet1);
    lineageStore.addAccess(run1, dataset4, AccessType.WRITE, EMPTY_METADATA, flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.READ, EMPTY_METADATA, flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.WRITE, EMPTY_METADATA, flowlet2);
    lineageStore.addAccess(run2, dataset5, AccessType.WRITE, EMPTY_METADATA, flowlet2);

    lineageStore.addAccess(run3, dataset5, AccessType.READ, EMPTY_METADATA);
    lineageStore.addAccess(run3, dataset6, AccessType.WRITE, EMPTY_METADATA);

    lineageStore.addAccess(run4, dataset2, AccessType.READ, EMPTY_METADATA);
    lineageStore.addAccess(run4, dataset3, AccessType.READ, EMPTY_METADATA);
    lineageStore.addAccess(run4, dataset7, AccessType.WRITE, EMPTY_METADATA);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(stream1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset4, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),

        new Relation(dataset2, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset5, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),

        new Relation(dataset5, program3, AccessType.READ, toSet(twillRunId(run3)), emptySet()),
        new Relation(dataset6, program3, AccessType.WRITE, toSet(twillRunId(run3)), emptySet()),

        new Relation(dataset2, program4, AccessType.READ, toSet(twillRunId(run4)), emptySet()),
        new Relation(dataset3, program4, AccessType.READ, toSet(twillRunId(run4)), emptySet()),
        new Relation(dataset7, program4, AccessType.WRITE, toSet(twillRunId(run4)), emptySet())
      )
    );

    // Lineage for D7
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset7, 500, 20000, 100));
    // Lineage for D6
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset6, 500, 20000, 100));
    // Lineage for D3
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset3, 500, 20000, 100));
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

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), dsFrameworkUtil.getFramework(),
                                                 Id.DatasetInstance.from("default", "testBranchLoopLineage"));
    LineageService lineageService = new LineageService(lineageStore);

    // Add accesses
    lineageStore.addAccess(run1, stream1, AccessType.READ, EMPTY_METADATA, flowlet1);
    lineageStore.addAccess(run1, dataset1, AccessType.READ, EMPTY_METADATA, flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.WRITE, EMPTY_METADATA, flowlet1);
    lineageStore.addAccess(run1, dataset4, AccessType.WRITE, EMPTY_METADATA, flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.READ, EMPTY_METADATA, flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.WRITE, EMPTY_METADATA, flowlet2);
    lineageStore.addAccess(run2, dataset5, AccessType.WRITE, EMPTY_METADATA, flowlet2);

    lineageStore.addAccess(run3, dataset5, AccessType.READ, EMPTY_METADATA);
    lineageStore.addAccess(run3, dataset6, AccessType.WRITE, EMPTY_METADATA);

    lineageStore.addAccess(run4, dataset2, AccessType.READ, EMPTY_METADATA);
    lineageStore.addAccess(run4, dataset3, AccessType.READ, EMPTY_METADATA);
    lineageStore.addAccess(run4, dataset7, AccessType.WRITE, EMPTY_METADATA);

    lineageStore.addAccess(run5, dataset3, AccessType.READ, EMPTY_METADATA);
    lineageStore.addAccess(run5, dataset6, AccessType.READ, EMPTY_METADATA);
    lineageStore.addAccess(run5, dataset1, AccessType.WRITE, EMPTY_METADATA);

    Lineage expectedLineage = new Lineage(
      ImmutableSet.of(
        new Relation(stream1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset4, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),

        new Relation(dataset2, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset5, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),

        new Relation(dataset5, program3, AccessType.READ, toSet(twillRunId(run3)), emptySet()),
        new Relation(dataset6, program3, AccessType.WRITE, toSet(twillRunId(run3)), emptySet()),

        new Relation(dataset2, program4, AccessType.READ, toSet(twillRunId(run4)), emptySet()),
        new Relation(dataset3, program4, AccessType.READ, toSet(twillRunId(run4)), emptySet()),
        new Relation(dataset7, program4, AccessType.WRITE, toSet(twillRunId(run4)), emptySet()),

        new Relation(dataset3, program5, AccessType.READ, toSet(twillRunId(run5)), emptySet()),
        new Relation(dataset6, program5, AccessType.READ, toSet(twillRunId(run5)), emptySet()),
        new Relation(dataset1, program5, AccessType.WRITE, toSet(twillRunId(run5)), emptySet())
      )
    );

    // Lineage for D1
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset1, 500, 20000, 100));
    // Lineage for D5
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset5, 500, 20000, 100));
    // Lineage for D7
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(dataset7, 500, 20000, 100));
    // Lineage for S1
    Assert.assertEquals(expectedLineage, lineageService.computeLineage(stream1, 500, 20000, 100));

    // Lineage for D5 for one level
    //                   -> D5 -> P3 -> D6
    //                   |
    //                   |
    //             D2 -> P2 -> D3
    Lineage oneLevelLineage = lineageService.computeLineage(dataset5, 500, 20000, 1);

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset2, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset5, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),

        new Relation(dataset5, program3, AccessType.READ, toSet(twillRunId(run3)), emptySet()),
        new Relation(dataset6, program3, AccessType.WRITE, toSet(twillRunId(run3)), emptySet())
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

    oneLevelLineage = lineageService.computeLineage(stream1, 500, 20000, 1);
    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(stream1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset4, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1))
      ),
      oneLevelLineage.getRelations()
    );
  }

  @SafeVarargs
  private static <T> Set<T> toSet(T... elements) {
    return ImmutableSet.copyOf(elements);
  }

  private Map<String, String> toMap(String key, String value) {
    return ImmutableMap.of(key, value);
  }

  @SafeVarargs
  private static Set<MetadataRecord> toSet(Set<MetadataRecord>... records) {
    ImmutableSet.Builder<MetadataRecord> recordBuilder = ImmutableSet.builder();
    for (Set<MetadataRecord> recordSet : records) {
      recordBuilder.addAll(recordSet);
    }
    return recordBuilder.build();
  }

  private static Set<Id.NamespacedId> emptySet() {
    return Collections.emptySet();
  }

  private RunId twillRunId(Id.Run run) {
    return RunIds.fromString(run.getId());
  }

  private TransactionExecutorFactory getTxExecFactory() {
    return dsFrameworkUtil.getInjector().getInstance(TransactionExecutorFactory.class);
  }
}
