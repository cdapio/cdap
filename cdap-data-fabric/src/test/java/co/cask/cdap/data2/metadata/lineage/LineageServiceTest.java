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
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

/**
 * Tests lineage computation.
 */
public class LineageServiceTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @Test
  public void testSimpleLineage() throws Exception {
    LineageStore lineageStore = new LineageStore(getTxExecFactory(), dsFrameworkUtil.getFramework(),
                                                 Id.DatasetInstance.from("default", "testSimpleLineage"));
    LineageService lineageService = new LineageService(lineageStore);

    // Define a run
    Id.DatasetInstance datasetInstance1 = Id.DatasetInstance.from("default", "dataset1");
    Id.DatasetInstance datasetInstance2 = Id.DatasetInstance.from("default", "dataset2");
    Id.Program program1 = Id.Program.from("default", "app1", ProgramType.FLOW, "flow1");
    Id.Flow.Flowlet flowlet1 = Id.Flow.Flowlet.from(program1.getApplication(), program1.getId(), "flowlet1");
    Id.Run run1 = new Id.Run(program1, RunIds.generate(10000).getId());

    // Define another run
    Id.DatasetInstance datasetInstance3 = Id.DatasetInstance.from("default", "dataset3");
    Id.Program program2 = Id.Program.from("default", "app2", ProgramType.FLOW, "flow2");
    Id.Flow.Flowlet flowlet2 = Id.Flow.Flowlet.from(program2.getApplication(), program2.getId(), "flowlet2");
    Id.Run run2 = new Id.Run(program2, RunIds.generate(900).getId());

    // Add accesses for D3 -> P2 -> D2 -> P1 -> D1
    lineageStore.addAccess(run1, datasetInstance1, AccessType.WRITE, "metadata11", flowlet1);
    lineageStore.addAccess(run1, datasetInstance2, AccessType.READ, "metadata12", flowlet1);

    lineageStore.addAccess(run2, datasetInstance2, AccessType.WRITE, "metadata21", flowlet2);
    lineageStore.addAccess(run2, datasetInstance3, AccessType.READ, "metadata22", flowlet2);

    // Lineage for D1 should be D3 -> P2 -> D2 -> P1 -> D1
    Lineage lineage = lineageService.computeLineage(Id.DatasetInstance.from("default", "dataset1"), 500, 20000, 5);
    Assert.assertNotNull(lineage);

    Assert.assertEquals(ImmutableSet.of(program1, program2), lineage.getPrograms());
    Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2, datasetInstance3), lineage.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(datasetInstance1, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(datasetInstance2, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(datasetInstance2, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(datasetInstance3, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2))
      ),
      lineage.getRelations());

    // Lineage for D2 should be D3 -> P2 -> D2
    Lineage lineage2 = lineageService.computeLineage(Id.DatasetInstance.from("default", "dataset2"), 500, 20000, 5);
    Assert.assertNotNull(lineage2);

    Assert.assertEquals(ImmutableSet.of(program2), lineage2.getPrograms());
    Assert.assertEquals(ImmutableSet.of(datasetInstance2, datasetInstance3), lineage2.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(datasetInstance2, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(datasetInstance3, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2))
      ),
      lineage2.getRelations());

    // Lineage for D1 for one level should be D2 -> P1 -> D1
    Lineage lineage3 = lineageService.computeLineage(Id.DatasetInstance.from("default", "dataset1"), 500, 20000, 1);
    Assert.assertNotNull(lineage3);

    Assert.assertEquals(ImmutableSet.of(program1), lineage3.getPrograms());
    Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2), lineage3.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(datasetInstance1, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(datasetInstance2, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1))
      ),
      lineage3.getRelations());
  }

  @SafeVarargs
  private static <T> Set<T> toSet(T... elements) {
    return ImmutableSet.copyOf(elements);
  }

  private RunId twillRunId(Id.Run run) {
    return RunIds.fromString(run.getId());
  }

  private TransactionExecutorFactory getTxExecFactory() {
    return dsFrameworkUtil.getInjector().getInstance(TransactionExecutorFactory.class);
  }
}
