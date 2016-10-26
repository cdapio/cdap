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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

/**
 * Tests storage and retrieval of Dataset accesses by Programs in {@link LineageDataset}.
 */
public class LineageDatasetTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @Test
  public void testOneRelation() throws Exception {
    final LineageDataset lineageDataset = getLineageDataset("testOneRelation");
    Assert.assertNotNull(lineageDataset);
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) lineageDataset);

    final RunId runId = RunIds.generate(10000);
    final DatasetId datasetInstance = new DatasetId("default", "dataset1");
    final ProgramId program = new ProgramId("default", "app1", ProgramType.FLOW, "flow1");
    final FlowletId flowlet = program.flowlet("flowlet1");
    final ProgramRunId run = program.run(runId.getId());

    final long accessTimeMillis = System.currentTimeMillis();
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        lineageDataset.addAccess(run, datasetInstance, AccessType.READ, accessTimeMillis, flowlet);
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Relation expected = new Relation(datasetInstance, program, AccessType.READ,
                                         runId, ImmutableSet.of(flowlet));
        Set<Relation> relations = lineageDataset.getRelations(datasetInstance, 0, 100000,
                                                              Predicates.<Relation>alwaysTrue());
        Assert.assertEquals(1, relations.size());
        Assert.assertEquals(expected, relations.iterator().next());
        Assert.assertEquals(toSet(program, datasetInstance), lineageDataset.getEntitiesForRun(run));
        Assert.assertEquals(ImmutableList.of(accessTimeMillis), lineageDataset.getAccessTimesForRun(run));
      }
    });
  }

  @Test
  public void testMultipleRelations() throws Exception {
    final LineageDataset lineageDataset = getLineageDataset("testMultipleRelations");
    Assert.assertNotNull(lineageDataset);
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) lineageDataset);

    final RunId runId1 = RunIds.generate(10000);
    final RunId runId2 = RunIds.generate(20000);
    final RunId runId3 = RunIds.generate(30000);
    final RunId runId4 = RunIds.generate(40000);

    final DatasetId datasetInstance1 = NamespaceId.DEFAULT.dataset("dataset1");
    final DatasetId datasetInstance2 = NamespaceId.DEFAULT.dataset("dataset2");

    final StreamId stream1 = NamespaceId.DEFAULT.stream("stream1");
    final StreamId stream2 = NamespaceId.DEFAULT.stream("stream2");

    final ProgramId program1 = NamespaceId.DEFAULT.app("app1").flow("flow1");
    final FlowletId flowlet1 = program1.flowlet("flowlet1");
    final ProgramId program2 = NamespaceId.DEFAULT.app("app2").worker("worker2");
    final ProgramId program3 = NamespaceId.DEFAULT.app("app3").service("service3");

    final ProgramRunId run11 = program1.run(runId1.getId());
    final ProgramRunId run22 = program2.run(runId2.getId());
    final ProgramRunId run23 = program2.run(runId3.getId());
    final ProgramRunId run34 = program3.run(runId4.getId());

    final long now = System.currentTimeMillis();
    final long run11Data1AccessTime = now;
    final long run22Data2AccessTime = now + 1;
    final long run22Stream1AccessTime = now + 2;
    final long run23Stream2AccessTime = now + 1;
    final long run23Data2AccessTime = now + 3;
    //noinspection UnnecessaryLocalVariable
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        lineageDataset.addAccess(run11, datasetInstance1, AccessType.READ, run11Data1AccessTime, flowlet1);
        lineageDataset.addAccess(run22, datasetInstance2, AccessType.WRITE, run22Data2AccessTime);
        lineageDataset.addAccess(run22, stream1, AccessType.READ, run22Stream1AccessTime);
        lineageDataset.addAccess(run23, stream2, AccessType.READ, run23Stream2AccessTime);
        lineageDataset.addAccess(run23, datasetInstance2, AccessType.WRITE, run23Data2AccessTime);
        lineageDataset.addAccess(run34, datasetInstance2, AccessType.READ_WRITE, System.currentTimeMillis());
        lineageDataset.addAccess(run34, stream2, AccessType.UNKNOWN, System.currentTimeMillis());
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(
          ImmutableSet.of(new Relation(datasetInstance1, program1, AccessType.READ, runId1, ImmutableSet.of(flowlet1))),
          lineageDataset.getRelations(datasetInstance1, 0, 100000, Predicates.<Relation>alwaysTrue())
        );

        Assert.assertEquals(
          ImmutableSet.of(new Relation(datasetInstance2, program2, AccessType.WRITE, runId2),
                          new Relation(datasetInstance2, program2, AccessType.WRITE, runId3),
                          new Relation(datasetInstance2, program3, AccessType.READ_WRITE, runId4)
          ),
          lineageDataset.getRelations(datasetInstance2, 0, 100000, Predicates.<Relation>alwaysTrue())
        );

        Assert.assertEquals(
          ImmutableSet.of(new Relation(stream1, program2, AccessType.READ, runId2)),
          lineageDataset.getRelations(stream1, 0, 100000, Predicates.<Relation>alwaysTrue())
        );

        Assert.assertEquals(
          ImmutableSet.of(new Relation(stream2, program2, AccessType.READ, runId3),
                          new Relation(stream2, program3, AccessType.UNKNOWN, runId4)),
          lineageDataset.getRelations(stream2, 0, 100000, Predicates.<Relation>alwaysTrue())
        );

        Assert.assertEquals(
          ImmutableSet.of(new Relation(datasetInstance2, program2, AccessType.WRITE, runId2),
                          new Relation(stream1, program2, AccessType.READ, runId2),
                          new Relation(datasetInstance2, program2, AccessType.WRITE, runId3),
                          new Relation(stream2, program2, AccessType.READ, runId3)
          ),
          lineageDataset.getRelations(program2, 0, 100000, Predicates.<Relation>alwaysTrue())
        );

        // Reduced time range
        Assert.assertEquals(
          ImmutableSet.of(new Relation(datasetInstance2, program2, AccessType.WRITE, runId2),
                          new Relation(datasetInstance2, program2, AccessType.WRITE, runId3)
          ),
          lineageDataset.getRelations(datasetInstance2, 0, 35000, Predicates.<Relation>alwaysTrue())
        );

        Assert.assertEquals(toSet(program1, datasetInstance1), lineageDataset.getEntitiesForRun(run11));
        Assert.assertEquals(ImmutableList.of(run11Data1AccessTime), lineageDataset.getAccessTimesForRun(run11));

        Assert.assertEquals(toSet(program2, datasetInstance2, stream1), lineageDataset.getEntitiesForRun(run22));
        Assert.assertEquals(ImmutableList.of(run22Data2AccessTime, run22Stream1AccessTime),
                            lineageDataset.getAccessTimesForRun(run22));

        Assert.assertEquals(toSet(program2, datasetInstance2, stream2), lineageDataset.getEntitiesForRun(run23));
        Assert.assertEquals(ImmutableList.of(run23Data2AccessTime, run23Stream2AccessTime),
                            lineageDataset.getAccessTimesForRun(run23));

        Assert.assertEquals(toSet(program3, datasetInstance2, stream2), lineageDataset.getEntitiesForRun(run34));
      }
    });
  }

  private static LineageDataset getLineageDataset(String instanceId) throws Exception {
    DatasetId id = DatasetFrameworkTestUtil.NAMESPACE_ID.dataset(instanceId);
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), id,
                                           LineageDataset.class.getName(), DatasetProperties.EMPTY, null, null);
  }

  @SafeVarargs
  private static <T> Set<T> toSet(T... elements) {
    return ImmutableSet.copyOf(elements);
  }
}
