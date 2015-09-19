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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
    LineageDataset lineageDataset = getLineageDataset("testOneRelation");
    Assert.assertNotNull(lineageDataset);

    RunId runId = RunIds.generate(10000);
    Id.DatasetInstance datasetInstance = Id.DatasetInstance.from("default", "dataset1");
    Id.Program program = Id.Program.from("default", "app1", ProgramType.FLOW, "flow1");
    Id.Flow.Flowlet flowlet = Id.Flow.Flowlet.from(program.getApplication(), program.getId(), "flowlet1");
    Id.Run run = new Id.Run(program, runId.getId());

    long accessTimeMillis = System.currentTimeMillis();
    lineageDataset.addAccess(run,
                             datasetInstance, AccessType.READ, accessTimeMillis,
                             flowlet);

    Relation expected = new Relation(datasetInstance, program, AccessType.READ,
                                     runId, ImmutableSet.of(flowlet));

    Set<Relation> relations = lineageDataset.getRelations(datasetInstance, 0, 100000,
                                                          Predicates.<Relation>alwaysTrue());
    Assert.assertEquals(1, relations.size());
    Assert.assertEquals(expected, relations.iterator().next());

    Assert.assertEquals(toSet(datasetInstance, program), lineageDataset.getEntitiesForRun(run));
    Assert.assertEquals(ImmutableList.of(accessTimeMillis), lineageDataset.getAccessTimesForRun(run));
  }

  @Test
  public void testMultipleRelations() throws Exception {
    LineageDataset lineageDataset = getLineageDataset("testMultipleRelations");
    Assert.assertNotNull(lineageDataset);

    RunId runId1 = RunIds.generate(10000);
    RunId runId2 = RunIds.generate(20000);
    RunId runId3 = RunIds.generate(30000);
    RunId runId4 = RunIds.generate(40000);

    Id.DatasetInstance datasetInstance1 = Id.DatasetInstance.from("default", "dataset1");
    Id.DatasetInstance datasetInstance2 = Id.DatasetInstance.from("default", "dataset2");

    Id.Stream stream1 = Id.Stream.from("default", "stream1");
    Id.Stream stream2 = Id.Stream.from("default", "stream2");

    Id.Program program1 = Id.Program.from("default", "app1", ProgramType.FLOW, "flow1");
    Id.Flow.Flowlet flowlet1 = Id.Flow.Flowlet.from(program1.getApplication(), program1.getId(), "flowlet1");
    Id.Program program2 = Id.Program.from("default", "app2", ProgramType.WORKER, "worker2");
    Id.Program program3 = Id.Program.from("default", "app3", ProgramType.SERVICE, "service3");

    Id.Run run11 = new Id.Run(program1, runId1.getId());
    Id.Run run22 = new Id.Run(program2, runId2.getId());
    Id.Run run23 = new Id.Run(program2, runId3.getId());
    Id.Run run34 = new Id.Run(program3, runId4.getId());

    long now = System.currentTimeMillis();
    //noinspection UnnecessaryLocalVariable
    long run11Data1AccessTime = now;
    lineageDataset.addAccess(run11, datasetInstance1, AccessType.READ, run11Data1AccessTime, flowlet1);
    long run22Data2AccessTime = now + 1;
    lineageDataset.addAccess(run22, datasetInstance2, AccessType.WRITE, run22Data2AccessTime);
    long run22Stream1AccessTime = now + 2;
    lineageDataset.addAccess(run22, stream1, AccessType.READ, run22Stream1AccessTime);
    long run23Stream2AccessTime = now + 1;
    lineageDataset.addAccess(run23, stream2, AccessType.READ, run23Stream2AccessTime);
    long run23Data2AccessTime = now + 3;
    lineageDataset.addAccess(run23, datasetInstance2, AccessType.WRITE, run23Data2AccessTime);
    lineageDataset.addAccess(run34, datasetInstance2, AccessType.READ_WRITE, System.currentTimeMillis());
    lineageDataset.addAccess(run34, stream2, AccessType.UNKNOWN, System.currentTimeMillis());

    Assert.assertEquals(
      ImmutableSet.of(new Relation(datasetInstance1, program1, AccessType.READ, runId1,
                                   ImmutableSet.of(flowlet1))),
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

  private static LineageDataset getLineageDataset(String instanceId) throws Exception {
    Id.DatasetInstance id = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, instanceId);
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), id,
                                           LineageDataset.class.getName(), DatasetProperties.EMPTY, null, null);
  }

  @SafeVarargs
  private static <T> Set<T> toSet(T... elements) {
    return ImmutableSet.copyOf(elements);
  }
}
