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
  public void testOneRelaltion() throws Exception {
    LineageDataset lineageDataset = getLineageDataset("testOneRelaltion");
    Assert.assertNotNull(lineageDataset);

    RunId runId = RunIds.generate(10000);
    Id.DatasetInstance datasetInstance = Id.DatasetInstance.from("default", "dataset1");
    Id.Program program = Id.Program.from("default", "app1", ProgramType.FLOW, "flow1");
    Id.Flow.Flowlet flowlet = Id.Flow.Flowlet.from(program.getApplication(), program.getId(), "flowlet1");

    lineageDataset.addAccess(new Id.Run(program, runId.getId()),
                             datasetInstance, AccessType.READ, "metadata",
                             flowlet);

    Relation expected = new Relation(datasetInstance, program, AccessType.READ,
                                     ImmutableSet.of(runId), ImmutableSet.of(flowlet));

    Set<Relation> relations = lineageDataset.getRelations(datasetInstance, 0, 100000);
    Assert.assertEquals(1, relations.size());
    Assert.assertEquals(expected, relations.iterator().next());
  }

  @Test
  public void testMultipleRelaltions() throws Exception {
    LineageDataset lineageDataset = getLineageDataset("testMultipleRelaltions");
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

    lineageDataset.addAccess(new Id.Run(program1, runId1.getId()), datasetInstance1, AccessType.READ, "metadata111",
                             flowlet1);
    lineageDataset.addAccess(new Id.Run(program2, runId2.getId()), datasetInstance2, AccessType.WRITE, "metadata222");
    lineageDataset.addAccess(new Id.Run(program2, runId2.getId()), stream1, AccessType.READ, "metadata222s");
    lineageDataset.addAccess(new Id.Run(program2, runId3.getId()), stream2, AccessType.READ, "metadata232s");
    lineageDataset.addAccess(new Id.Run(program2, runId3.getId()), datasetInstance2, AccessType.WRITE, "metadata232");
    lineageDataset.addAccess(new Id.Run(program3, runId4.getId()), datasetInstance2, AccessType.READ_WRITE,
                             "metadata342");
    lineageDataset.addAccess(new Id.Run(program3, runId4.getId()), stream2, AccessType.UNKNOWN, "metadata342s");

    Assert.assertEquals(
      ImmutableSet.of(new Relation(datasetInstance1, program1, AccessType.READ, ImmutableSet.of(runId1),
                                   ImmutableSet.of(flowlet1))),
      lineageDataset.getRelations(datasetInstance1, 0, 100000)
    );

    Assert.assertEquals(
      ImmutableSet.of(new Relation(datasetInstance2, program2, AccessType.WRITE, ImmutableSet.of(runId2)),
                      new Relation(datasetInstance2, program2, AccessType.WRITE, ImmutableSet.of(runId3)),
                      new Relation(datasetInstance2, program3, AccessType.READ_WRITE, ImmutableSet.of(runId4))
                      ),
      lineageDataset.getRelations(datasetInstance2, 0, 100000)
    );

    Assert.assertEquals(
      ImmutableSet.of(new Relation(stream1, program2, AccessType.READ, ImmutableSet.of(runId2))),
      lineageDataset.getRelations(stream1, 0, 100000)
    );

    Assert.assertEquals(
      ImmutableSet.of(new Relation(stream2, program2, AccessType.READ, ImmutableSet.of(runId3)),
                      new Relation(stream2, program3, AccessType.UNKNOWN, ImmutableSet.of(runId4))),
      lineageDataset.getRelations(stream2, 0, 100000)
    );

    Assert.assertEquals(
      ImmutableSet.of(new Relation(datasetInstance2, program2, AccessType.WRITE, ImmutableSet.of(runId2)),
                      new Relation(stream1, program2, AccessType.READ, ImmutableSet.of(runId2)),
                      new Relation(datasetInstance2, program2, AccessType.WRITE, ImmutableSet.of(runId3)),
                      new Relation(stream2, program2, AccessType.READ, ImmutableSet.of(runId3))
      ),
      lineageDataset.getRelations(program2, 0, 100000)
    );

    // Reduced time range
    Assert.assertEquals(
      ImmutableSet.of(new Relation(datasetInstance2, program2, AccessType.WRITE, ImmutableSet.of(runId2)),
                      new Relation(datasetInstance2, program2, AccessType.WRITE, ImmutableSet.of(runId3))
      ),
      lineageDataset.getRelations(datasetInstance2, 0, 35000)
    );
  }

  private static LineageDataset getLineageDataset(String instanceId) throws Exception {
    Id.DatasetInstance id = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, instanceId);
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), id,
                                           LineageDataset.class.getName(), DatasetProperties.EMPTY, null, null);
  }
}
