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
    long stopTime = 1234;

    lineageDataset.addAccess(new Id.Run(program, runId.getId()),
                             datasetInstance, AccessType.READ, stopTime,
                             flowlet);

    Set<Relation> relations = lineageDataset.getRelations(datasetInstance, 0, 100000);
    Assert.assertEquals(1, relations.size());
    Relation relation = relations.iterator().next();
    Assert.assertEquals(datasetInstance, relation.getData());
    Assert.assertEquals(program, relation.getProgram());
    Assert.assertEquals(runId.getId(), relation.getRuns().iterator().next().getId());
    Assert.assertEquals(AccessType.READ, relation.getAccess());
    Assert.assertEquals(flowlet, relation.getComponents().iterator().next());
  }

  private static LineageDataset getLineageDataset(String instanceId) throws Exception {
    Id.DatasetInstance id = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, instanceId);
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), id,
                                           LineageDataset.class.getName(), DatasetProperties.EMPTY, null, null);
  }
}
