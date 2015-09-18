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
import co.cask.cdap.proto.metadata.MetadataRecord;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

/**
 * Tests storage and retrieval of Dataset accesses by Programs in {@link LineageDataset}.
 */
public class LineageDatasetTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final Set<MetadataRecord> EMPTY_METADATA = ImmutableSet.of();

  @Test
  public void testOneRelation() throws Exception {
    LineageDataset lineageDataset = getLineageDataset("testOneRelation");
    Assert.assertNotNull(lineageDataset);

    RunId runId = RunIds.generate(10000);
    Id.DatasetInstance datasetInstance = Id.DatasetInstance.from("default", "dataset1");
    Id.Program program = Id.Program.from("default", "app1", ProgramType.FLOW, "flow1");
    Id.Flow.Flowlet flowlet = Id.Flow.Flowlet.from(program.getApplication(), program.getId(), "flowlet1");
    Id.Run run = new Id.Run(program, runId.getId());

    MetadataRecord programMeta = new MetadataRecord(program, toMap("pkey1", "pval1", "pkey2", "pval2"),
                                                    toSet("ptag1", "ptag2"));
    MetadataRecord dataMeta = new MetadataRecord(datasetInstance, toMap("dkey1", "dval1", "dkey2", "dval2"),
                                                 toSet("dtag1", "dtag2"));
    Set<MetadataRecord> metadataRecords = toSet(programMeta, dataMeta);

    lineageDataset.addAccess(run,
                             datasetInstance, AccessType.READ, metadataRecords,
                             flowlet);

    Relation expected = new Relation(datasetInstance, program, AccessType.READ,
                                     runId, ImmutableSet.of(flowlet));

    Set<Relation> relations = lineageDataset.getRelations(datasetInstance, 0, 100000,
                                                          Predicates.<Relation>alwaysTrue());
    Assert.assertEquals(1, relations.size());
    Assert.assertEquals(expected, relations.iterator().next());

    // Assert metadata
    Assert.assertEquals(metadataRecords, lineageDataset.getRunMetadata(run));
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

    // Define metadata
    Set<MetadataRecord> metaProgram1Data1Run1 =
      toSet(new MetadataRecord(program1, toMap("pkey111", "pval111"), toSet("ptag111-1")),
            new MetadataRecord(datasetInstance1, toMap("dkey111", "dval111"), toSet("dtag111-1")));

    MetadataRecord metadataRecordProgram2Run2 =
      new MetadataRecord(program2, toMap("pkey22", "pval22"), toSet("ptag22-1"));
    Set<MetadataRecord> metaProgram2Data2Run2 =
      toSet(metadataRecordProgram2Run2,
            new MetadataRecord(datasetInstance2, toMap("dkey222", "dval222"), toSet("dtag222-1")));
    Set<MetadataRecord> metaProgram2Stream1Run2 =
      toSet(metadataRecordProgram2Run2,
            new MetadataRecord(stream1, toMap("dkey212", "dval212"), toSet("dtag212-1")));

    MetadataRecord metadataRecordProgram2Run3 =
      new MetadataRecord(program2, toMap("pkey223", "pval223"), toSet("ptag223-1"));
    Set<MetadataRecord> metaProgram2Stream2Run3 =
      toSet(metadataRecordProgram2Run3,
            new MetadataRecord(stream2, toMap("dkey223", "dval223"), toSet("dtag223-1")));
    Set<MetadataRecord> metaProgram2Data2Run3 =
      toSet(metadataRecordProgram2Run3,
            new MetadataRecord(datasetInstance2, toMap("dkey223.1", "dval223"), toSet("dtag223-1.1")));

    Set<MetadataRecord> metaProgram3Data2Run4 =
      toSet(new MetadataRecord(program3, toMap("pkey324", "pval324"), toSet("ptag324-1")),
            new MetadataRecord(datasetInstance2, toMap("dkey324", "dval324"), toSet("dtag324-1")));

    lineageDataset.addAccess(run11, datasetInstance1, AccessType.READ, metaProgram1Data1Run1, flowlet1);
    lineageDataset.addAccess(run22, datasetInstance2, AccessType.WRITE, metaProgram2Data2Run2);
    lineageDataset.addAccess(run22, stream1, AccessType.READ, metaProgram2Stream1Run2);
    lineageDataset.addAccess(run23, stream2, AccessType.READ, metaProgram2Stream2Run3);
    lineageDataset.addAccess(run23, datasetInstance2, AccessType.WRITE, metaProgram2Data2Run3);
    lineageDataset.addAccess(run34, datasetInstance2, AccessType.READ_WRITE, metaProgram3Data2Run4);
    lineageDataset.addAccess(run34, stream2, AccessType.UNKNOWN, EMPTY_METADATA);

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

    // Verify metadata
    Assert.assertEquals(metaProgram1Data1Run1, lineageDataset.getRunMetadata(run11));
    Assert.assertEquals(2, lineageDataset.getRunMetadata(run11).size());
    Assert.assertEquals(toSet(metaProgram2Data2Run2, metaProgram2Stream1Run2),
                        lineageDataset.getRunMetadata(run22));
    Assert.assertEquals(3, lineageDataset.getRunMetadata(run22).size());
    Assert.assertEquals(toSet(metaProgram2Stream2Run3, metaProgram2Data2Run3),
                        lineageDataset.getRunMetadata(run23));
    Assert.assertEquals(3, lineageDataset.getRunMetadata(run23).size());
    Assert.assertEquals(metaProgram3Data2Run4, lineageDataset.getRunMetadata(run34));
    Assert.assertEquals(2, lineageDataset.getRunMetadata(run34).size());
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

  private Map<String, String> toMap(String key, String value) {
    return ImmutableMap.of(key, value);
  }

  private Map<String, String> toMap(String key1, String value1, String key2, String value2) {
    return ImmutableMap.of(key1, value1, key2, value2);
  }

  @SafeVarargs
  private static Set<MetadataRecord> toSet(Set<MetadataRecord>... records) {
    ImmutableSet.Builder<MetadataRecord> recordBuilder = ImmutableSet.builder();
    for (Set<MetadataRecord> recordSet : records) {
      recordBuilder.addAll(recordSet);
    }
    return recordBuilder.build();
  }
}
