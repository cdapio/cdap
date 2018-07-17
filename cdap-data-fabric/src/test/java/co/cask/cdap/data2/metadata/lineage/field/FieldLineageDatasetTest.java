/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.lineage.field;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.metadata.lineage.ProgramRunOperations;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test for storage and retrieval of the field lineage operations.
 */
public class FieldLineageDatasetTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @Test
  public void testSimpleOperations() throws Exception {
    final FieldLineageDataset fieldLineageDataset = getFieldLineageDataset("testSimpleOperations");
    Assert.assertNotNull(fieldLineageDataset);
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) fieldLineageDataset);

    RunId runId = RunIds.generate(10000);
    ProgramId program = new ProgramId("default", "app1", ProgramType.WORKFLOW, "workflow1");
    final ProgramRunId programRun1 = program.run(runId.getId());

    runId = RunIds.generate(11000);
    program = new ProgramId("default", "app1", ProgramType.WORKFLOW, "workflow1");
    final ProgramRunId programRun2 = program.run(runId.getId());

    final FieldLineageInfo info1 = new FieldLineageInfo(generateOperations(false));
    final FieldLineageInfo info2 = new FieldLineageInfo(generateOperations(true));

    txnl.execute(() -> fieldLineageDataset.addFieldLineageInfo(programRun1, info1));
    txnl.execute(() -> fieldLineageDataset.addFieldLineageInfo(programRun2, info2));

    runId = RunIds.generate(12000);
    program = new ProgramId("default", "app1", ProgramType.WORKFLOW, "workflow3");
    final ProgramRunId programRun3 = program.run(runId.getId());

    txnl.execute(() -> fieldLineageDataset.addFieldLineageInfo(programRun3, info2));

    txnl.execute(() -> {
      EndPoint source = EndPoint.of("ns1", "endpoint1");
      EndPoint destination = EndPoint.of("myns", "another_file");

      // end time 10000 should return empty set since its exclusive and run was added at time 10000
      Assert.assertEquals(Collections.EMPTY_SET, fieldLineageDataset.getFields(destination, 0, 10000));

      Set<String> expectedFields = new HashSet<>(Arrays.asList("offset", "name"));
      // end time 10001 should return the data for the run which was added at time 10000
      Assert.assertEquals(expectedFields, fieldLineageDataset.getFields(destination, 0, 10001));
      // providing start time as 10000 and endtime as 11000 should still return the same set of fields
      Assert.assertEquals(expectedFields, fieldLineageDataset.getFields(destination, 10000, 11000));

      // setting endtime to 11001 should include the information for from programRun2 as well, which added additional
      // field to the dataset.
      expectedFields.add("file_name");
      Assert.assertEquals(expectedFields, fieldLineageDataset.getFields(destination, 10000, 11001));

      // end time 10000 should return empty set since its exclusive and run was added at time 10000
      Assert.assertEquals(Collections.EMPTY_SET,
              fieldLineageDataset.getIncomingSummary(new EndPointField(destination, "offset"), 0, 10000));

      EndPointField expectedEndPointField = new EndPointField(source, "offset");
      Set<EndPointField> actualEndPointFields
              = fieldLineageDataset.getIncomingSummary(new EndPointField(destination, "offset"), 0, 10001);
      Assert.assertEquals(expectedEndPointField, actualEndPointFields.iterator().next());

      expectedEndPointField = new EndPointField(source, "body");
      actualEndPointFields = fieldLineageDataset.getIncomingSummary(new EndPointField(destination, "name"), 0, 10001);
      Assert.assertEquals(expectedEndPointField, actualEndPointFields.iterator().next());

      // end time is 10001, file_name is not written yet
      actualEndPointFields = fieldLineageDataset.getIncomingSummary(new EndPointField(destination, "file_name"), 0,
                                                                    10001);
      Assert.assertEquals(Collections.EMPTY_SET, actualEndPointFields);

      // end time 10000 should return empty set since its exclusive and run was added at time 10000
      Assert.assertEquals(Collections.EMPTY_SET,
              fieldLineageDataset.getOutgoingSummary(new EndPointField(destination, "offset"), 0, 10000));

      expectedEndPointField = new EndPointField(destination, "offset");
      actualEndPointFields = fieldLineageDataset.getOutgoingSummary(new EndPointField(source, "offset"), 0, 10001);
      Assert.assertEquals(expectedEndPointField, actualEndPointFields.iterator().next());

      expectedEndPointField = new EndPointField(destination, "name");
      actualEndPointFields = fieldLineageDataset.getOutgoingSummary(new EndPointField(source, "body"), 0, 10001);
      Assert.assertEquals(expectedEndPointField, actualEndPointFields.iterator().next());

      // no outgoing summary should exist for the field file_name at time 10001
      actualEndPointFields = fieldLineageDataset.getOutgoingSummary(new EndPointField(source, "file_name"), 0, 10001);
      Assert.assertEquals(Collections.EMPTY_SET, actualEndPointFields);

      // no outgoing summary should exist for the field file_name at end time time 11000 since end time is exclusive
      actualEndPointFields = fieldLineageDataset.getOutgoingSummary(new EndPointField(source, "file_name"), 0, 11000);
      Assert.assertEquals(Collections.EMPTY_SET, actualEndPointFields);

      // outgoing summary should exist for file_name at 11001, since the corresponding run executed at 11000
      expectedEndPointField = new EndPointField(destination, "file_name");
      actualEndPointFields = fieldLineageDataset.getOutgoingSummary(new EndPointField(source, "file_name"), 0, 11001);
      Assert.assertEquals(expectedEndPointField, actualEndPointFields.iterator().next());

      Set<ProgramRunOperations> incomingOperations = fieldLineageDataset.getIncomingOperations(destination, 0, 10001);
      Set<ProgramRunOperations> outgoingOperations = fieldLineageDataset.getOutgoingOperations(source, 0, 10001);
      Assert.assertEquals(1, incomingOperations.size());
      Assert.assertEquals(incomingOperations, outgoingOperations);

      ProgramRunOperations programRunOperations = incomingOperations.iterator().next();

      Assert.assertEquals(Collections.singleton(programRun1), programRunOperations.getProgramRunIds());

      // test with bigger time range for incoming and outgoing operations
      incomingOperations = fieldLineageDataset.getIncomingOperations(destination, 10000, 12001);
      outgoingOperations = fieldLineageDataset.getOutgoingOperations(source, 10000, 12001);

      Assert.assertEquals(2, incomingOperations.size());
      Assert.assertEquals(incomingOperations, outgoingOperations);

      Set<ProgramRunOperations> expectedSet = new HashSet<>();
      expectedSet.add(new ProgramRunOperations(Collections.singleton(programRun1), info1.getOperations()));
      expectedSet.add(new ProgramRunOperations(new HashSet<>(Arrays.asList(programRun2, programRun3)),
                                               info2.getOperations()));

      Assert.assertEquals(expectedSet, incomingOperations);
      Assert.assertEquals(expectedSet, outgoingOperations);
    });
  }

  @Test
  public void testMergeSummaries() throws Exception {
    final FieldLineageDataset fieldLineageDataset = getFieldLineageDataset("testMergeDataset");
    Assert.assertNotNull(fieldLineageDataset);
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) fieldLineageDataset);

    RunId runId = RunIds.generate(10000);
    ProgramId program = new ProgramId("default", "app1", ProgramType.WORKFLOW, "workflow1");
    final ProgramRunId programRun1 = program.run(runId.getId());

    runId = RunIds.generate(11000);
    program = new ProgramId("default", "app1", ProgramType.WORKFLOW, "workflow1");
    final ProgramRunId programRun2 = program.run(runId.getId());

    List<Operation> operations = new ArrayList<>();
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("ns1", "endpoint1"), "offset", "body");
    WriteOperation write = new WriteOperation("write", "some write", EndPoint.of("ns", "endpoint3"),
            InputField.of("read", "body"));

    operations.add(read);
    operations.add(write);
    final FieldLineageInfo info1 = new FieldLineageInfo(operations);

    ReadOperation anotherRead = new ReadOperation("anotherRead", "another read", EndPoint.of("ns1", "endpoint2"),
            "offset", "body");
    WriteOperation anotherWrite = new WriteOperation("anotherWrite", "another write", EndPoint.of("ns", "endpoint3"),
            InputField.of("anotherRead", "body"));
    operations.add(anotherRead);
    operations.add(anotherWrite);
    final FieldLineageInfo info2 = new FieldLineageInfo(operations);

    txnl.execute(() -> fieldLineageDataset.addFieldLineageInfo(programRun1, info1));
    txnl.execute(() -> fieldLineageDataset.addFieldLineageInfo(programRun2, info2));

    txnl.execute(() -> {
      EndPoint source1 = EndPoint.of("ns1", "endpoint1");
      EndPoint source2 = EndPoint.of("ns1", "endpoint2");
      EndPoint destination = EndPoint.of("ns", "endpoint3");

      Set<EndPointField> expected = new HashSet<>();
      expected.add(new EndPointField(source1, "body"));
      expected.add(new EndPointField(source2, "body"));
      Set<EndPointField> actualEndPointFields
              = fieldLineageDataset.getIncomingSummary(new EndPointField(destination, "body"), 0, 11001);
      Assert.assertEquals(expected, actualEndPointFields);
    });
  }

  private static FieldLineageDataset getFieldLineageDataset(String instanceId) throws Exception {
    DatasetId id = DatasetFrameworkTestUtil.NAMESPACE_ID.dataset(instanceId);
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), id, FieldLineageDataset.class.getName(),
                                           DatasetProperties.EMPTY, null);
  }

  private List<Operation> generateOperations(boolean addAditionalField) {
    // read: file -> (offset, body)
    // parse: (body) -> (first_name, last_name)
    // concat: (first_name, last_name) -> (name)
    // write: (offset, name) -> another_file

    List<String> readOutput = new ArrayList<>();
    readOutput.add("offset");
    readOutput.add("body");
    if (addAditionalField) {
      readOutput.add("file_name");
    }
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("ns1", "endpoint1"), readOutput);

    TransformOperation parse = new TransformOperation("parse", "parsing body",
            Collections.singletonList(InputField.of("read", "body")),
            "first_name", "last_name");

    TransformOperation concat = new TransformOperation("concat", "concatinating the fields",
            Arrays.asList(InputField.of("parse", "first_name"),
                    InputField.of("parse", "last_name")), "name");

    List<InputField> writeInput = new ArrayList<>();
    writeInput.add(InputField.of("read", "offset"));
    writeInput.add(InputField.of("concat", "name"));

    if (addAditionalField) {
      writeInput.add(InputField.of("read", "file_name"));
    }

    WriteOperation write = new WriteOperation("write_op", "writing data to file", EndPoint.of("myns", "another_file"),
                                              writeInput);

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(concat);
    operations.add(read);
    operations.add(write);

    return operations;
  }
}
