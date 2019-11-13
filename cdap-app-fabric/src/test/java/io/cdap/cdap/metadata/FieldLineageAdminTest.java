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

package io.cdap.cdap.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.InputField;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.TransformOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.lineage.field.EndPointField;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.metadata.lineage.DatasetField;
import io.cdap.cdap.proto.metadata.lineage.Field;
import io.cdap.cdap.proto.metadata.lineage.FieldLineageDetails;
import io.cdap.cdap.proto.metadata.lineage.FieldLineageSummary;
import io.cdap.cdap.proto.metadata.lineage.FieldOperationInfo;
import io.cdap.cdap.proto.metadata.lineage.FieldOperationInput;
import io.cdap.cdap.proto.metadata.lineage.FieldOperationOutput;
import io.cdap.cdap.proto.metadata.lineage.ProgramFieldOperationInfo;
import io.cdap.cdap.proto.metadata.lineage.ProgramInfo;
import io.cdap.cdap.proto.metadata.lineage.ProgramRunOperations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Test for {@link FieldLineageAdmin}.
 */
public class FieldLineageAdminTest extends AppFabricTestBase {

  private static MetadataAdmin metadataAdmin;
  private static DatasetFramework datasetFramework;

  @Before
  public void setUp() {
    metadataAdmin = getInjector().getInstance(MetadataAdmin.class);
    datasetFramework = getInjector().getInstance(DatasetFramework.class);
  }

  @Test
  public void testFields() throws Exception {
    FieldLineageAdmin fieldLineageAdmin = new FieldLineageAdmin(new FakeFieldLineageReader(getFieldNames(),
                                                                                           Collections.emptySet(),
                                                                                           Collections.emptySet()),
                                                                metadataAdmin);
    EndPoint endPoint = EndPoint.of("ns", "file");

    // test all fields
    Assert.assertEquals(getFields(getFieldNames()), fieldLineageAdmin.getFields(endPoint, 0, Long.MAX_VALUE, null,
                                                                                false));

    // test fields prefixed with string "add"
    Assert.assertEquals(new HashSet<>(Arrays.asList(new Field("address", true), new Field("address_original", true))),
                        fieldLineageAdmin.getFields(endPoint, 0, Long.MAX_VALUE, "add", false));
  }

  @Test
  public void testFieldsWithDsSchema() throws Exception {
    FieldLineageAdmin fieldLineageAdmin = new FieldLineageAdmin(new FakeFieldLineageReader(getFieldNames(),
                                                                                           Collections.emptySet(),
                                                                                           Collections.emptySet()),
                                                                metadataAdmin);
    EndPoint endPoint = EndPoint.of(NamespaceId.DEFAULT.getNamespace(), "file");

    // test that when there is no schema information present for the dataset and the we request for lineage with
    // includeCurrent set to true we get lineage fields correctly.
    Set<Field> expected = getFields(getFieldNames());
    // includeCurrent set to true
    Set<Field> actual = fieldLineageAdmin.getFields(endPoint, 0, Long.MAX_VALUE, null, true);
    Assert.assertEquals(expected, actual);

    // schema with fields which are different than known to lineage store
    Schema schema =
      Schema.recordOf("record",
                      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("address", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("addiffField1", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("diffField2", Schema.nullableOf(Schema.of(Schema.Type.INT)))
      );

    // add the the dataset with the schema with fields known in lineage store
    TableProperties.Builder props = TableProperties.builder();
    TableProperties.setSchema(props, schema);
    TableProperties.setRowFieldName(props, "name");
    DatasetId datasetId = NamespaceId.DEFAULT.dataset("file");
    MetadataEntity entity = datasetId.toMetadataEntity();
    datasetFramework.addInstance("table", datasetId, props.build());

    // wait until the metadata for this dataset has been stored
    Tasks.waitFor(false, () -> metadataAdmin.getProperties(MetadataScope.SYSTEM, entity).isEmpty(),
                  5, TimeUnit.SECONDS);

    // test all fields expected should have all the fields which was known the lineage store but should not contains
    // any dataset schema field since the includeCurrent is set to false
    expected = getFields(getFieldNames());
    actual = fieldLineageAdmin.getFields(endPoint, 0, Long.MAX_VALUE, null, false);
    Assert.assertEquals(expected, actual);

    // test all fields expected should have all the fields which was known the lineage store and also the fields
    // which were only present in the dataset schema since includeCurrent is set to true.
    // this also test that for the fields which are common in lineage store and dataset schema for example address in
    // this case has their lineage info field set to true as we do have lineage for this field
    expected = getFields(getFieldNames());
    expected.addAll(new HashSet<>(Arrays.asList(new Field("addiffField1", false),
                                                new Field("diffField2", false))));
    actual = fieldLineageAdmin.getFields(endPoint, 0, Long.MAX_VALUE, null, true);
    Assert.assertEquals(expected, actual);

    // test fields prefixed with string "add" when includeCurrent not set then the ds field show not show up
    Assert.assertEquals(new HashSet<>(Arrays.asList(new Field("address", true),
                                                    new Field("address_original", true))),
                        fieldLineageAdmin.getFields(endPoint, 0, Long.MAX_VALUE, "add", false));

    // test fields prefixed with string "add" when includeCurrent is set the ds field should also show up
    Assert.assertEquals(new HashSet<>(Arrays.asList(new Field("address", true),
                                                    new Field("address_original", true),
                                                    new Field("addiffField1", false))),
                        fieldLineageAdmin.getFields(endPoint, 0, Long.MAX_VALUE, "add", true));

    // test fields prefixed with string "ADD" (case insensitive)
    Assert.assertEquals(new HashSet<>(Arrays.asList(new Field("address", true),
                                                    new Field("address_original", true),
                                                    new Field("addiffField1", false))),
                        fieldLineageAdmin.getFields(endPoint, 0, Long.MAX_VALUE, "ADD", true));
  }

  @Test
  public void testSummary() {
    FieldLineageAdmin fieldLineageAdmin = new FieldLineageAdmin(new FakeFieldLineageReader(Collections.emptySet(),
                                                                                           summary(),
                                                                                           Collections.emptySet()),
                                                                metadataAdmin);
    EndPoint endPoint = EndPoint.of("ns", "file");

    DatasetField datasetField = new DatasetField(new DatasetId("ns", "file"),
                                                 new HashSet<>(Arrays.asList("a", "b", "c")));

    DatasetField anotherDatasetField = new DatasetField(new DatasetId("ns", "anotherfile"),
                                                        new HashSet<>(Arrays.asList("x", "y", "z")));

    Set<DatasetField> expected = new HashSet<>();
    expected.add(datasetField);
    expected.add(anotherDatasetField);

    // input args to the getFieldLineage below does not matter since data returned is mocked
    FieldLineageSummary summary = fieldLineageAdmin.getFieldLineage(Constants.FieldLineage.Direction.INCOMING,
                                                                    new EndPointField(endPoint, "somefield"), 0,
                                                                    Long.MAX_VALUE);

    Assert.assertEquals(expected, summary.getIncoming());
    Assert.assertNull(summary.getOutgoing());

    summary = fieldLineageAdmin.getFieldLineage(Constants.FieldLineage.Direction.OUTGOING,
                                                new EndPointField(endPoint, "somefield"), 0, Long.MAX_VALUE);

    Assert.assertEquals(expected, summary.getOutgoing());
    Assert.assertNull(summary.getIncoming());

    summary = fieldLineageAdmin.getFieldLineage(Constants.FieldLineage.Direction.BOTH,
                                                new EndPointField(endPoint, "somefield"), 0, Long.MAX_VALUE);

    Assert.assertEquals(expected, summary.getOutgoing());
    Assert.assertEquals(expected, summary.getIncoming());
  }

  @Test
  public void testDatasetFieldLineageSummary() throws Exception {
    // the dataset fields
    Set<String> fields = ImmutableSet.of("field1", "field2", "field3");
    ImmutableMap.Builder<EndPoint, Set<String>> allFields = ImmutableMap.builder();

    /*
      Incoming fields
      src1: src1f1 -> field1
            src1f2 -> field1
      src2: src2f1 -> field1

      src2: src2f2 -> field2
      src3: src3f1 -> field2

      src3: src3f2 -> field3
     */
    EndPoint src1 = EndPoint.of("ns1", "src1");
    EndPoint src2 = EndPoint.of("ns1", "src2");
    EndPoint src3 = EndPoint.of("ns1", "src3");
    Map<String, Set<EndPointField>> incomings =
      ImmutableMap.of("field1",
                      ImmutableSet.of(new EndPointField(src1, "src1f1"),
                                      new EndPointField(src1, "src1f2"),
                                      new EndPointField(src2, "src2f1")),
                      "field2",
                      ImmutableSet.of(new EndPointField(src2, "src2f2"),
                                      new EndPointField(src3, "src3f1")),
                      "field3",
                      ImmutableSet.of(new EndPointField(src3, "src3f2")));
    allFields.put(src1, ImmutableSet.of("src1f1", "src1f2", "src1f3"));
    allFields.put(src2, ImmutableSet.of("src2f1", "src2f2"));
    allFields.put(src3, ImmutableSet.of("src3f1", "src3f2"));

    /*
      Outgoing fields
      dest1: field1 -> dest1f1
      dest2: field1 -> dest2f1

      dest1: field2 -> dest1f2
      dest2: field2 -> dest2f1

      dest2: field3 -> dest2f2
     */
    EndPoint dest1 = EndPoint.of("ns1", "dest1");
    EndPoint dest2 = EndPoint.of("ns1", "dest2");
    Map<String, Set<EndPointField>> outgoings =
      ImmutableMap.of("field1",
                      ImmutableSet.of(new EndPointField(dest1, "dest1f1"),
                                      new EndPointField(dest2, "dest2f1")),
                      "field2",
                      ImmutableSet.of(new EndPointField(dest1, "dest1f2"),
                                      new EndPointField(dest2, "dest2f1")),
                      "field3",
                      ImmutableSet.of(new EndPointField(dest2, "dest2f2")));
    allFields.put(dest1, ImmutableSet.of("dest1f1", "dest1f2", "dest1f3", "dest1f4"));
    allFields.put(dest2, ImmutableSet.of("dest2f1", "dest2f2"));


    FieldLineageAdmin fieldLineageAdmin = new FieldLineageAdmin(new FakeFieldLineageReader(fields,
                                                                                           Collections.emptySet(),
                                                                                           incomings, outgoings,
                                                                                           Collections.emptySet(),
                                                                                           allFields.build()),
                                                                metadataAdmin);
    // input dataset name does not matter since we use a mocked reader
    DatasetFieldLineageSummary summary =
      fieldLineageAdmin.getDatasetFieldLineage(Constants.FieldLineage.Direction.BOTH, EndPoint.of("ns1", "ds1"),
                                               0L, Long.MAX_VALUE);

    Assert.assertEquals(Constants.FieldLineage.Direction.BOTH, summary.getDirection());
    Assert.assertEquals(0L, summary.getStartTs());
    Assert.assertEquals(Long.MAX_VALUE, summary.getEndTs());
    Assert.assertEquals(fields, summary.getFields());
    Assert.assertEquals(new DatasetId("ns1", "ds1"), summary.getDatasetId());

    Set<DatasetFieldLineageSummary.FieldLineageRelations> expectedIncomings = ImmutableSet.of(
      new DatasetFieldLineageSummary.FieldLineageRelations(new DatasetId("ns1", "src1"), 3,
                                                           ImmutableSet.of(new FieldRelation("src1f1", "field1"),
                                                                           new FieldRelation("src1f2", "field1"))),
      new DatasetFieldLineageSummary.FieldLineageRelations(new DatasetId("ns1", "src2"), 2,
                                                           ImmutableSet.of(new FieldRelation("src2f1", "field1"),
                                                new FieldRelation("src2f2", "field2"))),
      new DatasetFieldLineageSummary.FieldLineageRelations(new DatasetId("ns1", "src3"), 2,
                                                           ImmutableSet.of(new FieldRelation("src3f1", "field2"),
                                                new FieldRelation("src3f2", "field3"))));
    Assert.assertEquals(expectedIncomings, summary.getIncoming());

    Set<DatasetFieldLineageSummary.FieldLineageRelations> expectedOutgoings = ImmutableSet.of(
      new DatasetFieldLineageSummary.FieldLineageRelations(new DatasetId("ns1", "dest1"), 4,
                                                           ImmutableSet.of(new FieldRelation("field1", "dest1f1"),
                                                new FieldRelation("field2", "dest1f2"))),
      new DatasetFieldLineageSummary.FieldLineageRelations(new DatasetId("ns1", "dest2"), 2,
                                                           ImmutableSet.of(new FieldRelation("field1", "dest2f1"),
                                                                           new FieldRelation("field2", "dest2f1"),
                                                                           new FieldRelation("field3", "dest2f2"))));
    Assert.assertEquals(expectedOutgoings, summary.getOutgoing());
  }

  @Test
  public void testOperations() {
    FieldLineageAdmin fieldLineageAdmin = new FieldLineageAdmin(new FakeFieldLineageReader(Collections.emptySet(),
                                                                                           Collections.emptySet(),
                                                                                           operations()),
                                                                metadataAdmin);
    EndPoint endPoint = EndPoint.of("ns", "file");

    // input args to the getOperationDetails below does not matter since data returned is mocked
    FieldLineageDetails operationDetails =
      fieldLineageAdmin.getOperationDetails(Constants.FieldLineage.Direction.INCOMING,
                                            new EndPointField(endPoint, "somefield"), 0, Long.MAX_VALUE);

    ProgramId program1 = new ProgramId("ns", "app", ProgramType.SPARK, "sparkprogram");
    ProgramId program2 = new ProgramId("ns", "app", ProgramType.MAPREDUCE, "mrprogram");
    ProgramRunId program1Run1 = program1.run(RunIds.generate(1000));
    ProgramRunId program1Run2 = program1.run(RunIds.generate(2000));
    ProgramRunId program1Run3 = program1.run(RunIds.generate(3000));
    ProgramRunId program1Run4 = program1.run(RunIds.generate(5000));
    ProgramRunId program2Run1 = program2.run(RunIds.generate(4000));
    ProgramRunId program2Run2 = program2.run(RunIds.generate(6000));

    List<ProgramFieldOperationInfo> incomings = operationDetails.getIncoming();

    Set<ProgramFieldOperationInfo> expectedInfos = new HashSet<>();

    List<ProgramInfo> programInfos = new ArrayList<>();
    // program1Run1 and program1Run2 both generated same set of operations, however only the latest
    // run will be included in the returned list. None of the run of program2 generated these set of operations.
    programInfos.add(new ProgramInfo(program1, RunIds.getTime(program1Run2.getRun(), TimeUnit.SECONDS)));

    EndPoint endPoint1 = EndPoint.of("ns", "file");
    EndPoint endPoint2 = EndPoint.of("ns", "anotherfile");

    List<FieldOperationInfo> fieldOperationInfos = new ArrayList<>();
    // Return list should have topologically sorted operations
    fieldOperationInfos.add(new FieldOperationInfo("read", "reading file", FieldOperationInput.of(endPoint1),
                                                   FieldOperationOutput.of(Arrays.asList("offset", "body"))));

    List<InputField> inputFields = new ArrayList<>();
    inputFields.add(InputField.of("read", "offset"));
    inputFields.add(InputField.of("parse", "name"));
    inputFields.add(InputField.of("parse", "address"));
    inputFields.add(InputField.of("parse", "zip"));

    fieldOperationInfos.add(new FieldOperationInfo("write", "writing file", FieldOperationInput.of(inputFields),
                                                   FieldOperationOutput.of(endPoint2)));

    expectedInfos.add(new ProgramFieldOperationInfo(programInfos, fieldOperationInfos));

    programInfos = new ArrayList<>();
    // program1 and program2 both generated the next set of operations, returned list will contain the
    // only one latest run of each program and that too sorted by the last execution time.
    programInfos.add(new ProgramInfo(program2, RunIds.getTime(program2Run2.getRun(), TimeUnit.SECONDS)));
    programInfos.add(new ProgramInfo(program1, RunIds.getTime(program1Run4.getRun(), TimeUnit.SECONDS)));

    fieldOperationInfos = new ArrayList<>();
    fieldOperationInfos.add(new FieldOperationInfo("read", "reading file", FieldOperationInput.of(endPoint1),
                                                   FieldOperationOutput.of(Arrays.asList("offset", "body"))));

    FieldOperationInput input = FieldOperationInput.of(Collections.singletonList(InputField.of("read", "offset")));
    FieldOperationOutput output = FieldOperationOutput.of(Collections.singletonList("offset"));
    fieldOperationInfos.add(new FieldOperationInfo("normalize", "normalizing offset", input, output));

    inputFields = new ArrayList<>();
    inputFields.add(InputField.of("normalize", "offset"));
    inputFields.add(InputField.of("parse", "name"));
    inputFields.add(InputField.of("parse", "address"));
    inputFields.add(InputField.of("parse", "zip"));

    input = FieldOperationInput.of(inputFields);
    output = FieldOperationOutput.of(endPoint2);
    fieldOperationInfos.add(new FieldOperationInfo("write", "writing file", input, output));

    expectedInfos.add(new ProgramFieldOperationInfo(programInfos, fieldOperationInfos));
    Assert.assertNotNull(incomings);
    // converting to set because ordering in different versions of operations is not guaranteed
    Assert.assertEquals(expectedInfos, new HashSet<>(incomings));
  }

  private Set<Field> getFields(Set<String> fieldNames) {
    return fieldNames.stream().map(fieldName -> new Field(fieldName, true)).collect(Collectors.toSet());
  }

  private Set<String> getFieldNames() {
    return new HashSet<>(Arrays.asList("name", "address", "address_original", "offset", "body"));
  }

  private Set<EndPointField> summary() {
    Set<EndPointField> endPointFields = new HashSet<>();
    EndPoint endPoint1 = EndPoint.of("ns", "file");
    EndPoint endPoint2 = EndPoint.of("ns", "anotherfile");
    endPointFields.add(new EndPointField(endPoint1, "a"));
    endPointFields.add(new EndPointField(endPoint1, "b"));
    endPointFields.add(new EndPointField(endPoint1, "c"));
    endPointFields.add(new EndPointField(endPoint2, "x"));
    endPointFields.add(new EndPointField(endPoint2, "y"));
    endPointFields.add(new EndPointField(endPoint2, "z"));
    return endPointFields;
  }

  private Set<ProgramRunOperations> operations() {
    ProgramId program1 = new ProgramId("ns", "app", ProgramType.SPARK, "sparkprogram");
    ProgramId program2 = new ProgramId("ns", "app", ProgramType.MAPREDUCE, "mrprogram");

    EndPoint endPoint1 = EndPoint.of("ns", "file");
    EndPoint endPoint2 = EndPoint.of("ns", "anotherfile");

    ReadOperation read = new ReadOperation("read", "reading file", endPoint1, "offset", "body");
    WriteOperation write = new WriteOperation("write", "writing file", endPoint2, InputField.of("read", "offset"),
                                              InputField.of("parse", "name"), InputField.of("parse", "address"),
                                              InputField.of("parse", "zip"));

    ProgramRunId program1Run1 = program1.run(RunIds.generate(1000));
    ProgramRunId program1Run2 = program1.run(RunIds.generate(2000));
    Set<ProgramRunOperations> programRunOperations = new HashSet<>();
    programRunOperations.add(new ProgramRunOperations(new HashSet<>(Arrays.asList(program1Run1, program1Run2)),
                                                      new HashSet<>(Arrays.asList(read, write))));

    TransformOperation normalize = new TransformOperation("normalize", "normalizing offset",
                                                          Collections.singletonList(InputField.of("read", "offset")),
                                                          "offset");

    write = new WriteOperation("write", "writing file", endPoint2, InputField.of("normalize", "offset"),
                               InputField.of("parse", "name"), InputField.of("parse", "address"),
                               InputField.of("parse", "zip"));

    ProgramRunId program1Run3 = program1.run(RunIds.generate(3000));
    ProgramRunId program1Run4 = program1.run(RunIds.generate(5000));
    ProgramRunId program2Run1 = program2.run(RunIds.generate(4000));
    ProgramRunId program2Run2 = program2.run(RunIds.generate(6000));

    Set<ProgramRunId> programRunIds = new HashSet<>(Arrays.asList(program1Run3, program1Run4, program2Run1,
                                                                  program2Run2));
    Set<Operation> operations = new HashSet<>(Arrays.asList(read, normalize, write));
    programRunOperations.add(new ProgramRunOperations(programRunIds, operations));
    return programRunOperations;
  }
}
