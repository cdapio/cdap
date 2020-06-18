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

package io.cdap.cdap.data2.metadata.lineage.field;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.InputField;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.TransformOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.codec.OperationTypeAdapter;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test for {@link FieldLineageInfo}
 */
public class FieldLineageInfoTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
    .create();

  @Test
  public void testDirectReadWrite() {
    List<Operation> operations = new ArrayList<>();

    ReadOperation read = new ReadOperation("read", "", EndPoint.of("ns1", "endpoint1"), "id", "name");
    ReadOperation anotherRead = new ReadOperation("anotherRead", "", EndPoint.of("ns1", "endpoint2"),
                                                  "id1", "name1");
    WriteOperation write = new WriteOperation("write", "", EndPoint.of("ns1", "endpoint3"),
                                              InputField.of("read", "id"), InputField.of("read", "name"),
                                              InputField.of("anotherRead", "id1"),
                                              InputField.of("anotherRead", "name1"));
    operations.add(read);
    operations.add(write);
    operations.add(anotherRead);

    FieldLineageInfo info = new FieldLineageInfo(operations);
    Map<EndPointField, Set<EndPointField>> incoming = info.getIncomingSummary();
    Map<EndPointField, Set<EndPointField>> expected = ImmutableMap.of(
      new EndPointField(EndPoint.of("ns1", "endpoint3"), "id"),
      Collections.singleton(new EndPointField(EndPoint.of("ns1", "endpoint1"), "id")),
      new EndPointField(EndPoint.of("ns1", "endpoint3"), "id1"),
      Collections.singleton(new EndPointField(EndPoint.of("ns1", "endpoint2"), "id1")),
      new EndPointField(EndPoint.of("ns1", "endpoint3"), "name"),
      Collections.singleton(new EndPointField(EndPoint.of("ns1", "endpoint1"), "name")),
      new EndPointField(EndPoint.of("ns1", "endpoint3"), "name1"),
      Collections.singleton(new EndPointField(EndPoint.of("ns1", "endpoint2"), "name1")));
    Assert.assertEquals(expected, incoming);

    Map<EndPointField, Set<EndPointField>> outgoing = info.getOutgoingSummary();
    expected = ImmutableMap.of(
      new EndPointField(EndPoint.of("ns1", "endpoint1"), "id"),
      Collections.singleton(new EndPointField(EndPoint.of("ns1", "endpoint3"), "id")),
      new EndPointField(EndPoint.of("ns1", "endpoint2"), "id1"),
      Collections.singleton(new EndPointField(EndPoint.of("ns1", "endpoint3"), "id1")),
      new EndPointField(EndPoint.of("ns1", "endpoint1"), "name"),
      Collections.singleton(new EndPointField(EndPoint.of("ns1", "endpoint3"), "name")),
      new EndPointField(EndPoint.of("ns1", "endpoint2"), "name1"),
      Collections.singleton(new EndPointField(EndPoint.of("ns1", "endpoint3"), "name1")));
    Assert.assertEquals(expected, outgoing);
  }

  @Test
  public void testWriteToSameEndpoint() {
    List<Operation> operations = new ArrayList<>();
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("ns1", "endpoint1"), "offset", "body");
    WriteOperation write = new WriteOperation("write", "some write", EndPoint.of("ns", "endpoint3"),
                                              InputField.of("read", "body"));

    operations.add(read);
    operations.add(write);

    ReadOperation anotherRead = new ReadOperation("anotherRead", "another read", EndPoint.of("ns1", "endpoint2"),
                                                  "offset", "body");
    // this write is writing to field body in same endpoint
    WriteOperation anotherWrite = new WriteOperation("anotherWrite", "another write", EndPoint.of("ns", "endpoint3"),
                                                     InputField.of("anotherRead", "body"));
    operations.add(anotherRead);
    operations.add(anotherWrite);
    FieldLineageInfo info = new FieldLineageInfo(operations);

    Map<EndPointField, Set<EndPointField>> incoming = info.getIncomingSummary();
    Map<EndPointField, Set<EndPointField>> expected = Collections.singletonMap(
      new EndPointField(EndPoint.of("ns", "endpoint3"), "body"),
      ImmutableSet.of(new EndPointField(EndPoint.of("ns1", "endpoint1"), "body"),
                      new EndPointField(EndPoint.of("ns1", "endpoint2"), "body")));
    Assert.assertEquals(expected, incoming);

    Map<EndPointField, Set<EndPointField>> outgoing = info.getOutgoingSummary();
    expected = ImmutableMap.of(
      new EndPointField(EndPoint.of("ns1", "endpoint1"), "body"),
      Collections.singleton(new EndPointField(EndPoint.of("ns", "endpoint3"), "body")),
      new EndPointField(EndPoint.of("ns1", "endpoint2"), "body"),
      Collections.singleton(new EndPointField(EndPoint.of("ns", "endpoint3"), "body")));
    Assert.assertEquals(expected, outgoing);
  }

  @Test(timeout = 10000)
  public void testLargeLineageOperation() {
    List<String> inputs = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      inputs.add("num" + i);
    }

    List<Operation> operations = new ArrayList<>();
    operations.add(new ReadOperation("read", "Read from something", EndPoint.of("start"), inputs));

    // generate 500+ operations with 5 identity + all-to-all combos
    generateLineage(inputs, operations, "first identity", "read", "alltoall1");
    generateLineage(inputs, operations, "second identity", "alltoall1", "alltoall2");
    generateLineage(inputs, operations, "third identity", "alltoall2", "alltoall3");
    generateLineage(inputs, operations, "forth identity", "alltoall3", "alltoall4");
    generateLineage(inputs, operations, "fifth identity", "alltoall4", "alltoall5");

    List<InputField> newList = new ArrayList<>();
    inputs.forEach(s -> newList.add(InputField.of("alltoall5", s)));
    WriteOperation operation = new WriteOperation("Write", "", EndPoint.of("dest"), newList);
    operations.add(operation);

    FieldLineageInfo info = new FieldLineageInfo(operations);

    Assert.assertNotNull(info);
    Set<EndPointField> relatedSources = new HashSet<>();
    Map<EndPointField, Set<EndPointField>> expectedIncoming = new HashMap<>();
    for (int i = 0; i < inputs.size(); i++) {
      relatedSources.add(new EndPointField(EndPoint.of("start"), "num" + i));
    }
    for (int i = 0; i < inputs.size(); i++) {
      EndPointField key = new EndPointField(EndPoint.of("dest"), "num" + i);
      expectedIncoming.put(key, relatedSources);
    }
    Assert.assertEquals(expectedIncoming, info.getIncomingSummary());
  }

  @Test
  public void testInvalidOperations() {
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "name", "address");
    WriteOperation write = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint2"),
            Arrays.asList(InputField.of("read", "offset"),
                    InputField.of("parse", "name"),
                    InputField.of("parse", "body")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(write);

    try {
      // Create info without read operation
      FieldLineageInfo info = new FieldLineageInfo(operations);
      Assert.fail("Field lineage info creation should fail since no read operation is specified.");
    } catch (IllegalArgumentException e) {
      String msg = "Field level lineage requires at least one operation of type 'READ'.";
      Assert.assertEquals(msg, e.getMessage());
    }

    operations.clear();

    operations.add(read);
    operations.add(parse);

    try {
      // Create info without write operation
      FieldLineageInfo info = new FieldLineageInfo(operations);
      Assert.fail("Field lineage info creation should fail since no write operation is specified.");
    } catch (IllegalArgumentException e) {
      String msg = "Field level lineage requires at least one operation of type 'WRITE'.";
      Assert.assertEquals(msg, e.getMessage());
    }

    WriteOperation duplicateWrite = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint3"),
                                                       Arrays.asList(InputField.of("read", "offset"),
                                                                     InputField.of("parse", "name"),
                                                                     InputField.of("parse", "body")));

    operations.add(write);
    operations.add(duplicateWrite);

    try {
      // Create info with non-unique operation names
      FieldLineageInfo info = new FieldLineageInfo(operations);
      Assert.fail("Field lineage info creation should fail since operation name 'write' is repeated.");
    } catch (IllegalArgumentException e) {
      String msg = "Operation name 'write' is repeated";
      Assert.assertTrue(e.getMessage().contains(msg));
    }

    operations.clear();

    TransformOperation invalidOrigin = new TransformOperation("anotherparse", "parse body",
                                                              Arrays.asList(InputField.of("invalid", "body"),
                                                                            InputField.of("anotherinvalid", "body")),
                                                              "name", "address");

    operations.add(read);
    operations.add(parse);
    operations.add(write);
    operations.add(invalidOrigin);

    try {
      // Create info without invalid origins
      FieldLineageInfo info = new FieldLineageInfo(operations);
      Assert.fail("Field lineage info creation should fail since operation with name 'invalid' " +
              "and 'anotherinvalid' do not exist.");
    } catch (IllegalArgumentException e) {
      String msg = "No operation is associated with the origins '[invalid, anotherinvalid]'.";
      Assert.assertEquals(msg, e.getMessage());
    }
  }

  @Test
  public void testValidOperations() {
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "name", "address");
    WriteOperation write = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint2"),
                                              Arrays.asList(InputField.of("read", "offset"),
                                                            InputField.of("parse", "name"),
                                                            InputField.of("parse", "body")));

    List<Operation> operations = new ArrayList<>();
    operations.add(read);
    operations.add(write);
    operations.add(parse);
    FieldLineageInfo info1 = new FieldLineageInfo(operations);

    // Serializing and deserializing should result in the same checksum.
    String operationsJson = GSON.toJson(info1.getOperations());
    Type setType = new TypeToken<Set<Operation>>() { }.getType();
    Set<Operation> operationsFromJson = GSON.fromJson(operationsJson, setType);
    FieldLineageInfo info2 = new FieldLineageInfo(operationsFromJson);
    Assert.assertEquals(info1, info2);

    // Create lineage info with different ordering of same operations. Checksum should still be same.
    operations.clear();
    operations.add(write);
    operations.add(parse);
    operations.add(read);

    FieldLineageInfo info3 = new FieldLineageInfo(operations);
    Assert.assertEquals(info1, info3);

    // Change the namespace name of the write operation from ns to myns. The checksum should change now.
    operations.clear();

    WriteOperation anotherWrite = new WriteOperation("write", "write data", EndPoint.of("myns", "endpoint2"),
                                                     Arrays.asList(InputField.of("read", "offset"),
                                                                   InputField.of("parse", "name"),
                                                                   InputField.of("parse", "body")));
    operations.add(anotherWrite);
    operations.add(parse);
    operations.add(read);
    FieldLineageInfo info4 = new FieldLineageInfo(operations);
    Assert.assertNotEquals(info1, info4);
  }

  @Test
  public void testSimpleFieldLineageSummary() {
    // read: file -> (offset, body)
    // parse: (body) -> (first_name, last_name)
    // concat: (first_name, last_name) -> (name)
    // write: (offset, name) -> another_file

    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");

    TransformOperation parse = new TransformOperation("parse", "parsing body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "first_name", "last_name");

    TransformOperation concat = new TransformOperation("concat", "concatinating the fields",
                                                       Arrays.asList(InputField.of("parse", "first_name"),
                                                                     InputField.of("parse", "last_name")), "name");

    WriteOperation write = new WriteOperation("write_op", "writing data to file",
                                              EndPoint.of("myns", "another_file"),
                                              Arrays.asList(InputField.of("read", "offset"),
                                                            InputField.of("concat", "name")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(concat);
    operations.add(read);
    operations.add(write);

    FieldLineageInfo info = new FieldLineageInfo(operations);

    // EndPoint(myns, another_file) should have two fields: offset and name
    Map<EndPoint, Set<String>> destinationFields = info.getDestinationFields();
    EndPoint destination = EndPoint.of("myns", "another_file");
    Assert.assertEquals(1, destinationFields.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("offset", "name")), destinationFields.get(destination));

    Map<EndPointField, Set<EndPointField>> incomingSummary = info.getIncomingSummary();
    Map<EndPointField, Set<EndPointField>> outgoingSummary = info.getOutgoingSummary();

    // test incoming summaries

    // offset in the destination is generated from offset field read from source
    EndPointField endPointField = new EndPointField(destination, "offset");
    Set<EndPointField> sourceEndPointFields = incomingSummary.get(endPointField);
    Assert.assertEquals(1, sourceEndPointFields.size());
    EndPointField sourceEndpoint = new EndPointField(EndPoint.of("endpoint1"), "offset");
    Assert.assertEquals(sourceEndpoint, sourceEndPointFields.iterator().next());

    Set<Operation> operationsForField = info.getIncomingOperationsForField(endPointField);
    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(write);
    expectedOperations.add(read);
    Assert.assertEquals(expectedOperations, operationsForField);

    // test outgoing operations for offset field
    operationsForField = info.getOutgoingOperationsForField(sourceEndpoint);
    Assert.assertEquals(expectedOperations, operationsForField);

    // name in the destination is generated from body field read from source
    endPointField = new EndPointField(destination, "name");
    sourceEndPointFields = incomingSummary.get(endPointField);
    Assert.assertEquals(1, sourceEndPointFields.size());
    sourceEndpoint = new EndPointField(EndPoint.of("endpoint1"), "body");
    Assert.assertEquals(sourceEndpoint, sourceEndPointFields.iterator().next());

    operationsForField = info.getIncomingOperationsForField(endPointField);
    expectedOperations = new HashSet<>();
    expectedOperations.add(write);
    expectedOperations.add(concat);
    expectedOperations.add(parse);
    expectedOperations.add(read);
    Assert.assertEquals(expectedOperations, operationsForField);

    // offset in the source should only affect the field offset in the destination
    EndPoint source = EndPoint.of("endpoint1");
    endPointField = new EndPointField(source, "offset");
    Set<EndPointField> destinationEndPointFields = outgoingSummary.get(endPointField);
    Assert.assertEquals(1, destinationEndPointFields.size());
    sourceEndpoint = new EndPointField(EndPoint.of("myns", "another_file"), "offset");
    Assert.assertEquals(sourceEndpoint, destinationEndPointFields.iterator().next());

    // test outgoing operations for body field
    operationsForField = info.getOutgoingOperationsForField(new EndPointField(EndPoint.of("endpoint1"), "body"));
    Assert.assertEquals(expectedOperations, operationsForField);
  }


  @Test
  public void testSourceToMultipleDestinations() {
    // read: file -> (offset, body)
    // parse: body -> (id, name, address, zip)
    // write1: (parse.id, parse.name) -> info
    // write2: (parse.address, parse.zip) -> location

    EndPoint source = EndPoint.of("ns", "file");
    EndPoint info = EndPoint.of("ns", "info");
    EndPoint location = EndPoint.of("ns", "location");

    ReadOperation read = new ReadOperation("read", "Reading from file", source, "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parsing body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "id", "name", "address", "zip");
    WriteOperation infoWrite = new WriteOperation("infoWrite", "writing info", info,
                                                  Arrays.asList(InputField.of("parse", "id"),
                                                                InputField.of("parse", "name")));
    WriteOperation locationWrite = new WriteOperation("locationWrite", "writing location", location,
                                                      Arrays.asList(InputField.of("parse", "address"),
                                                                    InputField.of("parse", "zip")));

    List<Operation> operations = new ArrayList<>();
    operations.add(read);
    operations.add(parse);
    operations.add(infoWrite);
    operations.add(locationWrite);

    FieldLineageInfo fllInfo = new FieldLineageInfo(operations);

    Map<EndPoint, Set<String>> destinationFields = fllInfo.getDestinationFields();
    Assert.assertEquals(2, destinationFields.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("id", "name")), destinationFields.get(info));
    Assert.assertEquals(new HashSet<>(Arrays.asList("address", "zip")), destinationFields.get(location));

    Map<EndPointField, Set<EndPointField>> incomingSummary = fllInfo.getIncomingSummary();
    Assert.assertEquals(4, incomingSummary.size());
    EndPointField expected = new EndPointField(source, "body");
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(info, "id")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(info, "id")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(info, "name")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(info, "name")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(location, "address")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(location, "address")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(location, "zip")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(location, "zip")).iterator().next());

    Map<EndPointField, Set<EndPointField>> outgoingSummary = fllInfo.getOutgoingSummary();
    // Note that outgoing summary just contains 1 entry, because offset field from source
    // is not contributing to any destination field
    Assert.assertEquals(1, outgoingSummary.size());

    Set<EndPointField> expectedSet = new HashSet<>();
    expectedSet.add(new EndPointField(info, "id"));
    expectedSet.add(new EndPointField(info, "name"));
    expectedSet.add(new EndPointField(location, "address"));
    expectedSet.add(new EndPointField(location, "zip"));
    Assert.assertEquals(4, outgoingSummary.get(new EndPointField(source, "body")).size());
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(source, "body")));

    // test outgoing operations: offset field is read by the source but never processed by any operation
    EndPointField endPointField = new EndPointField(source, "offset");
    Set<Operation> operationsForField = fllInfo.getOutgoingOperationsForField(endPointField);
    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(read);
    Assert.assertEquals(expectedOperations, operationsForField);

    // body is used by other operations hence they must be in outgoing operations
    endPointField = new EndPointField(source, "body");
    operationsForField = fllInfo.getOutgoingOperationsForField(endPointField);
    expectedOperations = new HashSet<>();
    expectedOperations.add(read);
    expectedOperations.add(parse);
    expectedOperations.add(infoWrite);
    expectedOperations.add(locationWrite);
    Assert.assertEquals(expectedOperations, operationsForField);
  }

  @Test
  public void testMultiSourceSingleDestinationWithoutMerge() {
    // pRead: personFile -> (offset, body)
    // parse: body -> (id, name, address)
    // cRead: codeFile -> id
    // codeGen: (parse.id, cRead.id) -> id
    // sWrite: (codeGen.id, parse.name, parse.address) -> secureStore
    // iWrite: (parse.id, parse.name, parse.address) -> insecureStore

    EndPoint pEndPoint = EndPoint.of("ns", "personFile");
    EndPoint cEndPoint = EndPoint.of("ns", "codeFile");
    EndPoint sEndPoint = EndPoint.of("ns", "secureStore");
    EndPoint iEndPoint = EndPoint.of("ns", "insecureStore");

    ReadOperation pRead = new ReadOperation("pRead", "Reading from person file", pEndPoint, "offset", "body");

    ReadOperation cRead = new ReadOperation("cRead", "Reading from code file", cEndPoint, "id");

    TransformOperation parse = new TransformOperation("parse", "parsing body",
                                                      Collections.singletonList(InputField.of("pRead", "body")),
                                                      "id", "name", "address");

    TransformOperation codeGen = new TransformOperation("codeGen", "Generate secure code",
                                                        Arrays.asList(InputField.of("parse", "id"),
                                                                      InputField.of("cRead", "id")), "id");

    WriteOperation sWrite = new WriteOperation("sWrite", "writing secure store", sEndPoint,
                                               Arrays.asList(InputField.of("codeGen", "id"),
                                                             InputField.of("parse", "name"),
                                                             InputField.of("parse", "address")));

    WriteOperation iWrite = new WriteOperation("iWrite", "writing insecure store", iEndPoint,
                                               Arrays.asList(InputField.of("parse", "id"),
                                                             InputField.of("parse", "name"),
                                                             InputField.of("parse", "address")));

    List<Operation> operations = new ArrayList<>();
    operations.add(pRead);
    operations.add(cRead);
    operations.add(parse);
    operations.add(codeGen);
    operations.add(sWrite);
    operations.add(iWrite);

    FieldLineageInfo fllInfo = new FieldLineageInfo(operations);
    Map<EndPoint, Set<String>> destinationFields = fllInfo.getDestinationFields();
    Assert.assertEquals(new HashSet<>(Arrays.asList("id", "name", "address")), destinationFields.get(sEndPoint));
    Assert.assertEquals(new HashSet<>(Arrays.asList("id", "name", "address")), destinationFields.get(iEndPoint));
    Assert.assertNull(destinationFields.get(pEndPoint));

    Map<EndPointField, Set<EndPointField>> incomingSummary = fllInfo.getIncomingSummary();
    Assert.assertEquals(6, incomingSummary.size());
    EndPointField expected = new EndPointField(pEndPoint, "body");
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(iEndPoint, "id")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(iEndPoint, "id")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(iEndPoint, "name")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(iEndPoint, "name")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(iEndPoint, "address")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(iEndPoint, "address")).iterator().next());

    // name and address from secure endpoint also depends on the body field of pEndPoint
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(sEndPoint, "name")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(sEndPoint, "name")).iterator().next());
    Assert.assertEquals(1, incomingSummary.get(new EndPointField(sEndPoint, "address")).size());
    Assert.assertEquals(expected, incomingSummary.get(new EndPointField(sEndPoint, "address")).iterator().next());

    // id of secure endpoint depends on both body field of pEndPoint and id field of cEndPoint
    Set<EndPointField> expectedSet = new HashSet<>();
    expectedSet.add(new EndPointField(pEndPoint, "body"));
    expectedSet.add(new EndPointField(cEndPoint, "id"));
    Assert.assertEquals(expectedSet, incomingSummary.get(new EndPointField(sEndPoint, "id")));

    Map<EndPointField, Set<EndPointField>> outgoingSummary = fllInfo.getOutgoingSummary();
    // outgoing summary will not contain offset but only body from pEndPoint and id from cEndPoint
    Assert.assertEquals(2, outgoingSummary.size());

    expectedSet = new HashSet<>();
    expectedSet.add(new EndPointField(iEndPoint, "id"));
    expectedSet.add(new EndPointField(iEndPoint, "name"));
    expectedSet.add(new EndPointField(iEndPoint, "address"));
    expectedSet.add(new EndPointField(sEndPoint, "id"));
    expectedSet.add(new EndPointField(sEndPoint, "name"));
    expectedSet.add(new EndPointField(sEndPoint, "address"));
    // body affects all fields from both secure and insecure endpoints
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(pEndPoint, "body")));

    expectedSet.clear();
    expectedSet.add(new EndPointField(sEndPoint, "id"));
    // id field of cEndPoint only affects id field of secure endpoint
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(cEndPoint, "id")));

    // Test incoming operations from all destination fields
    Set<Operation> inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(iEndPoint, "id"));
    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(iWrite);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, inComingOperations);

    inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(iEndPoint, "name"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(iWrite);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(inComingOperations));

    inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(iEndPoint, "address"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(iWrite);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, inComingOperations);

    inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(sEndPoint, "id"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(sWrite);
    expectedOperations.add(codeGen);
    expectedOperations.add(cRead);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, inComingOperations);

    inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(sEndPoint, "name"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(sWrite);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, inComingOperations);

    inComingOperations = fllInfo.getIncomingOperationsForField(new EndPointField(sEndPoint, "address"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(sWrite);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, inComingOperations);

    // test outgoing operations for all source fields
    Set<Operation> outgoingOperations = fllInfo.getOutgoingOperationsForField(new EndPointField(pEndPoint, "offset"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, outgoingOperations);

    outgoingOperations = fllInfo.getOutgoingOperationsForField(new EndPointField(pEndPoint, "body"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(sWrite);
    expectedOperations.add(iWrite);
    expectedOperations.add(codeGen);
    expectedOperations.add(parse);
    expectedOperations.add(pRead);
    Assert.assertEquals(expectedOperations, outgoingOperations);

    outgoingOperations = fllInfo.getOutgoingOperationsForField(new EndPointField(cEndPoint, "id"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(sWrite);
    expectedOperations.add(codeGen);
    expectedOperations.add(cRead);
    Assert.assertEquals(expectedOperations, outgoingOperations);
  }

  @Test
  public void testMultiPathFieldLineage() {
    // read1: file1 -> (offset, body)
    // read2: file2 -> (offset, body)
    // merge: (read1.offset, read1.body, read2.offset, read2.body) -> (offset, body)
    // parse: (merge.body) -> (name,address)
    // write: (parse.name, parse.address, merge.offset) -> file

    EndPoint read1EndPoint = EndPoint.of("ns1", "file1");
    EndPoint read2EndPoint = EndPoint.of("ns2", "file2");
    EndPoint fileEndPoint = EndPoint.of("ns3", "file");

    ReadOperation read1 = new ReadOperation("read1", "Reading from file1", read1EndPoint, "offset", "body");

    ReadOperation read2 = new ReadOperation("read2", "Reading from file2", read2EndPoint, "offset", "body");

    TransformOperation merge = new TransformOperation("merge", "merging fields",
                                                      Arrays.asList(InputField.of("read1", "offset"),
                                                                    InputField.of("read2", "offset"),
                                                                    InputField.of("read1", "body"),
                                                                    InputField.of("read2", "body")), "offset", "body");

    TransformOperation parse = new TransformOperation("parse", "parsing body",
                                                      Collections.singletonList(InputField.of("merge", "body")),
                                                      "name", "address");

    WriteOperation write = new WriteOperation("write", "writing to another file", fileEndPoint,
                                              Arrays.asList(InputField.of("merge", "offset"),
                                                            InputField.of("parse", "name"),
                                                            InputField.of("parse", "address")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(merge);
    operations.add(read1);
    operations.add(read2);
    operations.add(write);
    FieldLineageInfo fllInfo = new FieldLineageInfo(operations);

    Map<EndPoint, Set<String>> destinationFields = fllInfo.getDestinationFields();
    Assert.assertEquals(1, destinationFields.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("name", "address", "offset")), destinationFields.get(fileEndPoint));

    Map<EndPointField, Set<EndPointField>> incomingSummary = fllInfo.getIncomingSummary();
    Assert.assertEquals(3, incomingSummary.size());

    Set<EndPointField> expectedSet = new HashSet<>();
    expectedSet.add(new EndPointField(read1EndPoint, "body"));
    expectedSet.add(new EndPointField(read1EndPoint, "offset"));
    expectedSet.add(new EndPointField(read2EndPoint, "body"));
    expectedSet.add(new EndPointField(read2EndPoint, "offset"));
    Assert.assertEquals(expectedSet, incomingSummary.get(new EndPointField(fileEndPoint, "name")));
    Assert.assertEquals(expectedSet, incomingSummary.get(new EndPointField(fileEndPoint, "address")));
    Assert.assertEquals(expectedSet, incomingSummary.get(new EndPointField(fileEndPoint, "offset")));

    Map<EndPointField, Set<EndPointField>> outgoingSummary = fllInfo.getOutgoingSummary();
    Assert.assertEquals(4, outgoingSummary.size());

    expectedSet = new HashSet<>();
    expectedSet.add(new EndPointField(fileEndPoint, "offset"));
    expectedSet.add(new EndPointField(fileEndPoint, "name"));
    expectedSet.add(new EndPointField(fileEndPoint, "address"));
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(read1EndPoint, "offset")));
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(read1EndPoint, "body")));
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(read2EndPoint, "offset")));
    Assert.assertEquals(expectedSet, outgoingSummary.get(new EndPointField(read2EndPoint, "body")));

    // test outgoing operations of all source endoints
    Set<Operation> outgoingOperations = fllInfo.getOutgoingOperationsForField(new EndPointField(read1EndPoint,
                                                                                                "offset"));
    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(read1);
    expectedOperations.add(merge);
    expectedOperations.add(parse);
    expectedOperations.add(write);
    Assert.assertEquals(expectedOperations, outgoingOperations);

    outgoingOperations = fllInfo.getOutgoingOperationsForField(new EndPointField(read1EndPoint, "body"));
    Assert.assertEquals(expectedOperations, outgoingOperations);

    outgoingOperations = fllInfo.getOutgoingOperationsForField(new EndPointField(read2EndPoint, "offset"));
    expectedOperations = new HashSet<>();
    expectedOperations.add(read2);
    expectedOperations.add(merge);
    expectedOperations.add(parse);
    expectedOperations.add(write);
    Assert.assertEquals(expectedOperations, outgoingOperations);

    outgoingOperations = fllInfo.getOutgoingOperationsForField(new EndPointField(read2EndPoint, "body"));
    Assert.assertEquals(expectedOperations, outgoingOperations);
  }

  @Test
  public void testNonCycle() {
    EndPoint readEndPoint = EndPoint.of("ns", "src");
    EndPoint writeEndPoint = EndPoint.of("ns", "dest");

    ReadOperation read = new ReadOperation("read", "read", readEndPoint, "a", "b");
    TransformOperation combine = new TransformOperation("combine", "combine",
                                                        Arrays.asList(InputField.of("read", "a"),
                                                                      InputField.of("read", "b")),
                                                        "a", "b");
    // an operation with no incoming inputs, this should not be considered an cycle, but should get treat like a
    // read operation
    TransformOperation generate = new TransformOperation("generate", "generate",
                                                          Collections.emptyList(), "c");
    WriteOperation write = new WriteOperation("write", "write", writeEndPoint,
                                              Arrays.asList(InputField.of("combine", "a"),
                                                            InputField.of("combine", "b"),
                                                            InputField.of("generate", "c")));
    Set<Operation> unOrdered = new HashSet<>();
    unOrdered.add(combine);
    unOrdered.add(read);
    unOrdered.add(generate);
    unOrdered.add(write);

    List<Operation> operations = FieldLineageInfo.getTopologicallySortedOperations(unOrdered);
    List<Operation> expected = ImmutableList.of(read, generate, combine, write);
    Assert.assertEquals(expected, operations);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCycle() {
    EndPoint readEndPoint = EndPoint.of("ns", "file1");
    EndPoint writeEndPoint = EndPoint.of("ns", "file2");

    ReadOperation read = new ReadOperation("read", "read", readEndPoint, "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse",
                                                      Arrays.asList(InputField.of("read", "body"),
                                                                    InputField.of("normalize", "name")),
                                                      "name", "address");
    TransformOperation normalize = new TransformOperation("normalize", "normalize",
                                                          Collections.singletonList(InputField.of("parse", "name")),
                                                          "name");
    WriteOperation write = new WriteOperation("write", "writing to another file", writeEndPoint,
                                              Arrays.asList(InputField.of("normalize", "name"),
                                                            InputField.of("parse", "address")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(read);
    operations.add(normalize);
    operations.add(write);
    FieldLineageInfo.getTopologicallySortedOperations(new HashSet<>(operations));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCycleWithNonExistentOperationNames() {
    EndPoint readEndPoint = EndPoint.of("ns", "file1");
    EndPoint writeEndPoint = EndPoint.of("ns", "file2");

    ReadOperation read = new ReadOperation("read", "read", readEndPoint, "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse",
                                                      Arrays.asList(InputField.of("read", "body"),
                                                                    InputField.of("normalize", "name"),
                                                                    InputField.of("nop1", "field1")),
                                                      "name", "address");
    TransformOperation normalize = new TransformOperation("normalize", "normalize",
                                                          Arrays.asList(InputField.of("parse", "name"),
                                                                        InputField.of("nop2", "field2")),
                                                          "name");
    WriteOperation write = new WriteOperation("write", "writing to another file", writeEndPoint,
                                              Arrays.asList(InputField.of("normalize", "name"),
                                                            InputField.of("parse", "address"),
                                                            InputField.of("nop3", "field3")));

    List<Operation> operations = new ArrayList<>();
    operations.add(parse);
    operations.add(read);
    operations.add(normalize);
    operations.add(write);
    FieldLineageInfo.getTopologicallySortedOperations(new HashSet<>(operations));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSelfReferentialOperations() {
    TransformOperation parse = new TransformOperation("parse", "parse",
                                                      Arrays.asList(InputField.of("read", "body"),
                                                                    InputField.of("parse", "name")),
                                                      "name", "address");
    FieldLineageInfo.getTopologicallySortedOperations(Collections.singleton(parse));
  }

  @Test
  public void testLinearTopologicalSort() {
    // read---->parse---->normalize--->write
    ReadOperation read = new ReadOperation("read", "read descr", EndPoint.of("ns", "input"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse descr",
                                                      Collections.singletonList(InputField.of("read", "body")), "name",
                                                      "address");
    TransformOperation normalize = new TransformOperation("normalize", "normalize descr",
                                                          Collections.singletonList(InputField.of("parse", "address")),
                                                          "address");
    List<InputField> writeInputs = new ArrayList<>();
    writeInputs.add(InputField.of("parse", "name"));
    writeInputs.add(InputField.of("normalize", "address"));
    WriteOperation write = new WriteOperation("write", "write descr", EndPoint.of("ns", "output"), writeInputs);

    Set<Operation> operations = new LinkedHashSet<>();
    operations.add(read);
    operations.add(parse);
    operations.add(normalize);
    operations.add(write);

    List<Operation> topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);

    // try with few different insertion orders, the topological sort should give the same results
    operations = new LinkedHashSet<>();
    operations.add(parse);
    operations.add(normalize);
    operations.add(write);
    operations.add(read);
    topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);

    operations = new LinkedHashSet<>();
    operations.add(write);
    operations.add(normalize);
    operations.add(parse);
    operations.add(read);
    topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);
  }

  @Test
  public void testBranchTopologicalSort() {
    // read----------------------write
    //   \                      /
    //    ----parse---normalize

    ReadOperation read = new ReadOperation("read", "read descr", EndPoint.of("ns", "input"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse descr",
                                                      Collections.singletonList(InputField.of("read", "body")), "name",
                                                      "address");
    TransformOperation normalize = new TransformOperation("normalize", "normalize descr",
                                                          Collections.singletonList(InputField.of("parse", "address")),
                                                          "address");
    List<InputField> writeInputs = new ArrayList<>();
    writeInputs.add(InputField.of("read", "offset"));
    writeInputs.add(InputField.of("parse", "name"));
    writeInputs.add(InputField.of("normalize", "address"));
    WriteOperation write = new WriteOperation("write", "write descr", EndPoint.of("ns", "output"), writeInputs);

    Set<Operation> operations = new LinkedHashSet<>();
    operations.add(read);
    operations.add(parse);
    operations.add(normalize);
    operations.add(write);

    List<Operation> topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);

    // try with different insertion orders
    operations = new LinkedHashSet<>();
    operations.add(parse);
    operations.add(normalize);
    operations.add(write);
    operations.add(read);

    topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);

    operations = new LinkedHashSet<>();
    operations.add(write);
    operations.add(normalize);
    operations.add(parse);
    operations.add(read);

    topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, parse);
    assertBefore(topologicallySortedOperations, parse, normalize);
    assertBefore(topologicallySortedOperations, normalize, write);
    assertBefore(topologicallySortedOperations, read, write);

    // When the field lineage is queried for offset field, we will only return the
    // read and write operations, since parse and normalize operations are not affecting
    // the offset field in anyway. In this case even though write operation has input with origin
    // as normalize, topological sort should not affect by this case, where normalize operation
    // itself is missing.
    operations = new LinkedHashSet<>();
    operations.add(write);
    operations.add(read);

    topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read, write);
  }

  @Test
  public void testDisjointBranches() {
    // read1 -----> write1
    // read2 -----> write2
    ReadOperation read1 = new ReadOperation("read1", "read descr", EndPoint.of("ns", "input1"), "offset", "body");
    WriteOperation write1 = new WriteOperation("write1", "write descr", EndPoint.of("ns", "output"),
                                               InputField.of("read1", "offset"));

    ReadOperation read2 = new ReadOperation("read2", "read descr", EndPoint.of("ns", "input2"), "offset", "body");
    WriteOperation write2 = new WriteOperation("write2", "write descr", EndPoint.of("ns", "output"),
                                               InputField.of("read2", "offset"));

    Set<Operation> operations = new LinkedHashSet<>();
    operations.add(write1);
    operations.add(write2);
    operations.add(read2);
    operations.add(read1);

    List<Operation> topologicallySortedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    assertBefore(topologicallySortedOperations, read1, write1);
    assertBefore(topologicallySortedOperations, read2, write2);
  }

  @Test
  public void testRenameThenDropFields() {
    // read: endpoint1 -> (first_name, last_name, social)
    // renameSocial: read.social -> ssn
    // renameSocialAgain: renameSocial.ssn -> ssn2
    // dropSocial: renameSocialAgain.ssn2 -> ()
    // write: (read.first_name, read.first_name) -> endpoint2
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"),
        "first_name", "last_name", "social");
    TransformOperation renameSocial = new TransformOperation("renameSocial", "rename social",
        Collections.singletonList(InputField.of("read", "social")), "ssn");
    TransformOperation renameSocialAgain = new TransformOperation("renameSocialAgain",
        "rename social again", Collections.singletonList(InputField.of("renameSocial", "ssn")),
        "ssn2");
    TransformOperation dropSocial = new TransformOperation("dropSocial", "drop ssn2",
        Collections.singletonList(InputField.of("renameSocialAgain", "ssn2")));
    WriteOperation write = new WriteOperation("write", "write data", EndPoint.of("endpoint2"),
        Arrays.asList(InputField.of("read", "first_name"), InputField.of("read", "last_name")));

    Set<Operation> operations = Sets.newHashSet(read, renameSocial, renameSocialAgain, dropSocial, write);
    FieldLineageInfo info = new FieldLineageInfo(operations);

    EndPoint ep1 = EndPoint.of("endpoint1");
    EndPoint ep2 = EndPoint.of("endpoint2");
    EndPointField ep2ln = new EndPointField(ep2, "last_name");
    EndPointField ep2fn = new EndPointField(ep2, "first_name");
    EndPointField ep1ln = new EndPointField(ep1, "last_name");
    EndPointField ep1fn = new EndPointField(ep1, "first_name");

    Map<EndPointField, Set<EndPointField>> expectedOutgoingSummary = new HashMap<>();
    expectedOutgoingSummary.put(ep1fn, Collections.singleton(ep2fn));
    expectedOutgoingSummary.put(ep1ln, Collections.singleton(ep2ln));
    expectedOutgoingSummary.put(new EndPointField(ep1, "social"),
        Collections.singleton(FieldLineageInfo.NULL_EPF));
    Map<EndPointField, Set<EndPointField>> outgoingSummary = info.getOutgoingSummary();
    Assert.assertEquals(expectedOutgoingSummary, outgoingSummary);

    Map<EndPointField, Set<EndPointField>> expectedIncomingSummary = new HashMap<>();
    expectedIncomingSummary.put(ep2ln, Collections.singleton(ep1ln));
    expectedIncomingSummary.put(ep2fn, Collections.singleton(ep1fn));
    Assert.assertEquals(expectedIncomingSummary, info.getIncomingSummary());
  }

  @Test
  public void testMultiSourceDroppedFields() {
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"),
        "first_name", "last_name", "social");
    TransformOperation combineNames = new TransformOperation("combineNames", "combine names",
        Arrays.asList(
            InputField.of("read", "first_name"),
            InputField.of("read", "last_name")),
        "full_name");
    TransformOperation dropSocial = new TransformOperation("dropSocial", "drop social",
        Collections.singletonList(InputField.of("read", "social")));
    WriteOperation write = new WriteOperation("write", "write data", EndPoint.of("endpoint2"),
        Collections.singletonList(InputField.of("combineNames", "full_name")));

    Set<Operation> operations = Sets.newHashSet(read, write, combineNames, dropSocial);
    FieldLineageInfo info1 = new FieldLineageInfo(operations);

    EndPoint ep1 = EndPoint.of("endpoint1");
    EndPoint ep2 = EndPoint.of("endpoint2");

    Map<EndPointField, Set<EndPointField>> expectedOutgoingSummary = new HashMap<>();
    expectedOutgoingSummary.put(new EndPointField(ep1, "first_name"),
        Collections.singleton(new EndPointField(ep2, "full_name")));
    expectedOutgoingSummary.put(new EndPointField(ep1, "last_name"),
        Collections.singleton(new EndPointField(ep2, "full_name")));
    expectedOutgoingSummary.put(new EndPointField(ep1, "social"),
        Collections.singleton(FieldLineageInfo.NULL_EPF));
    Assert.assertEquals(expectedOutgoingSummary, info1.getOutgoingSummary());

    Map<EndPointField, Set<EndPointField>> expectedIncomingSummary = new HashMap<>();
    expectedIncomingSummary.put(new EndPointField(ep2, "full_name"),
        Sets.newHashSet(
            new EndPointField(ep1, "first_name"),
            new EndPointField(ep1, "last_name")));
    Assert.assertEquals(expectedIncomingSummary, info1.getIncomingSummary());
  }

  private void assertBefore(List<Operation> list, Operation a, Operation b) {
    int aIndex = list.indexOf(a);
    int bIndex = list.indexOf(b);
    Assert.assertTrue(aIndex < bIndex);
  }

  private void generateLineage(List<String> inputs, List<Operation> operations, String identityNamePrefix,
                               String identityOrigin, String transform) {
    // emit identity transform for all fields
    for (int i = 0; i < inputs.size(); i++) {
      operations.add(new TransformOperation(identityNamePrefix + i, "identity transform",
                                            Collections.singletonList(InputField.of(identityOrigin, inputs.get(i))),
                                            inputs.get(i)));
    }

    // generate an all-to-all, so that when track back, this operation has to track back to all the previous
    // identity transform
    List<InputField> inputFields = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      inputFields.add(InputField.of(identityNamePrefix + i, inputs.get(i)));
    }
    TransformOperation parse = new TransformOperation(transform, "all to all transform",
                                                      inputFields, inputs);
    operations.add(parse);
  }
}
