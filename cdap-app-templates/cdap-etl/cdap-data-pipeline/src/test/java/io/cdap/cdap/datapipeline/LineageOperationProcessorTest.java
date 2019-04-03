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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldReadOperation;
import co.cask.cdap.etl.api.lineage.field.FieldTransformOperation;
import co.cask.cdap.etl.api.lineage.field.FieldWriteOperation;
import co.cask.cdap.etl.proto.Connection;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class LineageOperationProcessorTest {

  @Test
  public void testSimplePipeline() {
    // n1-->n2-->n3
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset",
                                               "body"));
    stageOperations.put("n1", fieldOperations);
    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("parse", "parsing data", Collections.singletonList("body"),
                                                    Arrays.asList("name", "address", "zip")));
    stageOperations.put("n2", fieldOperations);
    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                                "name", "address", "zip"));
    stageOperations.put("n3", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();
    Set<Operation> expected = new HashSet<>();
    expected.add(new ReadOperation("n1.read", "reading data",
                                   EndPoint.of("default", "file"), "offset", "body"));
    expected.add(new TransformOperation("n2.parse", "parsing data",
                                        Collections.singletonList(InputField.of("n1.read", "body")), "name", "address",
                                        "zip"));
    expected.add(new WriteOperation("n3.write", "writing data",
                                    EndPoint.of("default", "file2"), InputField.of("n2.parse", "name"),
                                    InputField.of("n2.parse", "address"), InputField.of("n2.parse", "zip")));

    Assert.assertEquals(new FieldLineageInfo(expected), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testAnotherSimplePipeline() {

    // n1-->n2-->n3-->n4
    // n1 => read: file -> (offset, body)
    // n2 => parse: (body) -> (first_name, last_name) | n2
    // n3 => concat: (first_name, last_name) -> (name) | n
    // n4 => write: (offset, name) -> another_file

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "some read", EndPoint.of("ns", "file1"), "offset",
                                               "body"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("parse", "parsing body", Collections.singletonList("body"),
                                                    "first_name", "last_name"));
    stageOperations.put("n2", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("concat", "concatinating the fields",
                                                    Arrays.asList("first_name", "last_name"), "name"));
    stageOperations.put("n3", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write_op", "writing data to file",
                                                EndPoint.of("myns", "another_file"),
                                                Arrays.asList("offset", "name")));
    stageOperations.put("n4", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();

    ReadOperation read = new ReadOperation("n1.read", "some read", EndPoint.of("ns", "file1"), "offset", "body");

    TransformOperation parse = new TransformOperation("n2.parse", "parsing body",
                                                      Collections.singletonList(InputField.of("n1.read", "body")),
                                                      "first_name", "last_name");

    TransformOperation concat = new TransformOperation("n3.concat", "concatinating the fields",
                                                       Arrays.asList(InputField.of("n2.parse", "first_name"),
                                                                     InputField.of("n2.parse", "last_name")),
                                                       "name");

    WriteOperation write = new WriteOperation("n4.write_op", "writing data to file",
                                              EndPoint.of("myns", "another_file"),
                                              Arrays.asList(InputField.of("n1.read", "offset"),
                                                            InputField.of("n3.concat", "name")));

    List<Operation> expectedOperations = new ArrayList<>();
    expectedOperations.add(parse);
    expectedOperations.add(concat);
    expectedOperations.add(read);
    expectedOperations.add(write);

    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testSourceWithMultipleDestinations() {
    //              |----->n3
    // n1--->n2-----|
    //              |----->n4

    // n1 => read: file -> (offset, body)
    // n2 => parse: body -> (id, name, address, zip)
    // n3 => write1: (parse.id, parse.name) -> info
    // n4 => write2: (parse.address, parse.zip) -> location

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint source = EndPoint.of("ns", "file");
    EndPoint info = EndPoint.of("ns", "info");
    EndPoint location = EndPoint.of("ns", "location");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "reading from file", source, "offset", "body"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("parse", "parsing body", Collections.singletonList("body"),
                                                    "id", "name", "address", "zip"));
    stageOperations.put("n2", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("infoWrite", "writing info", info, "id", "name"));
    stageOperations.put("n3", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("locationWrite", "writing location", location, "address", "zip"));
    stageOperations.put("n4", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();

    ReadOperation read = new ReadOperation("n1.read", "reading from file", source, "offset", "body");

    expectedOperations.add(read);

    TransformOperation parse = new TransformOperation("n2.parse", "parsing body",
                                                      Collections.singletonList(InputField.of("n1.read", "body")),
                                                      "id", "name", "address", "zip");

    expectedOperations.add(parse);

    WriteOperation infoWrite = new WriteOperation("n3.infoWrite", "writing info", info, InputField.of("n2.parse", "id"),
                                                  InputField.of("n2.parse", "name"));

    expectedOperations.add(infoWrite);

    WriteOperation locationWrite = new WriteOperation("n4.locationWrite", "writing location", location,
                                                      InputField.of("n2.parse", "address"),
                                                      InputField.of("n2.parse", "zip"));

    expectedOperations.add(locationWrite);
    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testDirectMerge() {

    // n1--------->n3
    //       |
    // n2--------->n4

    // n1 => pRead: personFile -> (offset, body)
    // n2 => hRead: hrFile -> (offset, body)
    // n1.n2.merge => n1.n2.merge: (pRead.offset, pRead.body, hRead.offset, hRead.body) -> (offset, body)
    // n3 => write1: (n1.n2.merge.offset, n1.n2.merge.body) -> testStore
    // n4 => write1: (n1.n2.merge.offset, n1.n2.merge.body) -> prodStore

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n1", "n4"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n2", "n4"));

    EndPoint pEndPoint = EndPoint.of("ns", "personFile");
    EndPoint hEndPoint = EndPoint.of("ns", "hrFile");
    EndPoint testEndPoint = EndPoint.of("ns", "testStore");
    EndPoint prodEndPoint = EndPoint.of("ns", "prodStore");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("pRead", "Reading from person file", pEndPoint, "offset", "body"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("hRead", "Reading from hr file", hEndPoint, "offset", "body"));
    stageOperations.put("n2", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write1", "Writing to test store", testEndPoint, "offset",
                                                "body"));
    stageOperations.put("n3", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write2", "Writing to prod store", prodEndPoint, "offset",
                                                "body"));
    stageOperations.put("n4", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();
    ReadOperation pRead = new ReadOperation("n1.pRead", "Reading from person file", pEndPoint, "offset", "body");
    expectedOperations.add(pRead);

    ReadOperation hRead = new ReadOperation("n2.hRead", "Reading from hr file", hEndPoint, "offset", "body");
    expectedOperations.add(hRead);

    // implicit merge should be added by app
    TransformOperation merge = new TransformOperation("n1.n2.merge", "Merged stages: n1,n2",
                                                      Arrays.asList(InputField.of("n1.pRead", "offset"),
                                                                    InputField.of("n1.pRead", "body"),
                                                                    InputField.of("n2.hRead", "offset"),
                                                                    InputField.of("n2.hRead", "body")),
                                                      "offset", "body");
    expectedOperations.add(merge);

    WriteOperation write1 = new WriteOperation("n3.write1", "Writing to test store", testEndPoint,
                                               Arrays.asList(InputField.of("n1.n2.merge", "offset"),
                                                             InputField.of("n1.n2.merge", "body")));
    expectedOperations.add(write1);

    WriteOperation write2 = new WriteOperation("n4.write2", "Writing to prod store", prodEndPoint,
                                               Arrays.asList(InputField.of("n1.n2.merge", "offset"),
                                                             InputField.of("n1.n2.merge", "body")));
    expectedOperations.add(write2);

    Assert.assertEquals(new FieldLineageInfo(expectedOperations), new FieldLineageInfo(processedOperations));
  }

  @Test
  public void testComplexMerge() {
    //
    //  n1----n2---
    //            |         |-------n6
    //            |----n5---|
    //  n3----n4---         |---n7----n8
    //
    //
    //  n1: read: file1 -> offset,body
    //  n2: parse: body -> name, address, zip
    //  n3: read: file2 -> offset,body
    //  n4: parse: body -> name, address, zip
    //  n5: normalize: address -> address
    //  n5: rename: address -> state_address
    //  n6: write: offset, name, address -> file3
    //  n7: rename: offset -> file_offset
    //  n8: write: file_offset, name, address, zip -> file4

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n2"));
    connections.add(new Connection("n2", "n5"));
    connections.add(new Connection("n3", "n4"));
    connections.add(new Connection("n4", "n5"));
    connections.add(new Connection("n5", "n6"));
    connections.add(new Connection("n5", "n7"));
    connections.add(new Connection("n7", "n8"));

    EndPoint n1EndPoint = EndPoint.of("ns", "file1");
    EndPoint n3EndPoint = EndPoint.of("ns", "file2");
    EndPoint n6EndPoint = EndPoint.of("ns", "file3");
    EndPoint n8EndPoint = EndPoint.of("ns", "file4");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    List<FieldOperation> fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "reading file 1", n1EndPoint, "offset", "body"));
    stageOperations.put("n1", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("parse", "parsing file 1", Collections.singletonList("body"),
                                                    "name", "address", "zip"));
    stageOperations.put("n2", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldReadOperation("read", "reading file 2", n3EndPoint, "offset", "body"));
    stageOperations.put("n3", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("parse", "parsing file 2", Collections.singletonList("body"),
                                                    "name", "address", "zip"));
    stageOperations.put("n4", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("normalize", "normalizing address",
                                                    Collections.singletonList("address"), "address"));
    fieldOperations.add(new FieldTransformOperation("rename", "renaming address to state_address",
                                                    Collections.singletonList("address"), "state_address"));
    stageOperations.put("n5", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write", "writing file 3", n6EndPoint, "offset", "name",
                                                "address"));
    stageOperations.put("n6", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldTransformOperation("rename", "renaming offset to file_offset",
                                                    Collections.singletonList("offset"), "file_offset"));
    stageOperations.put("n7", fieldOperations);

    fieldOperations = new ArrayList<>();
    fieldOperations.add(new FieldWriteOperation("write", "writing file 4", n8EndPoint, "file_offset", "name",
                                                "address", "zip"));
    stageOperations.put("n8", fieldOperations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.emptySet());
    Set<Operation> processedOperations = processor.process();
    Set<Operation> expectedOperations = new HashSet<>();

    ReadOperation read = new ReadOperation("n1.read", "reading file 1", n1EndPoint, "offset", "body");
    expectedOperations.add(read);

    TransformOperation parse = new TransformOperation("n2.parse", "parsing file 1",
                                                      Collections.singletonList(InputField.of("n1.read", "body")),
                                                      "name", "address", "zip");
    expectedOperations.add(parse);

    read = new ReadOperation("n3.read", "reading file 2", n3EndPoint, "offset", "body");
    expectedOperations.add(read);

    parse = new TransformOperation("n4.parse", "parsing file 2",
                                   Collections.singletonList(InputField.of("n3.read", "body")), "name", "address",
                                   "zip");
    expectedOperations.add(parse);

    List<InputField> inputsToMerge = new ArrayList<>();
    inputsToMerge.add(InputField.of("n1.read", "offset"));
    inputsToMerge.add(InputField.of("n1.read", "body"));
    inputsToMerge.add(InputField.of("n2.parse", "name"));
    inputsToMerge.add(InputField.of("n2.parse", "address"));
    inputsToMerge.add(InputField.of("n2.parse", "zip"));
    inputsToMerge.add(InputField.of("n3.read", "offset"));
    inputsToMerge.add(InputField.of("n3.read", "body"));
    inputsToMerge.add(InputField.of("n4.parse", "name"));
    inputsToMerge.add(InputField.of("n4.parse", "address"));
    inputsToMerge.add(InputField.of("n4.parse", "zip"));

    TransformOperation merge = new TransformOperation("n2.n4.merge", "Merged stages: n2,n4", inputsToMerge, "offset",
                                                      "body", "name", "address", "zip");
    expectedOperations.add(merge);

    TransformOperation normalize = new TransformOperation("n5.normalize", "normalizing address",
                                                          Collections.singletonList(InputField.of("n2.n4.merge",
                                                                                                  "address")),
                                                          "address");
    expectedOperations.add(normalize);

    TransformOperation rename = new TransformOperation("n5.rename", "renaming address to state_address",
                                                       Collections.singletonList(InputField.of("n5.normalize",
                                                                                               "address")),
                                                       "state_address");
    expectedOperations.add(rename);

    WriteOperation write = new WriteOperation("n6.write", "writing file 3", n6EndPoint,
                                              InputField.of("n2.n4.merge", "offset"),
                                              InputField.of("n2.n4.merge", "name"),
                                              InputField.of("n5.normalize", "address"));
    expectedOperations.add(write);

    rename = new TransformOperation("n7.rename", "renaming offset to file_offset",
                                    Collections.singletonList(InputField.of("n2.n4.merge", "offset")), "file_offset");
    expectedOperations.add(rename);

    write = new WriteOperation("n8.write", "writing file 4", n8EndPoint, InputField.of("n7.rename", "file_offset"),
                               InputField.of("n2.n4.merge", "name"), InputField.of("n5.normalize", "address"),
                               InputField.of("n2.n4.merge", "zip"));
    expectedOperations.add(write);

    Assert.assertEquals(expectedOperations, processedOperations);
  }

  @Test
  public void testSimpleJoinOperation() {
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint cEndPoint = EndPoint.of("default", "customer");
    EndPoint pEndPoint = EndPoint.of("default", "purchase");
    EndPoint cpEndPoint = EndPoint.of("default", "customer_purchase");


    //  customer -> (id)------------
    //                              |
    //                            JOIN  ------->(id, customer_id)
    //                              |
    //  purchase -> (customer_id)---

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("ReadCustomer", "read description",
                                                                               cEndPoint, "id")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("ReadPurchase", "read description",
                                                                               pEndPoint, "customer_id")));
    stageOperations.put("n3",
                        Collections.singletonList(new FieldTransformOperation("Join", "Join Operation",
                                                                              Arrays.asList("n1.id",
                                                                                            "n2.customer_id"),
                                                                              Arrays.asList("id", "customer_id"))));
    stageOperations.put("n4", Collections.singletonList(new FieldWriteOperation("Write", "write description",
                                                                                cpEndPoint, "id", "customer_id")));
    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n3"));
    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(new ReadOperation("n1.ReadCustomer", "read description", cEndPoint, "id"));
    expectedOperations.add(new ReadOperation("n2.ReadPurchase", "read description", pEndPoint, "customer_id"));
    expectedOperations.add(new TransformOperation("n3.Join", "Join Operation",
                                                  Arrays.asList(InputField.of("n1.ReadCustomer", "id"),
                                                                InputField.of("n2.ReadPurchase", "customer_id")),
                                                  "id", "customer_id"));
    expectedOperations.add(new WriteOperation("n4.Write", "write description", cpEndPoint,
                                              Arrays.asList(InputField.of("n3.Join", "id"),
                                                            InputField.of("n3.Join", "customer_id"))));
    Assert.assertEquals(expectedOperations, processor.process());
  }

  @Test
  public void testSimpleJoinWithAdditionalFields() {
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint cEndPoint = EndPoint.of("default", "customer");
    EndPoint pEndPoint = EndPoint.of("default", "purchase");
    EndPoint cpEndPoint = EndPoint.of("default", "customer_purchase");


    //  customer -> (id)------------
    //                              |
    //                            JOIN  ------->(id, customer_id)
    //                              |
    //  purchase -> (customer_id)---

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("ReadCustomer", "read description",
                                                                               cEndPoint, "id", "name")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("ReadPurchase", "read description",
                                                                               pEndPoint, "customer_id", "item")));

    List<FieldOperation> operationsFromJoin = new ArrayList<>();
    operationsFromJoin.add(new FieldTransformOperation("Join", "Join Operation",
                                                       Arrays.asList("n1.id", "n2.customer_id"),
                                                       Arrays.asList("id", "customer_id")));
    operationsFromJoin.add(new FieldTransformOperation("Identity name", "Identity Operation",
                                                       Collections.singletonList("n1.name"),
                                                       Collections.singletonList("name")));
    operationsFromJoin.add(new FieldTransformOperation("Identity item", "Identity Operation",
                                                       Collections.singletonList("n2.item"),
                                                       Collections.singletonList("item")));
    stageOperations.put("n3", operationsFromJoin);

    stageOperations.put("n4", Collections.singletonList(new FieldWriteOperation("Write", "write description",
                                                                                cpEndPoint, "id", "name",
                                                                                "customer_id", "item")));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n3"));

    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(new ReadOperation("n1.ReadCustomer", "read description", cEndPoint, "id", "name"));
    expectedOperations.add(new ReadOperation("n2.ReadPurchase", "read description", pEndPoint, "customer_id", "item"));
    expectedOperations.add(new TransformOperation("n3.Join", "Join Operation",
                                                  Arrays.asList(InputField.of("n1.ReadCustomer", "id"),
                                                                InputField.of("n2.ReadPurchase", "customer_id")),
                                                  "id", "customer_id"));
    expectedOperations.add(new TransformOperation("n3.Identity name", "Identity Operation",
                                                  Collections.singletonList(InputField.of("n1.ReadCustomer", "name")),
                                                  "name"));
    expectedOperations.add(new TransformOperation("n3.Identity item", "Identity Operation",
                                                  Collections.singletonList(InputField.of("n2.ReadPurchase", "item")),
                                                  "item"));

    expectedOperations.add(new WriteOperation("n4.Write", "write description", cpEndPoint,
                                              Arrays.asList(InputField.of("n3.Join", "id"),
                                                            InputField.of("n3.Identity name", "name"),
                                                            InputField.of("n3.Join", "customer_id"),
                                                            InputField.of("n3.Identity item", "item"))));
    Set<Operation> processedOperations = processor.process();
    Assert.assertEquals(expectedOperations, processedOperations);
  }

  @Test
  public void testSimpleJoinWithRenameJoinKeys() {
    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint cEndPoint = EndPoint.of("default", "customer");
    EndPoint pEndPoint = EndPoint.of("default", "purchase");
    EndPoint cpEndPoint = EndPoint.of("default", "customer_purchase");


    //  customer -> (id, name)------------
    //                                    |
    //                                  JOIN  ------->(id_from_customer, id_from_purchase, name, item)
    //                                    |
    //  purchase -> (customer_id, item)---

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("ReadCustomer", "read description",
                                                                               cEndPoint, "id", "name")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("ReadPurchase", "read description",
                                                                               pEndPoint, "customer_id", "item")));

    List<FieldOperation> operationsFromJoin = new ArrayList<>();
    operationsFromJoin.add(new FieldTransformOperation("Join", "Join Operation",
                                                       Arrays.asList("n1.id", "n2.customer_id"),
                                                       Arrays.asList("id", "customer_id")));
    operationsFromJoin.add(new FieldTransformOperation("Rename id", "Rename id", Collections.singletonList("id"),
                                                       "id_from_customer"));
    operationsFromJoin.add(new FieldTransformOperation("Rename customer_id", "Rename customer_id",
                                                       Collections.singletonList("customer_id"), "id_from_purchase"));
    operationsFromJoin.add(new FieldTransformOperation("Identity name", "Identity Operation",
                                                       Collections.singletonList("n1.name"),
                                                       Collections.singletonList("name")));
    operationsFromJoin.add(new FieldTransformOperation("Identity item", "Identity Operation",
                                                       Collections.singletonList("n2.item"),
                                                       Collections.singletonList("item")));
    stageOperations.put("n3", operationsFromJoin);

    stageOperations.put("n4", Collections.singletonList(new FieldWriteOperation("Write", "write description",
                                                                                cpEndPoint, "id_from_customer",
                                                                                "id_from_purchase",
                                                                                "name", "item")));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n3"));
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(new ReadOperation("n1.ReadCustomer", "read description", cEndPoint, "id", "name"));
    expectedOperations.add(new ReadOperation("n2.ReadPurchase", "read description", pEndPoint, "customer_id", "item"));
    expectedOperations.add(new TransformOperation("n3.Join", "Join Operation",
                                                  Arrays.asList(InputField.of("n1.ReadCustomer", "id"),
                                                                InputField.of("n2.ReadPurchase", "customer_id")),
                                                  "id", "customer_id"));
    expectedOperations.add(new TransformOperation("n3.Rename id", "Rename id",
                                                  Collections.singletonList(InputField.of("n3.Join", "id")),
                                                  "id_from_customer"));
    expectedOperations.add(new TransformOperation("n3.Rename customer_id", "Rename customer_id",
                                                  Collections.singletonList(InputField.of("n3.Join", "customer_id")),
                                                  "id_from_purchase"));
    expectedOperations.add(new TransformOperation("n3.Identity name", "Identity Operation",
                                                       Collections.singletonList(InputField.of("n1.ReadCustomer",
                                                                                               "name")), "name"));
    expectedOperations.add(new TransformOperation("n3.Identity item", "Identity Operation",
                                                  Collections.singletonList(InputField.of("n2.ReadPurchase", "item")),
                                                  "item"));

    expectedOperations.add(new WriteOperation("n4.Write", "write description", cpEndPoint,
                                              Arrays.asList(InputField.of("n3.Rename id", "id_from_customer"),
                                                            InputField.of("n3.Rename customer_id", "id_from_purchase"),
                                                            InputField.of("n3.Identity name", "name"),
                                                            InputField.of("n3.Identity item", "item"))));

    Assert.assertEquals(expectedOperations, processedOperations);
  }

  @Test
  public void testSimpleJoinWithRenameOnAdditionalFields() {
    //  customer -> (id, name)----------
    //                                  |
    //                                JOIN  --->(id_from_customer, customer_id, name_from_customer, item_from_purchase)
    //                                  |
    //  purchase ->(customer_id, item)---

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n3"));
    connections.add(new Connection("n2", "n3"));
    connections.add(new Connection("n3", "n4"));

    EndPoint cEndPoint = EndPoint.of("default", "customer");
    EndPoint pEndPoint = EndPoint.of("default", "purchase");
    EndPoint cpEndPoint = EndPoint.of("default", "customer_purchase");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("ReadCustomer", "read description",
                                                                               cEndPoint, "id", "name")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("ReadPurchase", "read description",
                                                                               pEndPoint, "customer_id", "item")));

    List<FieldOperation> operationsFromJoin = new ArrayList<>();
    operationsFromJoin.add(new FieldTransformOperation("Join", "Join Operation",
                                                       Arrays.asList("n1.id", "n2.customer_id"),
                                                       Arrays.asList("id", "customer_id")));
    operationsFromJoin.add(new FieldTransformOperation("Rename id", "Rename id", Collections.singletonList("id"),
                                                       "id_from_customer"));
    operationsFromJoin.add(new FieldTransformOperation("Rename name", "Rename name",
                                                       Collections.singletonList("n1.name"), "name_from_customer"));
    operationsFromJoin.add(new FieldTransformOperation("Rename item", "Rename item",
                                                       Collections.singletonList("n2.item"), "item_from_purchase"));
    stageOperations.put("n3", operationsFromJoin);

    stageOperations.put("n4", Collections.singletonList(new FieldWriteOperation("Write", "write description",
                                                                                cpEndPoint, "id_from_customer",
                                                                                "customer_id", "name_from_customer",
                                                                                "item_from_purchase")));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n3"));
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(new ReadOperation("n1.ReadCustomer", "read description", cEndPoint, "id", "name"));
    expectedOperations.add(new ReadOperation("n2.ReadPurchase", "read description", pEndPoint, "customer_id", "item"));
    expectedOperations.add(new TransformOperation("n3.Join", "Join Operation",
                                                  Arrays.asList(InputField.of("n1.ReadCustomer", "id"),
                                                                InputField.of("n2.ReadPurchase", "customer_id")),
                                                  "id", "customer_id"));
    expectedOperations.add(new TransformOperation("n3.Rename id", "Rename id",
                                                  Collections.singletonList(InputField.of("n3.Join", "id")),
                                                  "id_from_customer"));
    expectedOperations.add(new TransformOperation("n3.Rename name", "Rename name",
                                                  Collections.singletonList(InputField.of("n1.ReadCustomer",
                                                                                          "name")),
                                                  "name_from_customer"));
    expectedOperations.add(new TransformOperation("n3.Rename item", "Rename item",
                                                  Collections.singletonList(InputField.of("n2.ReadPurchase",
                                                                                          "item")),
                                                  "item_from_purchase"));

    expectedOperations.add(new WriteOperation("n4.Write", "write description", cpEndPoint,
                                              Arrays.asList(InputField.of("n3.Rename id", "id_from_customer"),
                                                            InputField.of("n3.Join", "customer_id"),
                                                            InputField.of("n3.Rename name", "name_from_customer"),
                                                            InputField.of("n3.Rename item", "item_from_purchase"))));

    Assert.assertEquals(expectedOperations, processedOperations);
  }

  @Test
  public void testJoinWith3Inputs() {
    // customer -> (id, name)---------- |
    //                                  |
    // purchase ->(customer_id, item)------> JOIN --->(id_from_customer, customer_id, address_id,
    //                                  |                   name_from_customer, address)
    //                                  |
    // address ->(address_id, address)--|

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection("n1", "n4"));
    connections.add(new Connection("n2", "n4"));
    connections.add(new Connection("n3", "n4"));
    connections.add(new Connection("n4", "n5"));

    EndPoint cEndPoint = EndPoint.of("default", "customer");
    EndPoint pEndPoint = EndPoint.of("default", "purchase");
    EndPoint aEndPoint = EndPoint.of("default", "address");
    EndPoint acpEndPoint = EndPoint.of("default", "customer_purchase_address");

    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    stageOperations.put("n1", Collections.singletonList(new FieldReadOperation("ReadCustomer", "read description",
                                                                               cEndPoint, "id", "name")));
    stageOperations.put("n2", Collections.singletonList(new FieldReadOperation("ReadPurchase", "read description",
                                                                               pEndPoint, "customer_id", "item")));
    stageOperations.put("n3", Collections.singletonList(new FieldReadOperation("ReadAddress", "read description",
                                                                               aEndPoint, "address_id", "address")));

    List<FieldOperation> operationsFromJoin = new ArrayList<>();

    operationsFromJoin.add(new FieldTransformOperation("Join", "Join Operation",
                                             Arrays.asList("n1.id", "n2.customer_id", "n3.address_id"),
                                             Arrays.asList("id", "customer_id", "address_id")));

    operationsFromJoin.add(new FieldTransformOperation("Rename id", "Rename Operation",
                                             Collections.singletonList("id"),
                                             Collections.singletonList("id_from_customer")));

    operationsFromJoin.add(new FieldTransformOperation("Rename customer.name", "Rename Operation",
                                             Collections.singletonList("n1.name"),
                                             Collections.singletonList("name_from_customer")));

    operationsFromJoin.add(new FieldTransformOperation("Identity address.address", "Identity Operation",
                                             Collections.singletonList("n3.address"),
                                             Collections.singletonList("address")));

    stageOperations.put("n4", operationsFromJoin);

    stageOperations.put("n5", Collections.singletonList(new FieldWriteOperation("Write", "Write Operation",
                                                                                acpEndPoint, "id_from_customer",
                                                                                "customer_id", "address_id",
                                                                                "name_from_customer",
                                                                                "address")));

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          Collections.singleton("n4"));
    Set<Operation> processedOperations = processor.process();

    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(new ReadOperation("n1.ReadCustomer", "read description", cEndPoint, "id", "name"));
    expectedOperations.add(new ReadOperation("n2.ReadPurchase", "read description", pEndPoint, "customer_id", "item"));
    expectedOperations.add(new ReadOperation("n3.ReadAddress", "read description", aEndPoint, "address_id", "address"));

    expectedOperations.add(new TransformOperation("n4.Join", "Join Operation",
                                                  Arrays.asList(InputField.of("n1.ReadCustomer", "id"),
                                                                InputField.of("n2.ReadPurchase", "customer_id"),
                                                                InputField.of("n3.ReadAddress", "address_id")),
                                                  "id", "customer_id", "address_id"));

    expectedOperations.add(new TransformOperation("n4.Rename id", "Rename Operation",
                                                  Collections.singletonList(InputField.of("n4.Join", "id")),
                                                  "id_from_customer"));

    expectedOperations.add(new TransformOperation("n4.Rename customer.name", "Rename Operation",
                                                  Collections.singletonList(InputField.of("n1.ReadCustomer",
                                                                                          "name")),
                                                  "name_from_customer"));

    expectedOperations.add(new TransformOperation("n4.Identity address.address", "Identity Operation",
                                                  Collections.singletonList(InputField.of("n3.ReadAddress",
                                                                                          "address")),
                                                  "address"));

    expectedOperations.add(new WriteOperation("n5.Write", "Write Operation", acpEndPoint,
                                              Arrays.asList(InputField.of("n4.Rename id", "id_from_customer"),
                                                            InputField.of("n4.Join", "customer_id"),
                                                            InputField.of("n4.Join", "address_id"),
                                                            InputField.of("n4.Rename customer.name",
                                                                          "name_from_customer"),
                                                            InputField.of("n4.Identity address.address", "address"))));

    Assert.assertEquals(expectedOperations, processedOperations);

  }
}
