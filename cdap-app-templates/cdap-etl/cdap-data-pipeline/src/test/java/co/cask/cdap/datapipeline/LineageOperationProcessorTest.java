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
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import co.cask.cdap.etl.api.lineage.field.Operation;
import co.cask.cdap.etl.api.lineage.field.ReadOperation;
import co.cask.cdap.etl.api.lineage.field.TransformOperation;
import co.cask.cdap.etl.api.lineage.field.WriteOperation;
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

    Map<String, List<Operation>> stageOperations = new HashMap<>();
    List<Operation> operations = new ArrayList<>();
    operations.add(new ReadOperation("read", "reading data", EndPoint.of("default", "file"), "offset", "body"));
    stageOperations.put("n1", operations);
    operations = new ArrayList<>();
    operations.add(new TransformOperation("parse", "parsing data", Collections.singletonList("body"),
                                          Arrays.asList("name", "address", "zip")));
    stageOperations.put("n2", operations);
    operations = new ArrayList<>();
    operations.add(new WriteOperation("write", "writing data", EndPoint.of("default", "file2"),
                                      "name", "address", "zip"));
    stageOperations.put("n3", operations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          new HashSet<>());
    Set<co.cask.cdap.api.lineage.field.Operation> processedOperations = processor.process();
    Set<co.cask.cdap.api.lineage.field.Operation> expected = new HashSet<>();
    expected.add(new co.cask.cdap.api.lineage.field.ReadOperation("n1.read", "reading data",
                                                                  EndPoint.of("default", "file"), "offset", "body"));
    expected.add(new co.cask.cdap.api.lineage.field.TransformOperation("n2.parse", "parsing data",
            Collections.singletonList(InputField.of("n1.read", "body")), "name", "address", "zip"));
    expected.add(new co.cask.cdap.api.lineage.field.WriteOperation("n3.write", "writing data",
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

    Map<String, List<Operation>> stageOperations = new HashMap<>();
    List<Operation> operations = new ArrayList<>();
    operations.add(new ReadOperation("read", "some read", EndPoint.of("ns", "file1"), "offset", "body"));
    stageOperations.put("n1", operations);

    operations = new ArrayList<>();
    operations.add(new TransformOperation("parse", "parsing body", Collections.singletonList("body"), "first_name",
                                          "last_name"));
    stageOperations.put("n2", operations);

    operations = new ArrayList<>();
    operations.add(new TransformOperation("concat", "concatinating the fields",
                                          Arrays.asList("first_name", "last_name"), "name"));
    stageOperations.put("n3", operations);

    operations = new ArrayList<>();
    operations.add(new WriteOperation("write_op", "writing data to file", EndPoint.of("myns", "another_file"),
                                      Arrays.asList("offset", "name")));
    stageOperations.put("n4", operations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          new HashSet<>());
    Set<co.cask.cdap.api.lineage.field.Operation> processedOperations = processor.process();

    co.cask.cdap.api.lineage.field.ReadOperation read
            = new co.cask.cdap.api.lineage.field.ReadOperation("n1.read", "some read", EndPoint.of("ns", "file1"),
                                                               "offset", "body");

    co.cask.cdap.api.lineage.field.TransformOperation parse
            = new co.cask.cdap.api.lineage.field.TransformOperation("n2.parse", "parsing body",
            Collections.singletonList(InputField.of("n1.read", "body")), "first_name", "last_name");

    co.cask.cdap.api.lineage.field.TransformOperation concat
            = new co.cask.cdap.api.lineage.field.TransformOperation("n3.concat", "concatinating the fields",
                    Arrays.asList(InputField.of("n2.parse", "first_name"),
                                  InputField.of("n2.parse", "last_name")), "name");

    co.cask.cdap.api.lineage.field.WriteOperation write
            = new co.cask.cdap.api.lineage.field.WriteOperation("n4.write_op", "writing data to file",
                                                                EndPoint.of("myns", "another_file"),
                                                                Arrays.asList(InputField.of("n1.read", "offset"),
                                                                              InputField.of("n3.concat", "name")));

    List<co.cask.cdap.api.lineage.field.Operation> expectedOperations = new ArrayList<>();
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

    Map<String, List<Operation>> stageOperations = new HashMap<>();
    List<Operation> operations = new ArrayList<>();
    operations.add(new ReadOperation("read", "reading from file", source, "offset", "body"));
    stageOperations.put("n1", operations);

    operations = new ArrayList<>();
    operations.add(new TransformOperation("parse", "parsing body", Collections.singletonList("body"),
                                          "id", "name", "address", "zip"));
    stageOperations.put("n2", operations);

    operations = new ArrayList<>();
    operations.add(new WriteOperation("infoWrite", "writing info", info, "id", "name"));
    stageOperations.put("n3", operations);

    operations = new ArrayList<>();
    operations.add(new WriteOperation("locationWrite", "writing location", location, "address", "zip"));
    stageOperations.put("n4", operations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          new HashSet<>());
    Set<co.cask.cdap.api.lineage.field.Operation> processedOperations = processor.process();

    Set<co.cask.cdap.api.lineage.field.Operation> expectedOperations = new HashSet<>();

    co.cask.cdap.api.lineage.field.ReadOperation read
            = new co.cask.cdap.api.lineage.field.ReadOperation("n1.read", "reading from file", source, "offset",
                                                               "body");

    expectedOperations.add(read);

    co.cask.cdap.api.lineage.field.TransformOperation parse
            = new co.cask.cdap.api.lineage.field.TransformOperation("n2.parse", "parsing body",
                  Collections.singletonList(InputField.of("n1.read", "body")), "id", "name", "address", "zip");

    expectedOperations.add(parse);

    co.cask.cdap.api.lineage.field.WriteOperation infoWrite
            = new co.cask.cdap.api.lineage.field.WriteOperation("n3.infoWrite", "writing info", info,
            InputField.of("n2.parse", "id"), InputField.of("n2.parse", "name"));

    expectedOperations.add(infoWrite);

    co.cask.cdap.api.lineage.field.WriteOperation locationWrite
            = new co.cask.cdap.api.lineage.field.WriteOperation("n4.locationWrite", "writing location", location,
            InputField.of("n2.parse", "address"), InputField.of("n2.parse", "zip"));

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

    Map<String, List<Operation>> stageOperations = new HashMap<>();
    List<Operation> operations = new ArrayList<>();
    operations.add(new ReadOperation("pRead", "Reading from person file", pEndPoint, "offset", "body"));
    stageOperations.put("n1", operations);

    operations = new ArrayList<>();
    operations.add(new ReadOperation("hRead", "Reading from hr file", hEndPoint, "offset", "body"));
    stageOperations.put("n2", operations);

    operations = new ArrayList<>();
    operations.add(new WriteOperation("write1", "Writing to test store", testEndPoint, "offset", "body"));
    stageOperations.put("n3", operations);

    operations = new ArrayList<>();
    operations.add(new WriteOperation("write2", "Writing to prod store", prodEndPoint, "offset", "body"));
    stageOperations.put("n4", operations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          new HashSet<>());
    Set<co.cask.cdap.api.lineage.field.Operation> processedOperations = processor.process();

    Set<co.cask.cdap.api.lineage.field.Operation> expectedOperations = new HashSet<>();
    co.cask.cdap.api.lineage.field.ReadOperation pRead
            = new co.cask.cdap.api.lineage.field.ReadOperation("n1.pRead", "Reading from person file", pEndPoint,
                                                               "offset", "body");
    expectedOperations.add(pRead);

    co.cask.cdap.api.lineage.field.ReadOperation hRead
            = new co.cask.cdap.api.lineage.field.ReadOperation("n2.hRead", "Reading from hr file", hEndPoint,
                                                               "offset", "body");
    expectedOperations.add(hRead);

    // implicit merge should be added by app
    co.cask.cdap.api.lineage.field.TransformOperation merge
            = new co.cask.cdap.api.lineage.field.TransformOperation("n1.n2.merge", "Merging stages: n1,n2",
            Arrays.asList(InputField.of("n1.pRead", "offset"), InputField.of("n1.pRead", "body"),
                    InputField.of("n2.hRead", "offset"), InputField.of("n2.hRead", "body")), "offset", "body");
    expectedOperations.add(merge);

    co.cask.cdap.api.lineage.field.WriteOperation write1
            = new co.cask.cdap.api.lineage.field.WriteOperation("n3.write1", "Writing to test store", testEndPoint,
                Arrays.asList(InputField.of("n1.n2.merge", "offset"), InputField.of("n1.n2.merge", "body")));
    expectedOperations.add(write1);

    co.cask.cdap.api.lineage.field.WriteOperation write2
            = new co.cask.cdap.api.lineage.field.WriteOperation("n4.write2", "Writing to prod store", prodEndPoint,
            Arrays.asList(InputField.of("n1.n2.merge", "offset"), InputField.of("n1.n2.merge", "body")));
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

    Map<String, List<Operation>> stageOperations = new HashMap<>();
    List<Operation> operations = new ArrayList<>();
    operations.add(new ReadOperation("read", "reading file 1", n1EndPoint, "offset", "body"));
    stageOperations.put("n1", operations);

    operations = new ArrayList<>();
    operations.add(new TransformOperation("parse", "parsing file 1", Collections.singletonList("body"), "name",
                                          "address", "zip"));
    stageOperations.put("n2", operations);

    operations = new ArrayList<>();
    operations.add(new ReadOperation("read", "reading file 2", n3EndPoint, "offset", "body"));
    stageOperations.put("n3", operations);

    operations = new ArrayList<>();
    operations.add(new TransformOperation("parse", "parsing file 2", Collections.singletonList("body"), "name",
            "address", "zip"));
    stageOperations.put("n4", operations);

    operations = new ArrayList<>();
    operations.add(new TransformOperation("normalize", "normalizing address", Collections.singletonList("address"),
                                          "address"));
    operations.add(new TransformOperation("rename", "renaming address to state_address",
                                          Collections.singletonList("address"), "state_address"));
    stageOperations.put("n5", operations);

    operations = new ArrayList<>();
    operations.add(new WriteOperation("write", "writing file 3", n6EndPoint, "offset", "name", "address"));
    stageOperations.put("n6", operations);

    operations = new ArrayList<>();
    operations.add(new TransformOperation("rename", "renaming offset to file_offset",
                                          Collections.singletonList("offset"), "file_offset"));
    stageOperations.put("n7", operations);

    operations = new ArrayList<>();
    operations.add(new WriteOperation("write", "writing file 4", n8EndPoint, "file_offset", "name", "address", "zip"));
    stageOperations.put("n8", operations);

    LineageOperationsProcessor processor = new LineageOperationsProcessor(connections, stageOperations,
                                                                          new HashSet<>());
    Set<co.cask.cdap.api.lineage.field.Operation> processedOperations = processor.process();
    Set<co.cask.cdap.api.lineage.field.Operation> expectedOperations = new HashSet<>();

    co.cask.cdap.api.lineage.field.ReadOperation read
            = new co.cask.cdap.api.lineage.field.ReadOperation("n1.read", "reading file 1", n1EndPoint, "offset",
                                                               "body");
    expectedOperations.add(read);

    co.cask.cdap.api.lineage.field.TransformOperation parse
            = new co.cask.cdap.api.lineage.field.TransformOperation("n2.parse", "parsing file 1",
            Collections.singletonList(InputField.of("n1.read", "body")), "name", "address", "zip");
    expectedOperations.add(parse);

    read = new co.cask.cdap.api.lineage.field.ReadOperation("n3.read", "reading file 2", n3EndPoint, "offset",
                                                            "body");
    expectedOperations.add(read);

    parse = new co.cask.cdap.api.lineage.field.TransformOperation("n4.parse", "parsing file 2",
            Collections.singletonList(InputField.of("n3.read", "body")), "name", "address", "zip");
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

    co.cask.cdap.api.lineage.field.TransformOperation merge
            = new co.cask.cdap.api.lineage.field.TransformOperation("n2.n4.merge",
            "Merging stages: n2,n4", inputsToMerge, "offset", "body", "name", "address", "zip");
    expectedOperations.add(merge);

    co.cask.cdap.api.lineage.field.TransformOperation normalize
            = new co.cask.cdap.api.lineage.field.TransformOperation("n5.normalize", "normalizing address",
              Collections.singletonList(InputField.of("n2.n4.merge", "address")), "address");
    expectedOperations.add(normalize);

    co.cask.cdap.api.lineage.field.TransformOperation rename
            = new co.cask.cdap.api.lineage.field.TransformOperation("n5.rename",
            "renaming address to state_address",
            Collections.singletonList(InputField.of("n5.normalize", "address")), "state_address");
    expectedOperations.add(rename);

    co.cask.cdap.api.lineage.field.WriteOperation write
            = new co.cask.cdap.api.lineage.field.WriteOperation("n6.write", "writing file 3", n6EndPoint,
            InputField.of("n2.n4.merge", "offset"), InputField.of("n2.n4.merge", "name"),
            InputField.of("n5.normalize", "address"));
    expectedOperations.add(write);

    rename = new co.cask.cdap.api.lineage.field.TransformOperation("n7.rename", "renaming offset to file_offset",
            Collections.singletonList(InputField.of("n2.n4.merge", "offset")), "file_offset");
    expectedOperations.add(rename);

    write = new co.cask.cdap.api.lineage.field.WriteOperation("n8.write", "writing file 4", n8EndPoint,
            InputField.of("n7.rename", "file_offset"), InputField.of("n2.n4.merge", "name"),
            InputField.of("n5.normalize", "address"), InputField.of("n2.n4.merge", "zip"));
    expectedOperations.add(write);

    Assert.assertEquals(expectedOperations, processedOperations);
  }
}
