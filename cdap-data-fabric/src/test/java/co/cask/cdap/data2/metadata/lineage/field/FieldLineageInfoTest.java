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

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.proto.codec.OperationTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test for {@link FieldLineageInfo}
 */
public class FieldLineageInfoTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
    .create();

  @Test
  public void testInvalidOperations() throws Exception {
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse body",
                                                      Arrays.asList(InputField.of("read", "body")), "name", "address");
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
      Assert.assertTrue(e.getMessage().equals(msg));
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
      Assert.assertTrue(e.getMessage().equals(msg));
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
      Assert.assertTrue(e.getMessage().equals(msg));
    }
  }

  @Test
  public void testValidOperations() throws Exception {
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("endpoint1"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse body",
                                                      Arrays.asList(InputField.of("read", "body")), "name", "address");
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
    String infoJson = GSON.toJson(info1);
    FieldLineageInfo info2 = GSON.fromJson(infoJson, FieldLineageInfo.class);
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
}
