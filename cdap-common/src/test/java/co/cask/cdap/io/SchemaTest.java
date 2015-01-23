/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.io;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Test for schema generation.
 */
public class SchemaTest {

  /**
   * Test node.
   */
  public final class Node {
    private int data;
    private List<Node> children;
  }

  /**
   * Test parent.
   * @param <T> Parameter
   */
  public class Parent<T> {
    private T data;
    private ByteBuffer buffer;
  }

  /**
   * Test child.
   * @param <T> Paramter.
   */
  public class Child<T> extends Parent<Map<String, T>> {
    private int height;
    private Node rootNode;
    private State state;
  }

  /**
   * Test enum.
   */
  public enum State {
    OK, ERROR
  }


  @Test
  public void testGenerateSchema() throws UnsupportedTypeException {
    Schema schema = (new ReflectionSchemaGenerator()).generate((new TypeToken<Child<Node>>() { }).getType());

    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();

    Assert.assertEquals(schema, gson.fromJson(gson.toJson(schema), Schema.class));
  }

  /**
   * Testing more node.
   */
  public final class Node2 {
    private int data;
    private List<Node2> children;
  }

  @Test
  public void testSchemaHash() throws UnsupportedTypeException {
    Schema s1 = new ReflectionSchemaGenerator().generate(Node.class);
    Schema s2 = new ReflectionSchemaGenerator().generate(Node2.class);

    Assert.assertEquals(s1.getSchemaHash(), s2.getSchemaHash());
    Assert.assertEquals(s1, s2);

    Schema schema = (new ReflectionSchemaGenerator()).generate((new TypeToken<Child<Node>>() { }).getType());
    Assert.assertNotEquals(s1.getSchemaHash(), schema.getSchemaHash());
  }

  /**
   * Yet more node.
   */
  public final class Node3 {
    private long data;
    private String tag;
    private List<Node3> children;
  }

  /**
   * More and more node.
   */
  public final class Node4 {
    private String data;
  }

  @Test
  public void testCompatible() throws UnsupportedTypeException {
    Schema s1 = new ReflectionSchemaGenerator().generate(Node.class);
    Schema s2 = new ReflectionSchemaGenerator().generate(Node3.class);
    Schema s3 = new ReflectionSchemaGenerator().generate(Node4.class);

    Assert.assertNotEquals(s1, s2);
    Assert.assertTrue(s1.isCompatible(s2));
    Assert.assertFalse(s2.isCompatible(s1));

    Assert.assertTrue(s2.isCompatible(s3));
  }

  @Test
  public void testPrimitiveArray() throws UnsupportedTypeException {
    Schema schema = new ReflectionSchemaGenerator().generate(int[].class);
    Assert.assertEquals(Schema.arrayOf(Schema.of(Schema.Type.INT)), schema);
  }

  @Test
  public void testParseJson() throws IOException, UnsupportedTypeException {
    Schema schema = new ReflectionSchemaGenerator().generate(Node.class);
    Assert.assertEquals(schema, Schema.parseJson(schema.toString()));
  }

  @Test
  public void testParseFlatSQL() throws IOException {
    // simple, non-nested types
    String schemaStr = "bool_field boolean, " +
      "int_field int not null, " +
      "long_field long not null, " +
      "float_field float NOT NULL, " +
      "double_field double NOT NULL, " +
      "bytes_field bytes not null, " +
      "string_field string not null, " +
      "array_field array<string> not null, " +
      "map_field map<string,int> not null, " +
      "record_field record<x:int,y:double>";
    Schema expected = Schema.recordOf(
      "rec",
      Schema.Field.of("bool_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("int_field", Schema.of(Schema.Type.INT)),
      Schema.Field.of("long_field", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("float_field", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("double_field", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("bytes_field", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("array_field", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))),
      Schema.Field.of("map_field", Schema.mapOf(
        Schema.nullableOf(Schema.of(Schema.Type.STRING)),
        Schema.nullableOf(Schema.of(Schema.Type.INT)))),
      Schema.Field.of("record_field", Schema.nullableOf(Schema.recordOf(
        "rec1",
        Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.INT))),
        Schema.Field.of("y", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))))))
    );
    Assert.assertEquals(expected, Schema.parseSQL(schemaStr));
  }

  @Test
  public void testNestedSQL() throws IOException {
    Schema expected = Schema.recordOf(
      "rec",
      Schema.Field.of(
        "x",
        Schema.mapOf(
          Schema.recordOf("rec1",
                          // String x
                          Schema.Field.of("x", Schema.of(Schema.Type.STRING)),
                          // String[] y
                          Schema.Field.of("y", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
                          // Map<byte[],double> z
                          Schema.Field.of("z", Schema.mapOf(Schema.of(Schema.Type.BYTES),
                                                            Schema.of(Schema.Type.DOUBLE)))),
          Schema.arrayOf(Schema.recordOf(
            "rec2",
            Schema.Field.of("x",
                            // Map<array<byte[]>, Map<boolean,byte[]> x
                            Schema.mapOf(Schema.arrayOf(Schema.of(Schema.Type.BYTES)),
                                         Schema.mapOf(Schema.of(Schema.Type.BOOLEAN),
                                                      Schema.of(Schema.Type.BYTES)))
          )))
        )),
      Schema.Field.of("y", Schema.of(Schema.Type.INT)));
    String schemaStr =
      "x map<" +
        "record<" +
          "x:string not null," +
          "y:array<string not null> not null," +
          "z:map<bytes not null,double not null> not null" +
        "> not null," +
        "array<" +
          "record<" +
            "x:map<" +
              "array<bytes not null> not null," +
              "map<boolean not null,bytes not null> not null" +
            "> not null" +
          "> not null" +
        "> not null" +
      "> not null, y int not null";
    Assert.assertEquals(expected, Schema.parseSQL(schemaStr));
  }

  @Test
  public void testParseSQLWithWhitespace() throws IOException {
    String schemaStr = "map_field map< string , int >   not  null,\n" +
      "arr_field array< record< x:int , y:double >\t> not null";
    Schema expectedSchema = Schema.recordOf(
      "rec",
      Schema.Field.of("map_field", Schema.mapOf(
        Schema.nullableOf(Schema.of(Schema.Type.STRING)), Schema.nullableOf(Schema.of(Schema.Type.INT)))),
      Schema.Field.of("arr_field",
                      Schema.arrayOf(Schema.nullableOf(
                        Schema.recordOf("rec1",
                                        Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                        Schema.Field.of("y", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))))))
    );
    Assert.assertEquals(expectedSchema, Schema.parseSQL(schemaStr));
  }

  @Test
  public void testInvalidSQL() {
    verifyThrowsException("int x");
    verifyThrowsException("x map<int, int");
    verifyThrowsException("x array<string");
    verifyThrowsException("x bool");
    verifyThrowsException("x integer");
    verifyThrowsException("x record<y int>");
    verifyThrowsException("x array<>");
  }

  private void verifyThrowsException(String toParse) {
    try {
      Schema.parseSQL(toParse);
      Assert.fail();
    } catch (IOException e) {
      // expected
    }
  }
}
