/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.TextNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
  public static final class Node4 {
    private static final Schema SCHEMA = Schema.recordOf(
      Node4.class.getName(),
      Schema.Field.of("data", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    private String data;

  }

  /**
   * More and more and more node.
   */
  public static final class Node5 {
    private static final Schema SCHEMA = Schema.recordOf(
      Node5.class.getName(),
      Schema.Field.of("x", Schema.nullableOf(Node4.SCHEMA)));
    private Node4 x;
  }

  /**
   * Tests a record as a field, and that record as an inner field of another record.
   */
  public static final class Node6 {
    private static final Schema SCHEMA = Schema.recordOf(
      Node6.class.getName(),
      Schema.Field.of("x", Schema.nullableOf(Node4.SCHEMA)),
      Schema.Field.of("y", Schema.nullableOf(Node5.SCHEMA)));
    private Node4 x;
    private Node5 y;
  }

  @Test
  public void testAvroEnumSchema() throws Exception {
    org.apache.avro.Schema schema = org.apache.avro.Schema.createEnum("UserInterests", "Describes interests of user",
                                                                        "org.example.schema",
                                                                        ImmutableList.of("CRICKET", "BASEBALL"));

    Schema parsedSchema = Schema.parseJson(schema.toString());
    Assert.assertEquals(ImmutableSet.of("CRICKET", "BASEBALL"), parsedSchema.getEnumValues());
  }

  @Test
  public void testAvroRecordSchema() throws Exception {
    org.apache.avro.Schema avroStringSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
    org.apache.avro.Schema avroIntSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);


    org.apache.avro.Schema schema = org.apache.avro.Schema.createRecord("UserInfo", "Describes user information",
                                                                        "org.example.schema", false);

    List<org.apache.avro.Schema.Field> fields = new ArrayList<>();

    org.apache.avro.Schema.Field field = new org.apache.avro.Schema.Field("username", avroStringSchema,
                                                                          "Field represents username",
                                                                          new TextNode("unknown"));
    fields.add(field);

    field = new org.apache.avro.Schema.Field("age", avroIntSchema, "Field represents age of user",
                                             new IntNode(-1));
    fields.add(field);

    schema.setFields(fields);
    Schema parsedSchema = Schema.parseJson(schema.toString());
    Assert.assertTrue("UserInfo".equals(parsedSchema.getRecordName()));
    Assert.assertEquals(2, parsedSchema.getFields().size());
    Schema.Field parsedField = parsedSchema.getFields().get(0);
    Assert.assertTrue("username".equals(parsedField.getName()));
    Assert.assertTrue("STRING".equals(parsedField.getSchema().getType().toString()));
    parsedField = parsedSchema.getFields().get(1);
    Assert.assertTrue("age".equals(parsedField.getName()));
    Assert.assertTrue("INT".equals(parsedField.getSchema().getType().toString()));
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
  public void testSameRecordDifferentLevels() throws UnsupportedTypeException, IOException {
    Schema actual = new ReflectionSchemaGenerator().generate(Node6.class);
    Assert.assertEquals(Node6.SCHEMA, actual);
    // check serialization and deserialization.
    Assert.assertEquals(Node6.SCHEMA, Schema.parseJson(actual.toString()));
  }

  @Test
  public void testFieldIgnoreCase() {
    Schema schema = Schema.recordOf("record", Schema.Field.of("firstName", Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(Schema.Type.STRING, schema.getField("firstName").getSchema().getType());
    Assert.assertNull(schema.getField("firstname"));
    Assert.assertEquals(Schema.Type.STRING, schema.getField("firstname", true).getSchema().getType());
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
      "array_field array<string> not null, " +
      "map_field map<string,int> not null, " +
      "record_field record<x:int,y:double>, " +
      "string_field string";
    Schema expected = Schema.recordOf(
      "rec",
      Schema.Field.of("bool_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("int_field", Schema.of(Schema.Type.INT)),
      Schema.Field.of("long_field", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("float_field", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("double_field", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("bytes_field", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("array_field", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))),
      Schema.Field.of("map_field", Schema.mapOf(
        Schema.nullableOf(Schema.of(Schema.Type.STRING)),
        Schema.nullableOf(Schema.of(Schema.Type.INT)))),
      Schema.Field.of("record_field", Schema.nullableOf(Schema.recordOf(
        "rec1",
        Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.INT))),
        Schema.Field.of("y", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))))),
      Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
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
    String schemaStr = "map_field map< string , int >   not null,\n" +
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

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("enum", Schema.enumWith("a", "b", "c")),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.INT))),
      Schema.Field.of("union", Schema.unionOf(Schema.of(Schema.Type.NULL), Schema.of(Schema.Type.STRING)))
    );

    // Trigger the computation of the schemaString field
    String schemaString = schema.toString();

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(os)) {
      oos.writeObject(schema);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray()))) {
      Schema restoredSchema = (Schema) ois.readObject();
      Assert.assertEquals(schema, restoredSchema);
      Assert.assertEquals(schemaString, restoredSchema.toString());
    }
  }

  /**
   * Parent class for testing recursive schema.
   */
  private static final class NodeParent {
    private List<NodeChild> children;
  }

  /**
   * Child class for testing recursive schema.
   */
  private static final class NodeChild {
    private int data;
    private NodeParent parent;
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testGeneratorRecursion() throws Exception {
    // This test the schema generator be able to handle recursive schema between multiple classes
    Schema generatedSchema = new ReflectionSchemaGenerator(false).generate(NodeParent.class);

    // Validate the NodeChild.parent schema is the same as the schema of the Parent class
    // The schema generator always generate array value as nullable union
    Schema childSchema = generatedSchema.getField("children").getSchema().getComponentSchema().getNonNullable();
    Assert.assertEquals(generatedSchema, childSchema.getField("parent").getSchema());

    // Serialize the schema and then parse it back
    Schema deserializedSchema = Schema.parseJson(generatedSchema.toString());
    Assert.assertEquals(generatedSchema, deserializedSchema);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testRecursion() throws Exception {
    // Test simple recursion
    Schema nodeSchema = Schema.recordOf("Node",
                                        Schema.Field.of("data", Schema.of(Schema.Type.INT)),
                                        Schema.Field.of("next", Schema.recordOf("Node")));
    Assert.assertEquals(nodeSchema, nodeSchema.getField("next").getSchema());

    // Serialize and deserialize and validate
    Assert.assertEquals(nodeSchema, Schema.parseJson(nodeSchema.toString()));
    Assert.assertEquals(nodeSchema, Schema.parseJson(nodeSchema.toString()).getField("next").getSchema());

    // Test multi-levels recursion. It has the following structure:
    // Parent {
    //   List<Child> children
    // }
    // Child {
    //   GrandChild child
    // }
    // GrandChild {
    //   Child parent
    //   Parent grandParent
    // }

    Schema parentSchema = Schema.recordOf(
      "Parent", Schema.Field.of("children", Schema.arrayOf(Schema.recordOf(
        "Child", Schema.Field.of("children", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.recordOf(
          "GrandChild", Schema.Field.of("parent", Schema.recordOf("Child")),
          Schema.Field.of("grandParent", Schema.recordOf("Parent"))
        )))))));

    Schema childSchema = parentSchema.getField("children").getSchema().getComponentSchema();
    Schema grandChildSchema = childSchema.getField("children").getSchema().getMapSchema().getValue();
    Assert.assertEquals(parentSchema, grandChildSchema.getField("grandParent").getSchema());
    Assert.assertEquals(childSchema, grandChildSchema.getField("parent").getSchema());

    // Serialize and deserialize it back and validate again
    parentSchema = Schema.parseJson(parentSchema.toString());
    Assert.assertEquals(parentSchema, grandChildSchema.getField("grandParent").getSchema());
    Assert.assertEquals(childSchema, grandChildSchema.getField("parent").getSchema());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testUnionRecordReference() throws Exception {
    // Test records defined earlier in an union can be referred by name in schema defined later in the union.
    Schema first = Schema.recordOf("First", Schema.Field.of("data", Schema.of(Schema.Type.INT)));
    Schema second = Schema.recordOf("Second", Schema.Field.of("firsts", Schema.arrayOf(Schema.recordOf("First"))));

    Schema union = Schema.unionOf(first, second);

    // Validate the schema are resolved
    Assert.assertEquals(first, union.getUnionSchemas().get(1).getField("firsts").getSchema().getComponentSchema());

    // Serialize and deserialize it back and validate again
    union = Schema.parseJson(union.toString());
    Assert.assertEquals(first, union.getUnionSchemas().get(1).getField("firsts").getSchema().getComponentSchema());
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
