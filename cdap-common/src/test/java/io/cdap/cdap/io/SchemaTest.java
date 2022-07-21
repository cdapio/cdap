/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.SchemaCache;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.commons.lang.SerializationUtils;
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
import java.util.Arrays;
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
  public void testSchemaEquals() throws UnsupportedTypeException {
    Schema s1 = new ReflectionSchemaGenerator().generate(Node.class);
    Schema s2 = new ReflectionSchemaGenerator().generate(Node.class);

    Assert.assertEquals(s1.getSchemaHash(), s2.getSchemaHash());
    Assert.assertEquals(s1, s2);

    Schema schema = (new ReflectionSchemaGenerator()).generate((new TypeToken<Child<Node>>() { }).getType());
    Assert.assertNotEquals(s1.getSchemaHash(), schema.getSchemaHash());
  }

  @Test
  public void testSchemaNotEquals() {
    Schema r1 = Schema.recordOf("Test1", Schema.Field.of("field", Schema.of(Schema.Type.STRING)));
    Schema r2 = Schema.recordOf("Test2", Schema.Field.of("field", Schema.of(Schema.Type.STRING)));

    // Record schema of different name should be different
    Assert.assertNotEquals(r1, r2);

    // Nested record schema should be non-equal if the nested names are different
    r1 = Schema.recordOf(
      "Test",
      Schema.Field.of("field", Schema.recordOf("Nested1", Schema.Field.of("f", Schema.of(Schema.Type.STRING))))
    );
    r2 = Schema.recordOf(
      "Test",
      Schema.Field.of("field", Schema.recordOf("Nested2", Schema.Field.of("f", Schema.of(Schema.Type.STRING))))
    );
    Assert.assertNotEquals(r1, r2);
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

  @Test
  public void testLogicalTypes() throws Exception {
    Schema dateType = Schema.nullableOf(Schema.of(Schema.LogicalType.DATE));
    Schema timeMicrosType = Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS));
    Schema timeMillisType = Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MILLIS));
    Schema tsMicrosType = Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    Schema tsMillisType = Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS));
    Schema dateTimeType = Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME));

    Assert.assertEquals(dateType, Schema.parseJson(dateType.toString()));
    Assert.assertEquals(timeMicrosType, Schema.parseJson(timeMicrosType.toString()));
    Assert.assertEquals(timeMillisType, Schema.parseJson(timeMillisType.toString()));
    Assert.assertEquals(tsMicrosType, Schema.parseJson(tsMicrosType.toString()));
    Assert.assertEquals(tsMillisType, Schema.parseJson(tsMillisType.toString()));
    Assert.assertEquals(convertSchema(dateType).toString(), dateType.toString());
    Assert.assertEquals(dateTimeType, Schema.parseJson(dateTimeType.toString()));

    Schema complexSchema = Schema.recordOf(
      "union",
      Schema.Field.of("a", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                          Schema.arrayOf(Schema.of(Schema.Type.STRING)))),
      Schema.Field.of("b", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                          Schema.arrayOf(Schema.of(Schema.LogicalType.DATE)))),
      Schema.Field.of("c", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                          Schema.enumWith("something"))),
      Schema.Field.of("d", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                          Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))),
      Schema.Field.of("e", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                          Schema.mapOf(Schema.of(Schema.LogicalType.DATE),
                                                       Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)))),
      Schema.Field.of("f", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("g", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
      Schema.Field.of("h", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
      Schema.Field.of("i", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))));

    Assert.assertEquals(complexSchema, Schema.parseJson(complexSchema.toString()));
  }

  @Test
  public void testAvroLogicalTypeSchema() throws IOException {
    Schema schema = Schema.recordOf("Record",
                                    Schema.Field.of("id", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                                                         Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("name", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                                                           Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("date", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                                                           Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("time_millis", Schema.unionOf(
                                      Schema.of(Schema.Type.NULL), Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("time_micros", Schema.unionOf(
                                      Schema.of(Schema.Type.NULL), Schema.of(Schema.LogicalType.TIME_MICROS))),
                                    Schema.Field.of("timestamp_millis", Schema.unionOf(
                                      Schema.of(Schema.Type.NULL), Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
                                    Schema.Field.of("timestamp_micros", Schema.unionOf(
                                      Schema.of(Schema.Type.NULL), Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                                    Schema.Field.of("datetime", Schema.unionOf(
                                      Schema.of(Schema.Type.NULL), Schema.of(Schema.LogicalType.DATETIME))),
                                    Schema.Field.of("union",
                                                    Schema.unionOf(Schema.of(Schema.Type.STRING),
                                                                   Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    org.apache.avro.Schema avroSchema = convertSchema(schema);
    org.apache.avro.Schema expectedAvroSchema = org.apache.avro.Schema.createRecord("Record", null, null, false);

    ImmutableList<org.apache.avro.Schema.Field> fields = ImmutableList.of(
      new org.apache.avro.Schema.Field("id",
                                       org.apache.avro.Schema.createUnion(ImmutableList.of(
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT))), null, null),
      new org.apache.avro.Schema.Field("name",
                                       org.apache.avro.Schema.createUnion(ImmutableList.of(
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))),
                                       null, null),
      new org.apache.avro.Schema.Field("date",
                                       org.apache.avro.Schema.createUnion(ImmutableList.of(
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                                         LogicalTypes.date().addToSchema(
                                           org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)))),
                                       null, null),
      new org.apache.avro.Schema.Field("time_millis",
                                       org.apache.avro.Schema.createUnion(ImmutableList.of(
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                                         LogicalTypes.timeMillis().addToSchema(
                                           org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)))),
                                       null, null),
      new org.apache.avro.Schema.Field("time_micros",
                                       org.apache.avro.Schema.createUnion(ImmutableList.of(
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                                         LogicalTypes.timeMicros().addToSchema(
                                           org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)))),
                                       null, null),
      new org.apache.avro.Schema.Field("timestamp_millis",
                                       org.apache.avro.Schema.createUnion(ImmutableList.of(
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                                         LogicalTypes.timestampMillis().addToSchema(
                                           org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)))),
                                       null, null),
      new org.apache.avro.Schema.Field("timestamp_micros",
                                       org.apache.avro.Schema.createUnion(ImmutableList.of(
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                                         LogicalTypes.timestampMicros().addToSchema(
                                           org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)))),
                                       null, null),
      new org.apache.avro.Schema.Field("datetime",
                                       org.apache.avro.Schema.createUnion(ImmutableList.of(
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                                         new LogicalType("datetime").addToSchema(
                                           org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)))),
                                       null, null),
      new org.apache.avro.Schema.Field("union",
                                       org.apache.avro.Schema.createUnion(ImmutableList.of(
                                         org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                                         LogicalTypes.timestampMicros().addToSchema(
                                           org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)))),
                                       null, null));

    expectedAvroSchema.setFields(fields);

    Assert.assertEquals(expectedAvroSchema, avroSchema);
    Assert.assertEquals(schema, Schema.parseJson(avroSchema.toString()));
  }

  @Test
  public void testLogicalTypeEquals() {
    Schema timestampMillis = Schema.Field.of("timestamp_millis", Schema.unionOf(
      Schema.of(Schema.Type.NULL), Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))).getSchema();
    Schema timestampMicros = Schema.Field.of("timestamp_micros", Schema.unionOf(
      Schema.of(Schema.Type.NULL), Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))).getSchema();
    Assert.assertNotEquals(timestampMillis, timestampMicros);
    Assert.assertEquals(timestampMicros, timestampMicros);
  }

  @Test
  public void testSchemaDisplayName() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("bytes", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("decimal", Schema.decimalOf(10, 5)),
                                    Schema.Field.of("ts_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("ts_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("time_micros", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                    Schema.Field.of("time_millis", Schema.of(Schema.LogicalType.TIME_MILLIS)),
                                    Schema.Field.of("datetime", Schema.of(Schema.LogicalType.DATETIME)),
                                    Schema.Field.of("map",
                                                    Schema.nullableOf(Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                                   Schema.of(Schema.Type.INT)))),
                                    Schema.Field.of("union", Schema.unionOf(Schema.of(Schema.Type.STRING))));

    Assert.assertEquals("bytes", schema.getField("bytes").getSchema().getNonNullable().getDisplayName());
    Assert.assertEquals("string", schema.getField("string").getSchema().getDisplayName());
    Assert.assertEquals("date", schema.getField("date").getSchema().getNonNullable().getDisplayName());
    Assert.assertEquals("decimal with precision 10 and scale 5",
                        schema.getField("decimal").getSchema().getDisplayName());
    Assert.assertEquals("timestamp in microseconds", schema.getField("ts_micros").getSchema().getDisplayName());
    Assert.assertEquals("timestamp in milliseconds", schema.getField("ts_millis").getSchema().getDisplayName());
    Assert.assertEquals("time of day in microseconds", schema.getField("time_micros").getSchema().getDisplayName());
    Assert.assertEquals("time of day in milliseconds", schema.getField("time_millis").getSchema().getDisplayName());
    Assert.assertEquals("map", schema.getField("map").getSchema().getNonNullable().getDisplayName());
    Assert.assertEquals("union", schema.getField("union").getSchema().getDisplayName());
    Assert.assertEquals("datetime in ISO-8601 format without timezone",
                        schema.getField("datetime").getSchema().getDisplayName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDecimalSchema() {
    Schema.of(Schema.LogicalType.DECIMAL);
  }

  private org.apache.avro.Schema convertSchema(Schema cdapSchema) {
    return new org.apache.avro.Schema.Parser().parse(cdapSchema.toString());
  }

  @Test
  public void testCachedJavaSerialization() throws UnsupportedTypeException {
    Schema s1 = SchemaCache.intern(new ReflectionSchemaGenerator().generate(Node.class));
    Assert.assertSame(s1, SerializationUtils.clone(s1));
  }

  @Test
  public void testNamedEnums() throws Exception {
    List<String> enumValues = Arrays.asList("OK", "ERROR");
    org.apache.avro.Schema avroEnumSchema = org.apache.avro.Schema.createEnum("state", null, null, enumValues);
    List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
    fields.add(new org.apache.avro.Schema.Field("f1", avroEnumSchema, null, null));
    fields.add(new org.apache.avro.Schema.Field("f2", avroEnumSchema, null, null));
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("x", null, null, false, fields);
    // named enums will only define the full type once. All other times it will be referenced by the enum name
    String schemaStr = avroSchema.toString();

    // should be parsed correctly, remembering the named enum
    List<Schema.Field> expectedFields = new ArrayList<>();
    Schema enumSchema = Schema.enumWith("state", enumValues);
    expectedFields.add(Schema.Field.of("f1", enumSchema));
    expectedFields.add(Schema.Field.of("f2", enumSchema));
    Schema expected = Schema.recordOf("x", expectedFields);
    Schema schema = Schema.parseJson(schemaStr);
    Assert.assertEquals(expected, schema);

    // test that the enum name is used when writing out the schema
    Assert.assertEquals(expected.toString(), schemaStr);
  }

  @Test
  public void testCompatibleWithAvroMaps() throws Exception {
    // Avro maps always have String keys whereas CDAP allows the key to be a different type
    // Test that a map type without an explicit key type resolves to a String key type.
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createMap(
      org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));

    String schemaStr = avroSchema.toString();
    Schema schema = Schema.parseJson(schemaStr);
    Schema expected = Schema.mapOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)), Schema.of(Schema.Type.INT));
    Assert.assertEquals(expected, schema);
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
