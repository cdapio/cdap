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

package co.cask.cdap.hive.serde;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.SchemaGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class ObjectDeserializerTest {
  private static final SchemaGenerator schemaGenerator = new ReflectionSchemaGenerator();

  // contains all types our Schema allows
  private static class SimpleRecord {
    final boolean booleanField = Boolean.TRUE;
    final byte byteField = Byte.MAX_VALUE;
    final char charField = Character.MAX_VALUE;
    final short shortField = Short.MAX_VALUE;
    final int intField = Integer.MAX_VALUE;
    final long longField = Long.MAX_VALUE;
    final float floatField = Float.MAX_VALUE;
    final double doubleField = Double.MAX_VALUE;
    final String stringField = "foobar";
    final byte[] bytesField = new byte[] { 1, 2, 3 };
    final ByteBuffer byteBufferField = ByteBuffer.wrap(new byte[] { 4, 5, 6 });
    final UUID uuidField = UUID.fromString("92633f3c-f358-47ef-89bd-2d8dd59a600d");
    URI uriField;
    URL urlField;
    final int[] intsField = new int[] { 1, 2, 3 };

    private SimpleRecord(URI uri, URL url) {
      this.uriField = uri;
      this.urlField = url;
    }
  }

  // Hive version of SimpleRecord, with types changed to expected types for Hive
  private static class HiveSimpleRecord {
    boolean booleanField = Boolean.TRUE;
    int byteField = Byte.MAX_VALUE;
    int charField = Character.MAX_VALUE;
    int shortField = Short.MAX_VALUE;
    int intField = Integer.MAX_VALUE;
    long longField = Long.MAX_VALUE;
    float floatField = Float.MAX_VALUE;
    double doubleField = Double.MAX_VALUE;
    String stringField = "foobar";
    byte[] bytesField = new byte[] { 1, 2, 3 };
    byte[] byteBufferField = new byte[] { 4, 5, 6 };
    byte[] uuidField;
    String uriField;
    String urlField;
    int[] intsField = new int[] { 1, 2, 3 };

    private HiveSimpleRecord(SimpleRecord r) {
      this.booleanField = r.booleanField;
      this.byteField = r.byteField;
      this.charField = r.charField;
      this.shortField = r.shortField;
      this.intField = r.intField;
      this.longField = r.longField;
      this.floatField = r.floatField;
      this.doubleField = r.doubleField;
      this.stringField = r.stringField;
      this.bytesField = r.bytesField;
      this.byteBufferField = Bytes.toBytes(r.byteBufferField);
      this.uuidField = Bytes.toBytes(r.uuidField);
      this.uriField = r.uriField.toString();
      this.urlField = r.urlField.toString();
      this.intsField = r.intsField;
    }

    // everything is lowercase to simulate hive
    private static List<String> getFieldNames() {
      return Lists.newArrayList(
        "booleanfield",
        "bytefield",
        "charfield",
        "shortfield",
        "intfield",
        "longfield",
        "floatfield",
        "doublefield",
        "stringfield",
        "urifield",
        "urlfield",
        "bytesfield",
        "bytebufferfield",
        "uuidfield",
        "intsfield"
      );
    }

    private static List<TypeInfo> getFieldTypes() {
      return Lists.newArrayList(
        TypeInfoFactory.booleanTypeInfo,
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.floatTypeInfo,
        TypeInfoFactory.doubleTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.binaryTypeInfo,
        TypeInfoFactory.binaryTypeInfo,
        TypeInfoFactory.binaryTypeInfo,
        TypeInfoFactory.getListTypeInfo(TypeInfoFactory.intTypeInfo)
      );
    }

    private List<Object> getAsList() {
      return Lists.<Object>newArrayList(
        booleanField,
        byteField,
        charField,
        shortField,
        intField,
        longField,
        floatField,
        doubleField,
        stringField,
        uriField,
        urlField,
        bytesField,
        byteBufferField,
        uuidField,
        intsField
      );
    }
  }

  public static class NestedRecord {
    final Map<Integer, String> mapField;
    final List<Boolean> listField;
    final SimpleRecord record;

    public NestedRecord() throws URISyntaxException, MalformedURLException {
      this.mapField = ImmutableMap.of(1, "1", 2, "2", 3, "3");
      this.listField = Lists.newArrayList(true, false, false);
      this.record = new SimpleRecord(new URI("http://abc.com"), new URL("http://123.com"));
    }

    private static List<String> getFieldNames() {
      return Lists.newArrayList(
        "mapfield",
        "listfield",
        "record"
      );
    }

    private static List<TypeInfo> getFieldTypes() {
      return Lists.newArrayList(
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo),
        TypeInfoFactory.getListTypeInfo(TypeInfoFactory.booleanTypeInfo),
        TypeInfoFactory.getStructTypeInfo(HiveSimpleRecord.getFieldNames(), HiveSimpleRecord.getFieldTypes())
      );
    }

    private List<Object> getAsList() {
      return Lists.newArrayList(
        mapField,
        listField,
        new HiveSimpleRecord(record).getAsList()
      );
    }
  }

  @Test
  public void testIdentityTranslations() throws Exception {
    List<String> names = Lists.newArrayList("dummy-name");
    // string
    ObjectDeserializer deserializer = new ObjectDeserializer(
      names, Lists.<TypeInfo>newArrayList(TypeInfoFactory.stringTypeInfo), Schema.of(Schema.Type.STRING));
    Assert.assertEquals("foobar", deserializer.deserialize("foobar"));
    // int
    deserializer = new ObjectDeserializer(
      names, Lists.<TypeInfo>newArrayList(TypeInfoFactory.intTypeInfo), Schema.of(Schema.Type.INT));
    Assert.assertEquals(Integer.MIN_VALUE, deserializer.deserialize(Integer.MIN_VALUE));
    // long
    deserializer = new ObjectDeserializer(
      names, Lists.<TypeInfo>newArrayList(TypeInfoFactory.longTypeInfo), Schema.of(Schema.Type.LONG));
    Assert.assertEquals(Long.MAX_VALUE, deserializer.deserialize(Long.MAX_VALUE));
    // boolean
    deserializer = new ObjectDeserializer(
      names, Lists.<TypeInfo>newArrayList(TypeInfoFactory.booleanTypeInfo), Schema.of(Schema.Type.BOOLEAN));
    Assert.assertTrue((Boolean) deserializer.deserialize(true));
    // float
    deserializer = new ObjectDeserializer(
      names, Lists.<TypeInfo>newArrayList(TypeInfoFactory.floatTypeInfo), Schema.of(Schema.Type.FLOAT));
    Assert.assertEquals(3.14f, deserializer.deserialize(3.14f));
    // double
    deserializer = new ObjectDeserializer(
      names, Lists.<TypeInfo>newArrayList(TypeInfoFactory.doubleTypeInfo), Schema.of(Schema.Type.DOUBLE));
    Assert.assertEquals(3.14, deserializer.deserialize(3.14));
    // bytes
    deserializer = new ObjectDeserializer(
      names, Lists.<TypeInfo>newArrayList(TypeInfoFactory.binaryTypeInfo), Schema.of(Schema.Type.BYTES));
    Assert.assertArrayEquals(new byte[] { 1, 2, 3 }, (byte[]) deserializer.deserialize(new byte[] { 1, 2, 3 }));
  }

  @Test
  public void testIntFieldTranslations() throws Exception {
    ObjectDeserializer translator = new ObjectDeserializer(
      Lists.newArrayList("dummy-name"),
      Lists.<TypeInfo>newArrayList(TypeInfoFactory.intTypeInfo),
      Schema.of(Schema.Type.INT));
    Byte byteVal = Byte.MAX_VALUE;
    Character charVal = Character.MAX_VALUE;
    Short shortVal = Short.MAX_VALUE;
    Assert.assertEquals(byteVal.intValue(), translator.deserialize(byteVal));
    Assert.assertEquals((int) charVal, translator.deserialize(charVal));
    Assert.assertEquals(shortVal.intValue(), translator.deserialize(shortVal));
  }

  @Test
  public void testURLAndURITranslation() throws Exception {
    ObjectDeserializer translator = new ObjectDeserializer(
      Lists.newArrayList("dummy-name"),
      Lists.<TypeInfo>newArrayList(TypeInfoFactory.stringTypeInfo),
      Schema.of(Schema.Type.STRING));
    String str = "http://abc.com/123";
    Assert.assertEquals(str, translator.deserialize(new URL(str)));
    Assert.assertEquals(str, translator.deserialize(new URI(str)));
  }

  @Test
  public void testByteBufferTranslation() throws Exception {
    ObjectDeserializer translator = new ObjectDeserializer(
      Lists.newArrayList("dummy-name"),
      Lists.<TypeInfo>newArrayList(TypeInfoFactory.binaryTypeInfo),
      Schema.of(Schema.Type.BYTES));
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
    byte[] translated = (byte[]) translator.deserialize(byteBuffer);
    Assert.assertArrayEquals(new byte[]{1, 2, 3}, translated);

    // check to make sure bytes before the position are not copied
    byteBuffer.get();
    translated = (byte[]) translator.deserialize(byteBuffer);
    Assert.assertArrayEquals(new byte[]{2, 3}, translated);
  }

  @Test
  public void testUUIDTranslation() throws Exception {
    ObjectDeserializer translator = new ObjectDeserializer(
      Lists.newArrayList("dummy-name"),
      Lists.<TypeInfo>newArrayList(TypeInfoFactory.binaryTypeInfo),
      Schema.of(Schema.Type.BYTES));
    UUID uuid = UUID.randomUUID();
    byte[] translated = (byte[]) translator.deserialize(uuid);
    Assert.assertArrayEquals(Bytes.toBytes(uuid), translated);
    Assert.assertEquals(uuid, Bytes.toUUID(translated));
  }

  @Test
  public void testListTranslation() throws Exception {
    ObjectDeserializer translator = new ObjectDeserializer(
      Lists.newArrayList("dummy-name"),
      Lists.newArrayList(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo)),
      Schema.arrayOf(Schema.of(Schema.Type.STRING)));
    List<String> list = Lists.newArrayList("foo", "bar", "baz");
    Assert.assertEquals(list, translator.deserialize(list));
  }

  @Test
  public void testMapTranslation() throws Exception {
    ObjectDeserializer translator = new ObjectDeserializer(
      Lists.newArrayList("dummy-name"),
      Lists.newArrayList(TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo)),
      Schema.mapOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.STRING)));
    Map<Character, URL> input = Maps.newHashMap();
    input.put('a', new URL("http://abc.com"));
    input.put('1', new URL("http://123.com"));
    Map<Integer, String> expected = Maps.newHashMap();
    expected.put((int) 'a', "http://abc.com");
    expected.put((int) '1', "http://123.com");
    Assert.assertEquals(expected, translator.deserialize(input));
  }

  @Test
  public void testFlattenSimpleRecord() throws Exception {
    SimpleRecord simpleRecord = new SimpleRecord(new URI("http://abc.com"), new URL("http://123.com"));
    HiveSimpleRecord hiveSimpleRecord = new HiveSimpleRecord(simpleRecord);
    List<String> fieldNames = HiveSimpleRecord.getFieldNames();
    List<TypeInfo> fieldTypes = HiveSimpleRecord.getFieldTypes();
    List<Object> expected = hiveSimpleRecord.getAsList();
    ObjectDeserializer translator =
      new ObjectDeserializer(fieldNames, fieldTypes, schemaGenerator.generate(SimpleRecord.class));
    List<Object> translated = translator.translateRecord(simpleRecord);
    assertSimpleRecordEquals(expected, translated);
  }

  @Test
  public void testFlattenSimpleStructuredRecord() throws Exception {
    SimpleRecord simpleRecord = new SimpleRecord(new URI("http://abc.com"), new URL("http://123.com"));
    Schema schema = new ReflectionSchemaGenerator().generate(SimpleRecord.class);
    StructuredRecord structuredRecord = StructuredRecord.builder(schema)
      .set("booleanField", simpleRecord.booleanField)
      .set("byteField", simpleRecord.byteField)
      .set("charField", simpleRecord.charField)
      .set("shortField", simpleRecord.shortField)
      .set("intField", simpleRecord.intField)
      .set("longField", simpleRecord.longField)
      .set("floatField", simpleRecord.floatField)
      .set("doubleField", simpleRecord.doubleField)
      .set("stringField", simpleRecord.stringField)
      .set("bytesField", simpleRecord.bytesField)
      .set("byteBufferField", simpleRecord.byteBufferField)
      .set("uuidField", simpleRecord.uuidField)
      .set("uriField", simpleRecord.uriField)
      .set("urlField", simpleRecord.urlField)
      .set("intsField", simpleRecord.intsField)
      .build();

    // create the Hive version of the record
    HiveSimpleRecord hiveSimpleRecord = new HiveSimpleRecord(simpleRecord);
    List<String> fieldNames = HiveSimpleRecord.getFieldNames();
    List<TypeInfo> fieldTypes = HiveSimpleRecord.getFieldTypes();
    List<Object> expected = hiveSimpleRecord.getAsList();
    // flatten the StructuredRecord into a list of objects
    ObjectDeserializer translator =
      new ObjectDeserializer(fieldNames, fieldTypes, schemaGenerator.generate(SimpleRecord.class));
    List<Object> translated = translator.translateRecord(structuredRecord);
    assertSimpleRecordEquals(expected, translated);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNestedRecord() throws Exception {
    NestedRecord nestedRecord = new NestedRecord();
    ObjectDeserializer translator = new ObjectDeserializer(
      NestedRecord.getFieldNames(), NestedRecord.getFieldTypes(), schemaGenerator.generate(NestedRecord.class));
    List<Object> translated = translator.translateRecord(nestedRecord);
    List<Object> expected = nestedRecord.getAsList();
    // first 2 fields are a map and list and can be compared directly
    Assert.assertEquals(expected.get(0), translated.get(0));
    Assert.assertEquals(expected.get(1), translated.get(1));
    // 3rd field is a list of objects which contains byte arrays and must be compared separately.
    assertSimpleRecordEquals((List<Object>) expected.get(2), (List<Object>) translated.get(2));
  }

  @SuppressWarnings("unchecked")
  private void assertSimpleRecordEquals(List<Object> expected, List<Object> actual) {
    // compare the non-array fields
    Assert.assertEquals(expected.subList(0, expected.size() - 4),
                        actual.subList(0, actual.size() - 4));
    // compare the byte array fields
    for (int i = expected.size() - 2; i > expected.size() - 5; i--) {
      Assert.assertArrayEquals((byte[]) expected.get(i), (byte[]) actual.get(i));
    }
    // compare the int array field, which becomes a list
    List<Integer> actualInts = (List<Integer>) actual.get(actual.size() - 1);
    int[] expectedInts = (int[]) expected.get(expected.size() - 1);
    for (int i = 0; i < expectedInts.length; i++) {
      Assert.assertEquals(expectedInts[i], (int) actualInts.get(i));
    }
  }
}
