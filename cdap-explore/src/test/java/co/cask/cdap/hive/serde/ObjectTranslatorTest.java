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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class ObjectTranslatorTest {

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
    }

    private static List<String> getFieldNames() {
      return Lists.newArrayList(
        "booleanField",
        "byteField",
        "charField",
        "shortField",
        "intField",
        "longField",
        "floatField",
        "doubleField",
        "stringField",
        "uriField",
        "urlField",
        "bytesField",
        "byteBufferField",
        "uuidField"
      );
    }

    private static List<TypeInfo> getFieldTypes() {
      return Lists.<TypeInfo>newArrayList(
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
        TypeInfoFactory.binaryTypeInfo
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
        uuidField
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
        "mapField",
        "listField",
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
    // include all types that are the same in our schema as they are in Hive
    Map<Object, TypeInfo> inputs = Maps.newHashMap();
    inputs.put("foobar", TypeInfoFactory.stringTypeInfo);
    inputs.put(5, TypeInfoFactory.intTypeInfo);
    inputs.put(Long.MAX_VALUE, TypeInfoFactory.longTypeInfo);
    inputs.put(true, TypeInfoFactory.booleanTypeInfo);
    inputs.put(3.14f, TypeInfoFactory.floatTypeInfo);
    inputs.put(3.14, TypeInfoFactory.doubleTypeInfo);
    inputs.put(Bytes.toBytes("foobar"), TypeInfoFactory.binaryTypeInfo);
    for (Map.Entry<Object, TypeInfo> input : inputs.entrySet()) {
      Object field = input.getKey();
      TypeInfo type = input.getValue();
      Assert.assertEquals(field, ObjectTranslator.translateField(field, type));
    }
  }

  @Test
  public void testIntFieldTranslations() throws Exception {
    Byte byteVal = Byte.MAX_VALUE;
    Character charVal = Character.MAX_VALUE;
    Short shortVal = Short.MAX_VALUE;
    Assert.assertEquals(byteVal.intValue(), ObjectTranslator.translateField(byteVal, TypeInfoFactory.intTypeInfo));
    Assert.assertEquals((int) charVal,
                        ObjectTranslator.translateField(charVal, TypeInfoFactory.intTypeInfo));
    Assert.assertEquals(shortVal.intValue(),
                        ObjectTranslator.translateField(shortVal, TypeInfoFactory.intTypeInfo));
  }

  @Test
  public void testURLAndURITranslation() throws Exception {
    String str = "http://abc.com/123";
    Assert.assertEquals(str, ObjectTranslator.translateField(new URL(str), TypeInfoFactory.stringTypeInfo));
    Assert.assertEquals(str, ObjectTranslator.translateField(new URI(str), TypeInfoFactory.stringTypeInfo));
  }

  @Test
  public void testByteBufferTranslation() throws Exception {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
    byte[] translated = (byte[]) ObjectTranslator.translateField(byteBuffer, TypeInfoFactory.binaryTypeInfo);
    Assert.assertTrue(Arrays.equals(new byte[]{1, 2, 3}, translated));

    // check to make sure bytes before the position are not copied
    byteBuffer.get();
    translated = (byte[]) ObjectTranslator.translateField(byteBuffer, TypeInfoFactory.binaryTypeInfo);
    Assert.assertTrue(Arrays.equals(new byte[]{2, 3}, translated));
  }

  @Test
  public void testUUIDTranslation() throws Exception {
    UUID uuid = UUID.randomUUID();
    byte[] translated = (byte[]) ObjectTranslator.translateField(uuid, TypeInfoFactory.binaryTypeInfo);
    Assert.assertTrue(Arrays.equals(Bytes.toBytes(uuid), translated));
    Assert.assertEquals(uuid, Bytes.toUUID(translated));
  }

  @Test
  public void testListTranslation() throws Exception {
    List<String> list = Lists.newArrayList("foo", "bar", "baz");
    Assert.assertEquals(list, ObjectTranslator.translateField(
      list, TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo)));
  }

  @Test
  public void testMapTranslation() throws Exception {
    Map<Character, URL> input = Maps.newHashMap();
    input.put('a', new URL("http://abc.com"));
    input.put('1', new URL("http://123.com"));
    Map<Integer, String> expected = Maps.newHashMap();
    expected.put((int) 'a', "http://abc.com");
    expected.put((int) '1', "http://123.com");
    Assert.assertEquals(expected, ObjectTranslator.translateField(
      input, TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo)));
  }

  @Test
  public void testFlattenSimpleRecord() throws Exception {
    SimpleRecord simpleRecord = new SimpleRecord(new URI("http://abc.com"), new URL("http://123.com"));
    HiveSimpleRecord hiveSimpleRecord = new HiveSimpleRecord(simpleRecord);
    List<String> fieldNames = HiveSimpleRecord.getFieldNames();
    List<TypeInfo> fieldTypes = HiveSimpleRecord.getFieldTypes();
    List<Object> expected = hiveSimpleRecord.getAsList();
    List<Object> translated = ObjectTranslator.flattenRecord(simpleRecord, fieldNames, fieldTypes);
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
      .build();

    // create the Hive version of the record
    HiveSimpleRecord hiveSimpleRecord = new HiveSimpleRecord(simpleRecord);
    List<String> fieldNames = HiveSimpleRecord.getFieldNames();
    List<TypeInfo> fieldTypes = HiveSimpleRecord.getFieldTypes();
    List<Object> expected = hiveSimpleRecord.getAsList();
    // flatten the StructuredRecord into a list of objects
    List<Object> translated = ObjectTranslator.flattenRecord(structuredRecord, fieldNames, fieldTypes);
    assertSimpleRecordEquals(expected, translated);
  }

  @Test
  public void testNestedRecord() throws Exception {
    NestedRecord nestedRecord = new NestedRecord();
    List<Object> translated =
      ObjectTranslator.flattenRecord(nestedRecord, NestedRecord.getFieldNames(), NestedRecord.getFieldTypes());
    List<Object> expected = nestedRecord.getAsList();
    // first 2 fields are a map and list and can be compared directly
    Assert.assertEquals(expected.get(0), translated.get(0));
    Assert.assertEquals(expected.get(1), translated.get(1));
    // 3rd field is a list of objects which contains byte arrays and must be compared separately.
    assertSimpleRecordEquals((List<Object>) expected.get(2), (List<Object>) translated.get(2));
  }

  private void assertSimpleRecordEquals(List<Object> expected, List<Object> actual) {
    // compare the non-array fields
    Assert.assertEquals(expected.subList(0, expected.size() - 3),
                        actual.subList(0, actual.size() - 3));
    // compare the array fields
    for (int i = expected.size() - 1; i > expected.size() - 4; i--) {
      Assert.assertTrue(Arrays.equals((byte[]) expected.get(i), (byte[]) actual.get(i)));
    }
  }
}
