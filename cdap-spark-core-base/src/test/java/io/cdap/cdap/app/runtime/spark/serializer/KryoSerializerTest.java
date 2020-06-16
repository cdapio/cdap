/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Unit tests for various Kryo serializers in CDAP.
 */
public class KryoSerializerTest {

  @Test
  public void testUnmodifiableSortedSetSerializer() {
    SortedSet<String> tempSortedSet = new TreeSet<>();
    tempSortedSet.addAll(Arrays.asList("This is a test string in a list".split(" ")));
    SortedSet unmodifableSortedSet = Collections.unmodifiableSortedSet(tempSortedSet);

    Kryo kryo = new Kryo();
    Class<?> sortedSetClass = Collections.unmodifiableSortedSet(new TreeSet<>()).getClass();
    kryo.addDefaultSerializer(sortedSetClass, UnmodifiableSortedSetSerializer.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, unmodifableSortedSet);
    }

    Input input = new Input(bos.toByteArray());
    SortedSet newSortedSet = (SortedSet) kryo.readObject(input, sortedSetClass);

    Assert.assertEquals(unmodifableSortedSet, newSortedSet);
  }

  @Test
  public void testUnmodifiableCollectionSerializer() {
    Collection unmodifableCollection = Collections
      .unmodifiableCollection(Arrays.asList("This is a test string in a list".split(" ")));

    Kryo kryo = new Kryo();
    Class<?> collectionClass = Collections.unmodifiableCollection(new LinkedList<>()).getClass();
    kryo.addDefaultSerializer(collectionClass, UnmodifiableCollectionSerializer.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, unmodifableCollection);
    }

    Input input = new Input(bos.toByteArray());
    Collection newCollection = (Collection) kryo.readObject(input, collectionClass);

    Assert.assertEquals(unmodifableCollection.toArray(), newCollection.toArray());
  }

  @Test
  public void testUnmodifiableSetSerializer() {
    Set<String> tempSet = new HashSet<String>();
    tempSet.addAll(Arrays.asList("This is a test string in a list".split(" ")));
    Set unmodifableSet = Collections.unmodifiableSet(tempSet);

    Kryo kryo = new Kryo();
    Class<?> setClass = Collections.unmodifiableSet(new HashSet<>()).getClass();
    kryo.addDefaultSerializer(setClass, UnmodifiableSetSerializer.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, unmodifableSet);
    }

    Input input = new Input(bos.toByteArray());
    Set newSet = (Set) kryo.readObject(input, setClass);

    Assert.assertEquals(unmodifableSet, newSet);
  }

  @Test
  public void testUnmodifiableListSerializer() {
    List unmodifableList = Collections.unmodifiableList(Arrays.asList("This is a test string in a list".split(" ")));

    Kryo kryo = new Kryo();
    Class<?> listClass = Collections.unmodifiableList(new LinkedList<>()).getClass();
    kryo.addDefaultSerializer(listClass, UnmodifiableListSerializer.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, unmodifableList);
    }

    Input input = new Input(bos.toByteArray());
    List newList = (List) kryo.readObject(input, listClass);

    Assert.assertEquals(unmodifableList, newList);
  }

  @Test
  public void testUnmodifiableMapSerializer() {
    HashMap<Integer, Integer> squaresMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      squaresMap.put(i, i * i);
    }

    Map<Integer, Integer> unmodifiableMap = Collections.unmodifiableMap(squaresMap);
    Kryo kryo = new Kryo();
    Class<?> mapClass = Collections.unmodifiableMap(new HashMap<>()).getClass();
    kryo.addDefaultSerializer(mapClass, UnmodifiableMapSerializer.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, unmodifiableMap);
    }

    Input input = new Input(bos.toByteArray());
    Map newMap = (Map) kryo.readObject(input, mapClass);

    Assert.assertEquals(unmodifiableMap, newMap);
  }

  @Test
  public void testUnmodifiableSortedMapSerializer() {
    SortedMap<Integer, Integer> squaresMap = new TreeMap<>();
    for (int i = 0; i < 10; i++) {
      squaresMap.put(i, i * i);
    }

    SortedMap<Integer, Integer> unmodifiableSortedMap = Collections.unmodifiableSortedMap(squaresMap);
    Kryo kryo = new Kryo();
    Class<?> sortedMapClass = Collections.unmodifiableSortedMap(new TreeMap<>()).getClass();
    kryo.addDefaultSerializer(sortedMapClass, UnmodifiableSortedMapSerializer.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, unmodifiableSortedMap);
    }

    Input input = new Input(bos.toByteArray());
    SortedMap newSortedMap = (SortedMap) kryo.readObject(input, sortedMapClass);

    Assert.assertEquals(unmodifiableSortedMap, newSortedMap);
  }

  @Test
  public void testSchemaSerializer() {
    Schema schema = createSchema();

    Kryo kryo = new Kryo();
    kryo.addDefaultSerializer(Schema.class, SchemaSerializer.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, schema);
    }

    Input input = new Input(bos.toByteArray());
    Schema newSchema = kryo.readObject(input, Schema.class);

    Assert.assertEquals(schema, newSchema);
  }

  @Test
  public void testStructuredRecordSerializer() throws IOException {
    Schema schema = createSchema();

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("boolean", true)
      .set("int", 10)
      .set("long", 1L + Integer.MAX_VALUE)
      .set("float", 1.5f)
      .set("double", 2.25d)
      .set("string", "Hello World")
      .set("bytes", "Hello Bytes".getBytes(StandardCharsets.UTF_8))
      .set("enum", "a")
      .set("array", new int[]{1, 2, 3})
      .set("map", ImmutableMap.of("1", 1, "2", 2, "3", 3))
      .set("union", null).build();

    Kryo kryo = new Kryo();
    kryo.addDefaultSerializer(Schema.class, SchemaSerializer.class);
    kryo.addDefaultSerializer(StructuredRecord.class, StructuredRecordSerializer.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, record);
    }

    Input input = new Input(bos.toByteArray());
    StructuredRecord newRecord = kryo.readObject(input, StructuredRecord.class);

    // The StructuredRecord.equals is broken, Json it and compare for now
    Assert.assertEquals(StructuredRecordStringConverter.toJsonString(record),
                        StructuredRecordStringConverter.toJsonString(newRecord));
  }

  private Schema createSchema() {
    return Schema.recordOf("record",
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
  }
}
