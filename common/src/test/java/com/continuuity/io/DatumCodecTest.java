/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.io;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.TypeRepresentation;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 *
 */
public class DatumCodecTest {

  /**
   *
   */
  public static class Value {
    private final int id;
    private final String name;

    public Value(int id, String name) {
      this.id = id;
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Value value = (Value) o;
      return id == value.id && name.equals(value.name);
    }

    @Override
    public int hashCode() {
      int result = id;
      result = 31 * result + name.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "Value{" +
        "id=" + id +
        ", name='" + name + '\'' +
        '}';
    }
  }

  /**
   *
   */
  public static class Record1 {
    private final int i;
    private final Map<Integer, Value> properties;
    private final int[] numbers;
    private final URL url;
    private final UUID uuid;
    private final String nullStr;

    public Record1(int i, Map<Integer, Value> properties, URL url) {
      this.i = i;
      this.properties = properties;
      this.numbers = new int[] {1, 2};
      this.url = url;
      this.uuid = UUID.randomUUID();
      this.nullStr = null;
    }
  }

  /**
   *
   */
  public static class Record2 {
    private final Long i;
    private final Map<String, Value> properties;
    private final String name;
    private final long[] numbers;
    private final URI url;
    private final UUID uuid;
    private final String nullStr;

    public Record2(long i, Map<String, Value> properties, String name) {
      this.i = i;
      this.properties = properties;
      this.name = name;
      this.numbers = new long[0];
      this.url = null;
      this.uuid = null;
      this.nullStr = null;
    }
  }

  @Test
  public void testTypeProject() throws IOException, UnsupportedTypeException {
    final Record1 r1 = new Record1(10, Maps.<Integer, Value>newHashMap(), new URL("http://www.yahoo.com"));
    r1.properties.put(1, new Value(1, "Name1"));
    r1.properties.put(2, new Value(2, "Name2"));
    r1.properties.put(3, null);

    PipedOutputStream output = new PipedOutputStream();
    PipedInputStream input = new PipedInputStream(output);

    Schema sourceSchema = new ReflectionSchemaGenerator().generate(Record1.class);
    Schema targetSchema = new ReflectionSchemaGenerator().generate(Record2.class);

    new ReflectionDatumWriter<Record1>(sourceSchema).encode(r1, new BinaryEncoder(output));
    Record2 r2 = new ReflectionDatumReader<Record2>(targetSchema, TypeToken.of(Record2.class))
                            .read(new BinaryDecoder(input), sourceSchema);

    Assert.assertEquals(10L, r2.i.longValue());

    Assert.assertTrue(Iterables.all(r2.properties.entrySet(), new Predicate<Map.Entry<String, Value>>() {
      @Override
      public boolean apply(Map.Entry<String, Value> input) {
        Value value = r1.properties.get(Integer.valueOf(input.getKey()));
        return (value == null && input.getValue() == null) || (value.equals(input.getValue()));
      }
    }));

    Assert.assertNull(r2.name);

    Assert.assertArrayEquals(new long[] {1L, 2L}, r2.numbers);
    Assert.assertEquals(URI.create("http://www.yahoo.com"), r2.url);

    Assert.assertEquals(r1.uuid, r2.uuid);
  }

  @Test
  public void testCollection() throws UnsupportedTypeException, IOException {
    List<String> list = Lists.newArrayList("1", "2", "3");
    Schema sourceSchema = new ReflectionSchemaGenerator().generate(new TypeToken<List<String>>() { }.getType());
    Schema targetSchema = new ReflectionSchemaGenerator().generate(new TypeToken<Set<String>>() { }.getType());

    PipedOutputStream output = new PipedOutputStream();
    PipedInputStream input = new PipedInputStream(output);

    new ReflectionDatumWriter<List<String>>(sourceSchema).encode(list, new BinaryEncoder(output));
    Set<String> set = new ReflectionDatumReader<Set<String>>(targetSchema, new TypeToken<Set<String>>() { })
                        .read(new BinaryDecoder(input), sourceSchema);

    Assert.assertEquals(Sets.newHashSet("1", "2", "3"), set);


    targetSchema = new ReflectionSchemaGenerator().generate(String[].class);
    new ReflectionDatumWriter<List<String>>(sourceSchema).encode(list, new BinaryEncoder(output));
    String[] array = new ReflectionDatumReader<String[]>(targetSchema, new TypeToken<String[]>() { })
                        .read(new BinaryDecoder(input), sourceSchema);

    Assert.assertArrayEquals(new String[]{"1", "2", "3"}, array);
  }

  /**
   *
   */
  public static final class Node {
    int d;
    Node next;
  }

  @Test(expected = IOException.class)
  public void testCircularRef() throws UnsupportedTypeException, IOException {
    Schema schema = new ReflectionSchemaGenerator().generate(Node.class);

    Node head = new Node();
    head.next = new Node();
    head.next.next = head;

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    new ReflectionDatumWriter<Node>(schema).encode(head, new BinaryEncoder(output));
  }


  /**
   *
   */
  public static final class MoreFields {

    static final class Inner {
      final Map<String, String> map;
      final String b;

      Inner(String b) {
        this.b = b;
        map = ImmutableMap.of("b", b);
      }
    }

    final int i;
    final double d;
    final String k;
    final List<String> list;
    final Inner inner;

    public MoreFields(int i, double d, String k, List<String> list) {
      this.i = i;
      this.d = d;
      this.k = k;
      this.list = list;
      inner = new Inner("inner");
    }
  }

  /**
   *
   */
  public static final class LessFields {
    static final class Inner {
      String b;
    }

    String k;
    Inner inner;
  }

  @Test
  public void testReduceProjection() throws IOException, UnsupportedTypeException {
    PipedOutputStream output = new PipedOutputStream();
    PipedInputStream input = new PipedInputStream(output);

    Schema sourceSchema = new ReflectionSchemaGenerator().generate(MoreFields.class);
    Schema targetSchema = new ReflectionSchemaGenerator().generate(LessFields.class);

    MoreFields moreFields = new MoreFields(10, 20.2, "30", ImmutableList.of("1", "2"));
    new ReflectionDatumWriter<MoreFields>(sourceSchema).encode(moreFields, new BinaryEncoder(output));
    LessFields lessFields = new ReflectionDatumReader<LessFields>(targetSchema, TypeToken.of(LessFields.class))
                                            .read(new BinaryDecoder(input), sourceSchema);

    Assert.assertEquals("30", lessFields.k);
    Assert.assertEquals(moreFields.inner.b, lessFields.inner.b);
  }

  /**
   *
   */
  public static enum TestEnum {
    VALUE1, VALUE2, VALUE3
  }

  @Test
  public void testEnum() throws IOException, UnsupportedTypeException {
    PipedOutputStream output = new PipedOutputStream();
    PipedInputStream input = new PipedInputStream(output);

    Schema schema = new ReflectionSchemaGenerator().generate(TestEnum.class);
    ReflectionDatumWriter<TestEnum> writer = new ReflectionDatumWriter<TestEnum>(schema);
    BinaryEncoder encoder = new BinaryEncoder(output);
    writer.encode(TestEnum.VALUE1, encoder);
    writer.encode(TestEnum.VALUE3, encoder);
    writer.encode(TestEnum.VALUE2, encoder);

    BinaryDecoder decoder = new BinaryDecoder(input);
    ReflectionDatumReader<TestEnum> reader = new ReflectionDatumReader<TestEnum>(schema, TypeToken.of(TestEnum.class));

    Assert.assertEquals(TestEnum.VALUE1, reader.read(decoder, schema));
    Assert.assertEquals(TestEnum.VALUE3, reader.read(decoder, schema));
    Assert.assertEquals(TestEnum.VALUE2, reader.read(decoder, schema));
  }

  // this tests that the datum reader treats empty fields correctly. It reproduces the issue in ENG-2404.
  @Test
  public void testEmptyValue() throws UnsupportedTypeException, IOException {
    Schema schema = new ReflectionSchemaGenerator().generate(RecordWithString.class);
    TypeRepresentation typeRep = new TypeRepresentation(RecordWithString.class);
    DatumWriter<RecordWithString> datumWriter = new ReflectionDatumWriter<RecordWithString>(schema);
    @SuppressWarnings("unchecked")
    ReflectionDatumReader<RecordWithString> datumReader = new ReflectionDatumReader<RecordWithString>(
      schema, (TypeToken<RecordWithString>) TypeToken.of(typeRep.toType()));

    RecordWithString record = new RecordWithString();
    record.setA(42);
    record.setTheString("");

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(bos);
    datumWriter.encode(record, encoder);
    byte[] bytes = bos.toByteArray();

    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = new BinaryDecoder(bis);
    RecordWithString rec = datumReader.read(decoder, schema);

    Assert.assertEquals(record.getA(), rec.getA());
    Assert.assertEquals(record.getTheString(), rec.getTheString());
  }
}

// dummy class for testEmptyValue()
class RecordWithString {
  int a;

  int getA() {
    return a;
  }

  void setA(int a) {
    this.a = a;
  }

  private String theString;

  String getTheString() {
    return theString;
  }

  void setTheString(String theString) {
    this.theString = theString;
  }
}

