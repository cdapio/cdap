/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.io;

import com.continuuity.internal.api.io.Schema;
import com.continuuity.internal.api.io.UnsupportedTypeException;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

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

  public static class Value {
    private final int id;
    private final String name;

    public Value(int id, String name) {
      this.id = id;
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Value value = (Value) o;

      if (id != value.id) return false;
      if (!name.equals(value.name)) return false;

      return true;
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

    new ReflectionDatumWriter(sourceSchema).write(r1, new BinaryEncoder(output));
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
    Schema sourceSchema = new ReflectionSchemaGenerator().generate(new TypeToken<List<String>>() {}.getType());
    Schema targetSchema = new ReflectionSchemaGenerator().generate(new TypeToken<Set<String>>() {}.getType());

    PipedOutputStream output = new PipedOutputStream();
    PipedInputStream input = new PipedInputStream(output);

    new ReflectionDatumWriter(sourceSchema).write(list, new BinaryEncoder(output));
    Set<String> set = new ReflectionDatumReader<Set<String>>(targetSchema, new TypeToken<Set<String>>() {})
                        .read(new BinaryDecoder(input), sourceSchema);

    Assert.assertEquals(Sets.newHashSet("1", "2", "3"), set);


    targetSchema = new ReflectionSchemaGenerator().generate(String[].class);
    new ReflectionDatumWriter(sourceSchema).write(list, new BinaryEncoder(output));
    String[] array = new ReflectionDatumReader<String[]>(targetSchema, new TypeToken<String[]>() {})
                        .read(new BinaryDecoder(input), sourceSchema);

    Assert.assertArrayEquals(new String[]{"1", "2", "3"}, array);
  }

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
    new ReflectionDatumWriter(schema).write(head, new BinaryEncoder(output));
  }
}
