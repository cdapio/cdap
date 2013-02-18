/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.io;

import com.continuuity.api.io.Schema;
import com.continuuity.api.io.UnsupportedTypeException;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;

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
  }

  public static class Record1 {
    private final int i;
    private final Map<Integer, Value> properties;
    private final int[] numbers;

    public Record1(int i, Map<Integer, Value> properties) {
      this.i = i;
      this.properties = properties;
      this.numbers = new int[] {1, 2};
    }
  }

  public static class Record2 {
    private final Long i;
    private final Map<String, Value> properties;
    private final String name;
    private final long[] numbers;

    public Record2(long i, Map<String, Value> properties, String name) {
      this.i = i;
      this.properties = properties;
      this.name = name;
      this.numbers = new long[0];
    }
  }

  @Test
  public void testTypeProject() throws IOException, UnsupportedTypeException {
    Record1 r1 = new Record1(10, Maps.<Integer, Value>newHashMap());
    r1.properties.put(1, new Value(1, "Name1"));
    r1.properties.put(2, new Value(2, "Name2"));

    PipedOutputStream output = new PipedOutputStream();
    PipedInputStream input = new PipedInputStream(output);

    Schema sourceSchema = new ReflectionSchemaGenerator().generate(Record1.class);
    Schema targetSchema = new ReflectionSchemaGenerator().generate(Record2.class);

    new ReflectionDatumWriter(sourceSchema).write(r1, new BinaryEncoder(output));
    Record2 r2 = new ReflectionDatumReader<Record2>(targetSchema, TypeToken.of(Record2.class))
                            .read(new BinaryDecoder(input), sourceSchema);

    Assert.assertEquals(10L, r2.i.longValue());
    Assert.assertEquals(ImmutableMap.of("1", new Value(1, "Name1"), "2", new Value(2, "Name2")), r2.properties);
    Assert.assertNull(r2.name);

    Assert.assertArrayEquals(new long[] {1L, 2L}, r2.numbers);
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
