package com.continuuity.io;

import com.continuuity.api.io.BinaryDecoder;
import com.continuuity.api.io.BinaryEncoder;
import com.continuuity.api.io.ReflectionSchemaGenerator;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.UnsupportedTypeException;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.Charset;
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

    public Record1(int i, Map<Integer, Value> properties) {
      this.i = i;
      this.properties = properties;
    }
  }

  public static class Record2 {
    private final Long i;
    private final Map<String, Value> properties;
    private final String name;

    public Record2(long i, Map<String, Value> properties, String name) {
      this.i = i;
      this.properties = properties;
      this.name = name;
    }
  }

  @Test
  public void test() throws IOException, UnsupportedTypeException {
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
  }
}
