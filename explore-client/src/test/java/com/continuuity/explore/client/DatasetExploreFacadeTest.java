package com.continuuity.explore.client;

import com.continuuity.common.utils.ImmutablePair;
import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Test DatasetExploreFacade.
 */
public class DatasetExploreFacadeTest {
  enum Enum { FOO, BAR }

  @SuppressWarnings("unused")
  static class Record {
    int a;
    long b;
    boolean c;
    float d;
    double e;
    String f;
    byte[] g;
    Enum[] h;
    Collection<Boolean> i;
    Map<Integer, String> j;
  }

  @SuppressWarnings("unused")
  static class Int {
    Integer value;
  }

  @SuppressWarnings("unused")
  static class Longg {
    long value;
  }

  public static class KeyValue {
    private final String key;
    private final Value value;

    public KeyValue(String key, Value value) {
      this.key = key;
      this.value = value;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getKey() {
      return key;
    }

    @SuppressWarnings("UnusedDeclaration")
    public Value getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      KeyValue that = (KeyValue) o;

      return Objects.equal(this.key, that.key) &&
        Objects.equal(this.value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key, value);
    }

    public static class Value {
      private final String name;
      private final List<Integer> ints;

      public Value(String name, List<Integer> ints) {
        this.name = name;
        this.ints = ints;
      }

      @SuppressWarnings("UnusedDeclaration")
      public String getName() {
        return name;
      }

      @SuppressWarnings("UnusedDeclaration")
      public List<Integer> getInts() {
        return ints;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }

        Value that = (Value) o;

        return Objects.equal(this.name, that.name) &&
          Objects.equal(this.ints, that.ints);
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(name, ints);
      }
    }
  }

  @Test
  public void testHiveSchemaFor() throws Exception {

    Assert.assertEquals("(value INT)", DatasetExploreFacade.hiveSchemaFor(Int.class));
    Assert.assertEquals("(value BIGINT)", DatasetExploreFacade.hiveSchemaFor(Longg.class));
    Assert.assertEquals("(first INT, second STRING)",
                        DatasetExploreFacade.hiveSchemaFor(new TypeToken<ImmutablePair<Integer, String>>() {
                        }.getType()));
    Assert.assertEquals("(a INT, b BIGINT, c BOOLEAN, d FLOAT, e DOUBLE, f STRING, g BINARY, " +
                          "h ARRAY<STRING>, i ARRAY<BOOLEAN>, j MAP<INT,STRING>)",
                        DatasetExploreFacade.hiveSchemaFor(Record.class));

    Assert.assertEquals("(key STRING, value STRUCT<ints:ARRAY<INT>, name:STRING>)",
                        DatasetExploreFacade.hiveSchemaFor(KeyValue.class));
  }

}
