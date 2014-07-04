package com.continuuity.explore.client;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
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

  public class Recursive {
    private final int a;
    private final Recursive b;
    private final int c;

    public Recursive(int a, Recursive b) {
      this.a = a;
      this.b = b;
      this.c = a;
    }
  }

  public class TransitiveRecursive {
    private final boolean empty;
    private final List<TransitiveRecursive> children;

    public TransitiveRecursive(boolean empty, List<TransitiveRecursive> children) {
      this.empty = empty;
      this.children = children;
    }
  }

  public class Value {
    private int a;
  }

  public class NotRecursive {
    private final Value a;
    private final Value b;

    public NotRecursive(Value a, Value b) {
      this.a = a;
      this.b = b;
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


  private void verifyUnsupportedSchema(Type type) {
    String schema;
    try {
      schema = DatasetExploreFacade.hiveSchemaFor(type);
    } catch (UnsupportedTypeException e) {
      // expected
      return;
    }
    Assert.fail("Type " + type + " should not be supported and cause exception but returned " + schema);
  }

  @Test
  public void testUnsupportedTypes() {
    verifyUnsupportedSchema(Integer.class);
    verifyUnsupportedSchema(String.class);
    verifyUnsupportedSchema(new TypeToken<List<Integer>>() { }.getType());
    verifyUnsupportedSchema(new TypeToken<Map<String, Integer>>() { }.getType());
    verifyUnsupportedSchema(Recursive.class);
    verifyUnsupportedSchema(TransitiveRecursive.class);
  }

  @Test
  public void testSupportedTypes() throws Exception {
    // Should not throw an exception
    DatasetExploreFacade.hiveSchemaFor(NotRecursive.class);
  }

}
