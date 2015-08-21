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

package co.cask.cdap.explore.table;

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SchemaConverterTest {

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

  @SuppressWarnings("unused")
  enum Foo { FOO, BAR }

  @SuppressWarnings("unused")
  static class Record {
    int a;
    long b;
    boolean c;
    float d;
    double e;
    String f;
    byte[] g;
    Foo[] h;
    Collection<Boolean> i;
    Map<Integer, String> j;
  }

  @SuppressWarnings("unused")
  static class Record2 {
    String s2;
    Record record;
  }

  @SuppressWarnings("unused")
  static class Record3 {
    int i3;
    Record2 record2;
  }

  @SuppressWarnings("unused")
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

  @SuppressWarnings("unused")
  public class TransitiveRecursive {
    private final boolean empty;
    private final List<TransitiveRecursive> children;

    public TransitiveRecursive(boolean empty, List<TransitiveRecursive> children) {
      this.empty = empty;
      this.children = children;
    }
  }

  @SuppressWarnings("unused")
  public class Value {
    private int a;
  }

  @SuppressWarnings("unused")
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
    Assert.assertEquals("(a int, b bigint, c boolean, d float, e double, f string, g binary, " +
                          "h array<string>, i array<boolean>, j map<int,string>)",
                        SchemaConverter.toHiveSchema(Record.class));

    Assert.assertEquals("(key string, value struct<ints:array<int>,name:string>)",
                        SchemaConverter.toHiveSchema(KeyValue.class));

    Assert.assertEquals("(i3 int, record2 struct<" +
                          "record:struct<a:int,b:bigint,c:boolean,d:float,e:double,f:string,g:binary," +
                            "h:array<string>,i:array<boolean>,j:map<int,string>>," +
                          "s2:string>)",
                        SchemaConverter.toHiveSchema(Record3.class));
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
    SchemaConverter.toHiveSchema(NotRecursive.class);
  }

  private void verifyUnsupportedSchema(Type type) {
    String schema;
    try {
      schema = SchemaConverter.toHiveSchema(type);
    } catch (UnsupportedTypeException e) {
      // expected
      return;
    }
    Assert.fail("Type " + type + " should not be supported and cause exception but returned " + schema);
  }
}
