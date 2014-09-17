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

package co.cask.cdap.io;

import co.cask.cdap.internal.io.ASMFieldAccessorFactory;
import co.cask.cdap.internal.io.FieldAccessor;
import co.cask.cdap.internal.io.FieldAccessorFactory;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class FieldAccessorTest {

  /**
   * Test parent.
   * @param <T>
   */
  public static class Parent<T> {
    private T value;
    private boolean b;
  }

  /**
   * Test child.
   */
  public static class Child extends Parent<String> {
    protected String str;
    int integer;
  }

  @Test
  public void testGetter() {
    TypeToken<Child> type = TypeToken.of(Child.class);

    FieldAccessorFactory factory = new ASMFieldAccessorFactory();
    FieldAccessor accessor = factory.getFieldAccessor(type, "integer");

    Assert.assertSame(accessor, factory.getFieldAccessor(type, "integer"));

    Child c = new Child();
    c.integer = 10;
    c.str = "child value";
    ((Parent<String>) c).value = "string value";
    ((Parent<String>) c).b = true;

    Assert.assertEquals(c.integer, accessor.getInt(c));
    Assert.assertSame(c.str, factory.getFieldAccessor(type, "str").get(c));
    Assert.assertSame(((Parent) c).value, factory.getFieldAccessor(type, "value").get(c));
    Assert.assertEquals(((Parent) c).b, factory.getFieldAccessor(type, "b").get(c));
  }

  @Test
  public void testSetter() {
    TypeToken<Child> type = TypeToken.of(Child.class);

    FieldAccessorFactory factory = new ASMFieldAccessorFactory();

    Child c = new Child();
    c.integer = 10;
    c.str = "child value";
    ((Parent<String>) c).value = "string value";
    ((Parent<String>) c).b = true;

    Child c2 = new Child();

    factory.getFieldAccessor(type, "integer").setInt(c2, c.integer);
    factory.getFieldAccessor(type, "str").set(c2, c.str);
    factory.getFieldAccessor(type, "value").set(c2, ((Parent) c).value);
    factory.getFieldAccessor(type, "b").setBoolean(c2, ((Parent) c).b);

    Assert.assertEquals(c.integer, c2.integer);
    Assert.assertSame(c.str, c2.str);
    Assert.assertSame(((Parent) c).value, ((Parent) c2).value);
    Assert.assertEquals(((Parent) c).b, ((Parent) c2).b);
  }
}
