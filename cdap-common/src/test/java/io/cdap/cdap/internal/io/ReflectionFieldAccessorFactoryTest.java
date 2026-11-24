/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.internal.io;

import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ReflectionFieldAccessorFactory}.
 */
public class ReflectionFieldAccessorFactoryTest {

  private static class BaseHolder<T> {
    private static final String CONSTANT = "immutable";
    private T genericValue;
  }

  private static final class PrimitivesAndRefHolder extends BaseHolder<String> {
    private boolean boolVal;
    private byte byteVal;
    private char charVal;
    private short shortVal;
    private int intVal;
    private long longVal;
    private float floatVal;
    private double doubleVal;
  }

  @Test
  public void testAllPrimitiveAndGenericAccessors() {
    ReflectionFieldAccessorFactory factory = new ReflectionFieldAccessorFactory();
    TypeToken<PrimitivesAndRefHolder> type = TypeToken.of(PrimitivesAndRefHolder.class);
    PrimitivesAndRefHolder target = new PrimitivesAndRefHolder();

    FieldAccessor genericAcc = factory.getFieldAccessor(type, "genericValue");
    Assert.assertSame(genericAcc, factory.getFieldAccessor(type, "genericValue"));
    Assert.assertEquals(String.class, genericAcc.getType());
    genericAcc.set(target, "hello");
    Assert.assertEquals("hello", genericAcc.get(target));

    factory.getFieldAccessor(type, "boolVal").setBoolean(target, true);
    factory.getFieldAccessor(type, "byteVal").setByte(target, (byte) 12);
    factory.getFieldAccessor(type, "charVal").setChar(target, 'Z');
    factory.getFieldAccessor(type, "shortVal").setShort(target, (short) 321);
    factory.getFieldAccessor(type, "intVal").setInt(target, 4567);
    factory.getFieldAccessor(type, "longVal").setLong(target, 9876543210L);
    factory.getFieldAccessor(type, "floatVal").setFloat(target, 1.5f);
    factory.getFieldAccessor(type, "doubleVal").setDouble(target, 2.75d);

    Assert.assertTrue(factory.getFieldAccessor(type, "boolVal").getBoolean(target));
    Assert.assertEquals((byte) 12, factory.getFieldAccessor(type, "byteVal").getByte(target));
    Assert.assertEquals('Z', factory.getFieldAccessor(type, "charVal").getChar(target));
    Assert.assertEquals((short) 321, factory.getFieldAccessor(type, "shortVal").getShort(target));
    Assert.assertEquals(4567, factory.getFieldAccessor(type, "intVal").getInt(target));
    Assert.assertEquals(9876543210L, factory.getFieldAccessor(type, "longVal").getLong(target));
    Assert.assertEquals(1.5f, factory.getFieldAccessor(type, "floatVal").getFloat(target), 0.0001f);
    Assert.assertEquals(2.75d,
        factory.getFieldAccessor(type, "doubleVal").getDouble(target), 0.0001d);
  }

  @Test
  public void testSetAndGetUncheckedAndCheckedExceptionPropagation() {
    ReflectionFieldAccessorFactory factory = new ReflectionFieldAccessorFactory();
    TypeToken<PrimitivesAndRefHolder> type = TypeToken.of(PrimitivesAndRefHolder.class);
    FieldAccessor intAccessor = factory.getFieldAccessor(type, "intVal");

    // 1. Wrong instance type triggers unchecked IllegalArgumentException, rethrown unwrapped.
    try {
      intAccessor.get("wrongInstance");
      Assert.fail("Expected IllegalArgumentException unwrapped on get");
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      intAccessor.set("wrongInstance", 10);
      Assert.fail("Expected IllegalArgumentException unwrapped on set");
    } catch (IllegalArgumentException expected) {
      // expected
    }

    // 2. Mutating a static final field triggers checked IllegalAccessException, wrapped in RuntimeException.
    FieldAccessor constAccessor = factory.getFieldAccessor(type, "CONSTANT");
    try {
      constAccessor.set(new PrimitivesAndRefHolder(), "changed");
      Assert.fail("Expected RuntimeException wrapping IllegalAccessException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof IllegalAccessException);
    }
  }
}
