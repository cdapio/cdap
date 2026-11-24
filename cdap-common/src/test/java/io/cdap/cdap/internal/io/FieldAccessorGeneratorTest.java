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

import io.cdap.cdap.internal.asm.ByteCodeClassLoader;
import io.cdap.cdap.internal.asm.ClassDefinition;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link FieldAccessorGenerator}.
 */
public class FieldAccessorGeneratorTest {

  private static final class PrivateTarget {
    private static final String IMMUTABLE = "constant";
    private int count;
    private String label;
  }

  /**
   * Public target for direct non-reflection bytecode access.
   */
  public static final class PublicTarget {
    public int number;
    public String text;
  }

  @Test
  public void testDirectAndPrivateFieldAccessors() throws Exception {
    // 1. Direct bytecode access on public fields.
    FieldAccessor numberAccessor = instantiate(
        PublicTarget.class, PublicTarget.class.getDeclaredField("number"),
        true, PublicTarget.class);
    FieldAccessor textAccessor = instantiate(
        PublicTarget.class, PublicTarget.class.getDeclaredField("text"),
        true, PublicTarget.class);

    PublicTarget pub = new PublicTarget();
    numberAccessor.setInt(pub, 55);
    textAccessor.set(pub, "direct");
    Assert.assertEquals(55, numberAccessor.getInt(pub));
    Assert.assertEquals(Integer.valueOf(55), numberAccessor.get(pub));
    Assert.assertEquals("direct", textAccessor.get(pub));

    // 2. Reflection-backed bytecode access on private primitive and object fields.
    FieldAccessor countAccessor = instantiate(
        PrivateTarget.class, PrivateTarget.class.getDeclaredField("count"),
        false, PrivateTarget.class);
    FieldAccessor labelAccessor = instantiate(
        PrivateTarget.class, PrivateTarget.class.getDeclaredField("label"),
        false, PrivateTarget.class);

    PrivateTarget priv = new PrivateTarget();
    countAccessor.setInt(priv, 123);
    labelAccessor.set(priv, "secret");
    Assert.assertEquals(123, countAccessor.getInt(priv));
    Assert.assertEquals(Integer.valueOf(123), countAccessor.get(priv));
    Assert.assertEquals("secret", labelAccessor.get(priv));
  }

  @Test
  public void testGeneratedCatchBlocksInConstructorAndSetter() throws Exception {
    // 1. Passing Object.class (which lacks 'label') into the generated constructor executes
    // the generated catch (Exception e) block in initializeReflectionField.
    Field labelField = PrivateTarget.class.getDeclaredField("label");
    try {
      instantiate(PrivateTarget.class, labelField, false, Object.class);
      Assert.fail("Expected InvocationTargetException from generated constructor catch block");
    } catch (InvocationTargetException e) {
      Throwable target = e.getTargetException();
      Assert.assertTrue(target instanceof RuntimeException);
      Assert.assertTrue(target.getCause() instanceof NoSuchFieldException);
    }

    // 2. Calling set() on a private static final field triggers IllegalAccessException inside
    // Field.set(), executing the generated catch (IllegalAccessException e) block in invokeReflection.
    Field immutableField = PrivateTarget.class.getDeclaredField("IMMUTABLE");
    FieldAccessor immutableAccessor = instantiate(
        PrivateTarget.class, immutableField, false, PrivateTarget.class);
    try {
      immutableAccessor.set(new PrivateTarget(), "mutated");
      Assert.fail("Expected RuntimeException wrapping IllegalAccessException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof IllegalAccessException);
    }
  }

  private FieldAccessor instantiate(Class<?> ownerType, Field field,
      boolean publicOnly, Type ctorArgType) throws Exception {
    ClassDefinition classDef = new FieldAccessorGenerator().generate(ownerType, field, publicOnly);
    ByteCodeClassLoader loader = new ByteCodeClassLoader(getClass().getClassLoader());
    Class<?> generatedClass = loader.addClass(classDef).loadClass(classDef.getClassName());
    return (FieldAccessor) generatedClass.getConstructor(Type.class).newInstance(ctorArgType);
  }
}
