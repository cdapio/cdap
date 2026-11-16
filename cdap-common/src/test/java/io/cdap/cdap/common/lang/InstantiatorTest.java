/*
 * Copyright © 2014-2024 Cask Data, Inc.
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

package io.cdap.cdap.common.lang;

import com.google.common.base.Defaults;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.internal.lang.FieldVisitor;
import io.cdap.cdap.internal.lang.Reflections;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link InstantiatorFactory}.
 */
public class InstantiatorTest {

  @Test
  public void testUnsafe() {
    Record record = new InstantiatorFactory(false).get(TypeToken.of(Record.class)).create();

    Reflections.visit(record, Record.class, new FieldVisitor() {
      @Override
      public void visit(Object instance, Type inspectType, Type declareType, Field field)
          throws Exception {
        if (!Modifier.isStatic(field.getModifiers())) {
          Assert.assertEquals(Defaults.defaultValue(field.getType()), field.get(instance));
        }
      }
    });
  }

  @Test
  public void testDefaultConstructorAndExceptionPropagation() {
    InstantiatorFactory factory = new InstantiatorFactory(false);
    DefaultConstructible created = factory.get(TypeToken.of(DefaultConstructible.class)).create();
    Assert.assertEquals("initialized", created.value);

    Instantiator<ThrowingDefaultCtor> throwingInstantiator =
        factory.get(TypeToken.of(ThrowingDefaultCtor.class));
    try {
      throwingInstantiator.create();
      Assert.fail("Expected RuntimeException wrapping InvocationTargetException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof InvocationTargetException);
      Assert.assertEquals("ctor boom", caught.getCause().getCause().getMessage());
    }
  }

  @Test
  public void testKnownTypesAndUnsafeFailurePropagation() {
    InstantiatorFactory knownFactory = new InstantiatorFactory(true);
    Assert.assertTrue(knownFactory.get(new TypeToken<SortedSet<String>>() { }).create() != null);
    Assert.assertTrue(knownFactory.get(new TypeToken<Set<String>>() { }).create() != null);
    Assert.assertTrue(knownFactory.get(new TypeToken<Queue<String>>() { }).create() != null);
    Assert.assertTrue(knownFactory.get(new TypeToken<List<String>>() { }).create() != null);
    Assert.assertTrue(
        knownFactory.get(new TypeToken<SortedMap<String, Integer>>() { }).create() != null);
    Assert.assertTrue(
        knownFactory.get(new TypeToken<Map<String, Integer>>() { }).create() != null);
    Assert.assertNotNull((Object) knownFactory.get(TypeToken.of(String[].class)).create());

    // Allocating an interface via Unsafe throws InstantiationException -> RuntimeException.
    InstantiatorFactory unsafeOnly = new InstantiatorFactory(false);
    Instantiator<Runnable> runnableInstantiator = unsafeOnly.get(TypeToken.of(Runnable.class));
    try {
      runnableInstantiator.create();
      Assert.fail("Expected RuntimeException wrapping InstantiationException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof InstantiationException);
    }
  }

  private static final class DefaultConstructible {
    private final String value;

    private DefaultConstructible() {
      this.value = "initialized";
    }
  }

  private static final class ThrowingDefaultCtor {
    private ThrowingDefaultCtor() {
      throw new IllegalStateException("ctor boom");
    }
  }

  /**
   * Sample class without default constructor.
   */
  public static final class Record {
    private static final Logger LOG = LoggerFactory.getLogger(Record.class);

    private boolean bool;
    private byte b;
    private char c;
    private short s;
    private int i;
    private long l;
    private float f;
    private double d;
    private String str;

    // Just to avoid having default constructor.
    public Record(String s) {
    }
  }
}
