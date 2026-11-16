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

package io.cdap.cdap.common.lang;

import io.cdap.cdap.api.annotation.Property;
import io.cdap.cdap.internal.lang.Reflections;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link PropertyFieldSetter}.
 */
public class PropertyFieldSetterTest {

  private enum Mode {
    FAST,
    SAFE
  }

  private static final class ConfigurableTarget {
    @Property
    private String name;
    @Property
    private int count;
    @Property
    private Integer boxedCount;
    @Property
    private boolean enabled;
    @Property
    private Mode mode;
    private String nonProperty = "untouched";
  }

  private static final class UnsupportedCharTarget {
    @Property
    private char ch;
  }

  @Test
  public void testSetsSupportedPropertyFields() {
    String prefix = ConfigurableTarget.class.getName() + ".";
    Map<String, String> props = new HashMap<>();
    props.put(prefix + "name", "pipeline");
    props.put(prefix + "count", "42");
    props.put(prefix + "boxedCount", "99");
    props.put(prefix + "enabled", "true");
    props.put(prefix + "mode", "SAFE");
    props.put(prefix + "nonProperty", "ignored");

    ConfigurableTarget target = new ConfigurableTarget();
    Reflections.visit(target, ConfigurableTarget.class, new PropertyFieldSetter(props));

    Assert.assertEquals("pipeline", target.name);
    Assert.assertEquals(42, target.count);
    Assert.assertEquals(Integer.valueOf(99), target.boxedCount);
    Assert.assertTrue(target.enabled);
    Assert.assertEquals(Mode.SAFE, target.mode);
    Assert.assertEquals("untouched", target.nonProperty);
  }

  @Test
  public void testNoSuchMethodAndInvocationTargetPropagation() {
    // Character lacks valueOf(String), triggering NoSuchMethodException -> RuntimeException.
    UnsupportedCharTarget charTarget = new UnsupportedCharTarget();
    Map<String, String> charProps = Collections.singletonMap(
        UnsupportedCharTarget.class.getName() + ".ch", "x");
    try {
      Reflections.visit(charTarget, UnsupportedCharTarget.class,
          new PropertyFieldSetter(charProps));
      Assert.fail("Expected RuntimeException caused by NoSuchMethodException");
    } catch (RuntimeException caught) {
      // Reflections.visit also wraps checked exceptions, so inspect causal chain for NoSuchMethodException.
      boolean foundNoSuchMethod = false;
      for (Throwable cur = caught; cur != null; cur = cur.getCause()) {
        if (cur instanceof NoSuchMethodException) {
          foundNoSuchMethod = true;
          break;
        }
      }
      Assert.assertTrue(foundNoSuchMethod);
    }

    // Invalid integer triggers NumberFormatException inside valueOf(String) -> InvocationTargetException.
    ConfigurableTarget badIntTarget = new ConfigurableTarget();
    Map<String, String> badIntProps = Collections.singletonMap(
        ConfigurableTarget.class.getName() + ".count", "not_an_int");
    try {
      Reflections.visit(badIntTarget, ConfigurableTarget.class,
          new PropertyFieldSetter(badIntProps));
      Assert.fail("Expected RuntimeException caused by InvocationTargetException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof InvocationTargetException);
      Assert.assertTrue(caught.getCause().getCause() instanceof NumberFormatException);
    }
  }
}
