/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class FieldCollectorTest {

  @Test
  public void testFieldsCollected() {
    Map<Field, Object> actual = FieldCollector.getFieldsWithDefaults(new TypeToken<TestClass>() {
    });
    Assert.assertEquals(
      ImmutableList.of(
        new AbstractMap.SimpleEntry<>("stringField", null),
        new AbstractMap.SimpleEntry<>("intField", 0),
        new AbstractMap.SimpleEntry<>("arrayField", null)
      ),
      actual.entrySet().stream()
        .map(e -> new AbstractMap.SimpleEntry<>(e.getKey().getName(), e.getValue())).collect(Collectors.toList())
    );
  }

  private class TestClass {
    private String stringField;
    private int intField;
    private long[] arrayField;
  }
}
