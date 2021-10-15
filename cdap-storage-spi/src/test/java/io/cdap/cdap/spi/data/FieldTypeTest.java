/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.spi.data;

import io.cdap.cdap.spi.data.table.field.FieldType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumSet;

/**
 * Unit tests for {@link FieldType}.
 */
public class FieldTypeTest {

  @Test
  public void testSelfCompatible() {
    for (FieldType.Type type : FieldType.Type.values()) {
      Assert.assertTrue(type.isCompatible(type));
    }
  }

  @Test
  public void testCompatible() {
    Assert.assertTrue(FieldType.Type.LONG.isCompatible(FieldType.Type.INTEGER));
    Assert.assertTrue(FieldType.Type.DOUBLE.isCompatible(FieldType.Type.FLOAT));
  }

  @Test
  public void testIncompatible() {
    // These four types shouldn't be compatible with others except itself
    for (FieldType.Type type : EnumSet.of(FieldType.Type.INTEGER, FieldType.Type.FLOAT,
                                          FieldType.Type.STRING, FieldType.Type.BYTES)) {
      for (FieldType.Type otherType : FieldType.Type.values()) {
        if (type != otherType) {
          Assert.assertFalse(type.isCompatible(otherType));
        }
      }
    }

    // Long and Double only compatible with Integer and Float respectively (and itself)
    Assert.assertTrue(
      Arrays.stream(FieldType.Type.values())
        .filter(f -> EnumSet.complementOf(EnumSet.of(FieldType.Type.LONG, FieldType.Type.INTEGER)).contains(f))
        .noneMatch(FieldType.Type.LONG::isCompatible));

    Assert.assertTrue(
      Arrays.stream(FieldType.Type.values())
        .filter(f -> EnumSet.complementOf(EnumSet.of(FieldType.Type.DOUBLE, FieldType.Type.FLOAT)).contains(f))
        .noneMatch(FieldType.Type.DOUBLE::isCompatible));
  }
}
