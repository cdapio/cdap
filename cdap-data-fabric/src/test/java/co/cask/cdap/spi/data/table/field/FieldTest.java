/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.spi.data.table.field;

import co.cask.cdap.api.common.Bytes;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 *
 */
public class FieldTest {
  @Test
  public void testHashcode() {
    Assert.assertEquals(generateFieldsSet(), generateFieldsSet());
  }

  @Test
  public void testToString() {
    // This test makes sure that toString() does not throw any exception due to bytes encoding
    Assert.assertEquals(generateFieldsSet().toString(), generateFieldsSet().toString());
  }

  private Set<Field<?>> generateFieldsSet() {
    return ImmutableSet.of(
      Fields.bytesField("bytes", Bytes.toBytes("bytesval")),
      Fields.stringField("string", "strval"),
      Fields.doubleField("double", 100.0),
      Fields.intField("int", 30),
      Fields.bytesField("double-bytes", Bytes.toBytes(100.0)),
      Fields.bytesField("long-bytes", Bytes.toBytes(600L))
    );
  }
}
