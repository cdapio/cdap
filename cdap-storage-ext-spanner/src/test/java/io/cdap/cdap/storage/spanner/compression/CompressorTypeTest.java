/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner.compression;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link CompressorType}.
 */
public class CompressorTypeTest {

  @Test
  public void testFromString() {
    Assert.assertNull(CompressorType.fromString(null));
    Assert.assertNull(CompressorType.fromString(""));
  }

  @Test
  public void testFromString_snappy() {
    Assert.assertEquals(CompressorType.SNAPPY, CompressorType.fromString("SNAPPY"));
    Assert.assertEquals(CompressorType.SNAPPY, CompressorType.fromString("snappy"));
    Assert.assertEquals(CompressorType.SNAPPY, CompressorType.fromString("SnApPy"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFromString_InvalidType() {
    CompressorType.fromString("INVALID");
  }
}
