/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api.common;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link Bytes} class.
 */
public class BytesTest {

  @Test
  public void testHexString() {
    byte[] bytes = new byte[] {1, 2, 0, 127, -128, 63, -1};

    String hexString = Bytes.toHexString(bytes);
    Assert.assertEquals("0102007f803fff", hexString);

    Assert.assertArrayEquals(bytes, Bytes.fromHexString(hexString));
    Assert.assertArrayEquals(bytes, Bytes.fromHexString(hexString.toUpperCase()));
  }

  @Test
  public void testUnsafeComparer() {
    testComparer(Bytes.LexicographicalComparerHolder.UnsafeComparer.INSTANCE);
  }

  @Test
  public void testPureJavaComparer() {
    testComparer(Bytes.LexicographicalComparerHolder.PureJavaComparer.INSTANCE);
  }

  private void testComparer(Bytes.Comparer<byte[]> comparer) {
    byte[] left = "aaabbbcccdddeeefffggghhh".getBytes(StandardCharsets.US_ASCII);
    byte[] right = "aaabbbcccdddeeefffggghhh".getBytes(StandardCharsets.US_ASCII);

    // Simple comparison
    int cmp = comparer.compareTo(left, 0, left.length, right, 0, right.length);
    Assert.assertEquals(0, cmp);

    // Compare with offset
    cmp = comparer.compareTo(left, 2, left.length - 2, right, 0, right.length);
    Assert.assertTrue(cmp > 0);

    // Different in length
    cmp = comparer.compareTo(left, 0, left.length - 1, right, 0, right.length);
    Assert.assertTrue(cmp < 0);

    // Empty array
    cmp = comparer.compareTo(Bytes.EMPTY_BYTE_ARRAY, 0, 0, Bytes.EMPTY_BYTE_ARRAY, 0, 0);
    Assert.assertEquals(0, cmp);

    // Empty array is always smallest
    cmp = comparer.compareTo(left, 0, left.length, Bytes.EMPTY_BYTE_ARRAY, 0, 0);
    Assert.assertTrue(cmp > 0);
  }
}
