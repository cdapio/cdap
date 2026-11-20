/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.common.utils;

import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link StringUtils}.
 */
public class StringUtilsTest {

  @Test
  public void testNullReturnsEmptyArray() {
    Assert.assertSame(StringUtils.EMPTY_STRING_ARRAY, StringUtils.getTrimmedStrings(null));
  }

  @Test
  public void testEmptyStringReturnsEmptyArray() {
    Assert.assertSame(StringUtils.EMPTY_STRING_ARRAY, StringUtils.getTrimmedStrings(""));
  }

  @Test
  public void testWhitespaceOnlyReturnsEmptyArray() {
    Assert.assertSame(StringUtils.EMPTY_STRING_ARRAY, StringUtils.getTrimmedStrings("  \t "));
  }

  @Test
  public void testSingleValueIsTrimmed() {
    Assert.assertArrayEquals(new String[] {"a"}, StringUtils.getTrimmedStrings("  a  "));
  }

  @Test
  public void testWhitespaceAroundCommasIsTrimmed() {
    Assert.assertArrayEquals(new String[] {"a", "b", "c", "d"},
        StringUtils.getTrimmedStrings(" a ,b,\tc , d "));
  }

  @Test
  public void testWhitespaceInsideValueIsPreserved() {
    Assert.assertArrayEquals(new String[] {"a b", "c"}, StringUtils.getTrimmedStrings("a b, c"));
  }

  @Test
  public void testInteriorEmptySegmentIsKept() {
    // String.split keeps empty segments between delimiters.
    Assert.assertArrayEquals(new String[] {"a", "", "b"}, StringUtils.getTrimmedStrings("a,,b"));
  }

  @Test
  public void testTrailingEmptySegmentIsDropped() {
    // String.split drops trailing empty segments; pinned so a future change is deliberate.
    Assert.assertArrayEquals(new String[] {"a", "b"}, StringUtils.getTrimmedStrings("a,b,"));
  }

  @Test
  public void testCollectionMatchesArrayForm() {
    String input = " x , y,z ";
    Assert.assertEquals(Arrays.asList(StringUtils.getTrimmedStrings(input)),
        StringUtils.getTrimmedStringCollection(input));
    Assert.assertEquals(Arrays.asList("x", "y", "z"),
        StringUtils.getTrimmedStringCollection(input));
  }

  @Test
  public void testCollectionForNullIsEmpty() {
    Assert.assertEquals(Collections.emptyList(), StringUtils.getTrimmedStringCollection(null));
  }
}
