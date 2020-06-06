/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link DataprocUtils}.
 */
public class DataprocUtilsTest {

  @Test
  public void testParseSingleLabel() {
    Map<String, String> expected = new HashMap<>();
    expected.put("key", "val");
    Assert.assertEquals(expected, DataprocUtils.parseLabels("key=val"));
  }

  @Test
  public void testParseMultipleLabels() {
    Map<String, String> expected = new HashMap<>();
    expected.put("k1", "v1");
    expected.put("k2", "v2");
    Assert.assertEquals(expected, DataprocUtils.parseLabels("k1=v1,k2=v2"));
  }

  @Test
  public void testParseLabelsIgnoresWhitespace() {
    Map<String, String> expected = new HashMap<>();
    expected.put("k1", "v1");
    expected.put("k2", "v2");
    Assert.assertEquals(expected, DataprocUtils.parseLabels(" k1  =\tv1  ,\nk2 = v2  "));
  }

  @Test
  public void testParseLabelsWithoutVal() {
    Map<String, String> expected = new HashMap<>();
    expected.put("k1", "");
    expected.put("k2", "");
    Assert.assertEquals(expected, DataprocUtils.parseLabels("k1,k2="));
  }

  @Test
  public void testParseLabelsIgnoresInvalidKey() {
    Map<String, String> expected = new HashMap<>();
    Assert.assertEquals(expected, DataprocUtils.parseLabels("A"));
    Assert.assertEquals(expected, DataprocUtils.parseLabels("0"));
    Assert.assertEquals(expected, DataprocUtils.parseLabels("a.b"));
    Assert.assertEquals(expected, DataprocUtils.parseLabels("a^b"));
    StringBuilder longStr = new StringBuilder();
    for (int i = 0; i < 64; i++) {
      longStr.append('a');
    }
    Assert.assertEquals(expected, DataprocUtils.parseLabels(longStr.toString()));
  }

  @Test
  public void testParseLabelsIgnoresInvalidVal() {
    Map<String, String> expected = new HashMap<>();
    Assert.assertEquals(expected, DataprocUtils.parseLabels("a=A"));
    Assert.assertEquals(expected, DataprocUtils.parseLabels("a=ab.c"));
    StringBuilder longStr = new StringBuilder();
    for (int i = 0; i < 64; i++) {
      longStr.append('a');
    }
    Assert.assertEquals(expected, DataprocUtils.parseLabels(String.format("a=%s", longStr.toString())));
  }
}
