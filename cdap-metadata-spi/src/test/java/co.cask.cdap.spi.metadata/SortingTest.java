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

package co.cask.cdap.spi.metadata;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SortingTest {

  @Test
  public void testToAndFromString() {
    Sorting[] testCases = {
      new Sorting("field", Sorting.Order.ASC),
      new Sorting("AAA", Sorting.Order.ASC),
      new Sorting("mixedCase", Sorting.Order.DESC)
    };
    for (Sorting sorting : testCases) {
      Assert.assertEquals("For '" + sorting + "':", sorting, Sorting.of(sorting.toString()));
    }

    Map<String, Sorting> cases = ImmutableMap.of(
      "a desc", new Sorting("a", Sorting.Order.DESC),
      "A", new Sorting("a", Sorting.Order.ASC),
      "  xyz  DeSc  ", new Sorting("xyz", Sorting.Order.DESC),
      "   abC   ", new Sorting("abc", Sorting.Order.ASC)
    );
    for (Map.Entry<String, Sorting> entry : cases.entrySet()) {
      Assert.assertEquals("For '" + entry.getKey() + "':", entry.getValue(), Sorting.of(entry.getKey()));
    }

    String[] errors = {
      " xyz ascending",
      "f g",
      "a b c",
      "  "
    };
    for (String str : errors) {
      try {
        Sorting sorting = Sorting.of(str);
        Assert.fail("Expected IllegalArgumentException for '" + str + "' but got: " + sorting);
      } catch (IllegalArgumentException e) {
        //expected
      }
    }
  }


}
