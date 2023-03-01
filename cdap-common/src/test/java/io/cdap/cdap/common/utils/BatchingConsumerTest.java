/*
 * Copyright © 2018-2022 Cask Data, Inc.
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

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BatchingConsumerTest {

  private static List<Integer> numbers = new ArrayList<>();
  private static BatchingConsumer<Integer> batchingConsumer =
      new BatchingConsumer<>(list -> list.forEach(n -> numbers.add(n)), 10);

  @Before
  public void setup() {
    this.numbers.clear();
  }

  @Test
  public void testAccept() {
    boolean isOutputEmptyBeforeLastIteration = true;
    for (int i = 0; i < 10; i++) {
      batchingConsumer.accept(i);
      isOutputEmptyBeforeLastIteration = (i < 9) && (numbers.isEmpty());
    }

    Assert.assertEquals(numbers.size(), 10);
    Assert.assertFalse(isOutputEmptyBeforeLastIteration);
  }

  @Test
  public void testClose() {
    for (int i = 0; i < 13; i++) {
      batchingConsumer.accept(i);
    }
    Assert.assertEquals(numbers.size(), 10);

    batchingConsumer.close();
    Assert.assertEquals(numbers.size(), 13);
  }
}
