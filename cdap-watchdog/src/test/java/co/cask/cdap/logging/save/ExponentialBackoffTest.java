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

package co.cask.cdap.logging.save;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test ExponentialBackoff
 */
public class ExponentialBackoffTest {
  @Test
  public void testExponentialBackoff() throws Exception {
    List<Long> expectedBackoffs = ImmutableList.of(1L, 2L, 4L, 8L, 16L, 32L, 60L, 60L, 60L, 60L);
    final List<Long> actualBackoffs = new ArrayList<>();

    ExponentialBackoff exponentialBackoff =
      new ExponentialBackoff(1, 60,
                             new ExponentialBackoff.BackoffHandler() {
                               @Override
                               public void handle(long backoff) throws InterruptedException {
                                 actualBackoffs.add(backoff);
                               }
                             });

    for (int i = 0; i < 10; i++) {
      exponentialBackoff.backoff();
    }
    Assert.assertEquals(expectedBackoffs, actualBackoffs);

    // Now reset and try again
    exponentialBackoff.reset();
    actualBackoffs.clear();

    for (int i = 0; i < 10; i++) {
      exponentialBackoff.backoff();
    }
    Assert.assertEquals(expectedBackoffs, actualBackoffs);

  }
}
