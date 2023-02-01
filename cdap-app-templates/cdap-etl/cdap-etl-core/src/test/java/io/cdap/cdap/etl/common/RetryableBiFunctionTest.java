/*
 * Copyright © 2023 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.retry.RetryableException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RetryableBiFunction}.
 */
public class RetryableBiFunctionTest {
  @Test
  public void testRetryableExceptionRetries() {
    RetryableBiFunction<String, String, Long> func = new RetryableBiFunction<String, String, Long>() {
      private long counter;

      @Override
      protected Long apply(String s, String s2) throws Throwable {
        if (counter <= 5) {
          counter++;
          throw new RetryableException("retry");
        }
        return counter;
      }
    };

    // Ensure that the returned value is the counter and was incremented.
    Assert.assertEquals(func.applyWithRetries("testRetryableExceptionRetries", "a", "b"), new Long(6));
  }

  @Test(expected = Exception.class)
  public void testNonRetryableExceptionFails() {
    RetryableBiFunction<String, String, Boolean> func = new RetryableBiFunction<String, String, Boolean>() {
      @Override
      protected Boolean apply(String s, String s2) throws Throwable {
        throw new Exception("some failure");
      }
    };

    func.applyWithRetries("testNonRetryableExceptionFails", "a", "b");
  }
}
