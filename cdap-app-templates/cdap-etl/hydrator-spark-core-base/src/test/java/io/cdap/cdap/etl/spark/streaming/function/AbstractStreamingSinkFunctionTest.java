/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.streaming.function;

import io.cdap.cdap.etl.spark.streaming.StreamingRetrySettings;
import org.apache.spark.streaming.Time;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link AbstractStreamingSinkFunction}
 */
public class AbstractStreamingSinkFunctionTest {

  @Test
  public void testRetry() throws Exception {
    AtomicInteger attempt = new AtomicInteger(1);
    StreamingRetrySettings streamingRetrySettings = new StreamingRetrySettings(2L, 1L, 10L);
    AbstractStreamingSinkFunction<Object> testFunction =
      new AbstractStreamingSinkFunction<Object>(streamingRetrySettings, () -> null) {
        @Override
        protected Set<String> getSinkNames() {
          return Collections.singleton("test-recovering-sink");
        }

        @Override
        protected void retryableCall(Object v1, Time v2) throws Exception {
          if (attempt.getAndIncrement() < 1) {
            throw new RuntimeException("test exception");
          }
        }
      };
    // Should run without throwing exception
    testFunction.call(null, Time.apply(System.currentTimeMillis()));
  }

  @Test(expected = RuntimeException.class)
  public void testRetryTimeout() throws Exception {
    StreamingRetrySettings streamingRetrySettings = new StreamingRetrySettings(2L, 1L, 10L);
    long startTime = System.currentTimeMillis();
    AbstractStreamingSinkFunction<Object> testFunction =
      new AbstractStreamingSinkFunction<Object>(streamingRetrySettings, () -> null) {
        @Override
        protected Set<String> getSinkNames() {
          return Collections.singleton("test-sink");
        }

        @Override
        protected void retryableCall(Object v1, Time v2) throws Exception {
          if (System.currentTimeMillis() - startTime > TimeUnit.MINUTES.toMillis(3)) {
            return;
          }
          throw new RuntimeException("test exception");
        }
      };
    testFunction.call(null, Time.apply(System.currentTimeMillis()));
  }
}
