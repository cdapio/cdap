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
 */

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.retry.RetryableException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractServiceRetryableMacroEvaluator}.
 */
public class AbstractServiceRetryableMacroEvaluatorTest {

  @Test
  public void testEvaluateRetriesWithStopwatchAndSucceeds() {
    AtomicInteger attempts = new AtomicInteger(0);
    AbstractServiceRetryableMacroEvaluator evaluator =
        new AbstractServiceRetryableMacroEvaluator("testService", "testFn") {
          @Override
          Map<String, String> evaluateMacroMap(String macroFunction, String... args) {
            return Collections.emptyMap();
          }

          @Override
          String evaluateMacro(String macroFunction, String... args) {
            if (attempts.incrementAndGet() == 1) {
              throw new RetryableException("transient failure");
            }
            return "resolved-" + args[0];
          }
        };

    String result = evaluator.evaluate("testFn", "arg1");
    Assert.assertEquals("resolved-arg1", result);
    Assert.assertEquals(2, attempts.get());
  }

  @Test
  public void testEvaluateMapRetriesWithStopwatchAndSucceeds() {
    AtomicInteger attempts = new AtomicInteger(0);
    AbstractServiceRetryableMacroEvaluator evaluator =
        new AbstractServiceRetryableMacroEvaluator("testService", "testMapFn") {
          @Override
          Map<String, String> evaluateMacroMap(String macroFunction, String... args) {
            if (attempts.incrementAndGet() == 1) {
              throw new RetryableException("transient map failure");
            }
            return Collections.singletonMap("key", args[0]);
          }

          @Override
          String evaluateMacro(String macroFunction, String... args) {
            return "";
          }
        };

    Map<String, String> result = evaluator.evaluateMap("testMapFn", "val1");
    Assert.assertEquals(Collections.singletonMap("key", "val1"), result);
    Assert.assertEquals(2, attempts.get());
  }

  @Test(expected = InvalidMacroException.class)
  public void testEvaluateWrapsIoExceptionInInvalidMacroException() {
    AbstractServiceRetryableMacroEvaluator evaluator =
        new AbstractServiceRetryableMacroEvaluator("testService", "testFn") {
          @Override
          Map<String, String> evaluateMacroMap(String macroFunction, String... args) {
            return Collections.emptyMap();
          }

          @Override
          String evaluateMacro(String macroFunction, String... args) throws IOException {
            throw new IOException("io failure");
          }
        };

    evaluator.evaluate("testFn", "arg1");
  }
}
