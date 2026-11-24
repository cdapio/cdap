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

package io.cdap.cdap.etl.common.plugin;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Caller}.
 */
public class CallerTest {

  @Test
  public void testCallUncheckedReturnsResult() {
    Assert.assertEquals("ok", Caller.DEFAULT.callUnchecked(() -> "ok"));
  }

  @Test
  public void testCallUncheckedPropagatesRuntimeExceptionDirectly() {
    IllegalStateException expected = new IllegalStateException("unchecked failure");
    try {
      Caller.DEFAULT.callUnchecked(() -> {
        throw expected;
      });
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException actual) {
      Assert.assertSame(expected, actual);
    }
  }

  @Test
  public void testCallUncheckedWrapsCheckedException() {
    IOException expected = new IOException("checked failure");
    try {
      Caller.DEFAULT.callUnchecked(() -> {
        throw expected;
      });
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expected, actual.getCause());
    }
  }
}
