/*
 * Copyright © 2018-2024 Cask Data, Inc.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link BatchingConsumer}.
 */
public class BatchingConsumerTest {

  private static final List<Integer> NUMBERS = new ArrayList<>();
  private static final BatchingConsumer<Integer> BATCHING_CONSUMER =
      new BatchingConsumer<>(list -> list.forEach(NUMBERS::add), 10);

  @Before
  public void setup() {
    NUMBERS.clear();
  }

  @Test
  public void testAccept() {
    boolean isOutputEmptyBeforeLastIteration = true;
    for (int i = 0; i < 10; i++) {
      BATCHING_CONSUMER.accept(i);
      isOutputEmptyBeforeLastIteration = (i < 9) && NUMBERS.isEmpty();
    }

    Assert.assertEquals(10, NUMBERS.size());
    Assert.assertFalse(isOutputEmptyBeforeLastIteration);
  }

  @Test
  public void testClose() {
    for (int i = 0; i < 13; i++) {
      BATCHING_CONSUMER.accept(i);
    }
    Assert.assertEquals(10, NUMBERS.size());

    BATCHING_CONSUMER.close();
    Assert.assertEquals(13, NUMBERS.size());
  }

  @Test
  public void testAutoCloseableChildAndExceptionPropagation() {
    List<Integer> collected = new ArrayList<>();
    AtomicBoolean closed = new AtomicBoolean(false);
    CloseableChildConsumer happyChild = new CloseableChildConsumer(collected, closed, null);

    BatchingConsumer<Integer> happyConsumer = new BatchingConsumer<>(happyChild, 4);
    happyConsumer.accept(1);
    happyConsumer.accept(2);
    happyConsumer.close();
    Assert.assertEquals(Arrays.asList(1, 2), collected);
    Assert.assertTrue(closed.get());

    // Checked Exception from child.close() is wrapped in RuntimeException.
    IOException checkedCloseFailure = new IOException("close io error");
    BatchingConsumer<Integer> checkedConsumer = new BatchingConsumer<>(
        new CloseableChildConsumer(new ArrayList<>(), new AtomicBoolean(), checkedCloseFailure), 4);
    try {
      checkedConsumer.close();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException caught) {
      Assert.assertSame(checkedCloseFailure, caught.getCause());
    }

    // Unchecked RuntimeException from child.close() is rethrown unwrapped.
    IllegalStateException uncheckedCloseFailure = new IllegalStateException("close state error");
    BatchingConsumer<Integer> uncheckedConsumer = new BatchingConsumer<>(
        new CloseableChildConsumer(
            new ArrayList<>(), new AtomicBoolean(), uncheckedCloseFailure), 4);
    try {
      uncheckedConsumer.close();
      Assert.fail("Expected IllegalStateException unwrapped");
    } catch (IllegalStateException caught) {
      Assert.assertSame(uncheckedCloseFailure, caught);
    }
  }

  private static final class CloseableChildConsumer
      implements Consumer<List<Integer>>, AutoCloseable {
    private final List<Integer> sink;
    private final AtomicBoolean closed;
    private final Exception closeFailure;

    CloseableChildConsumer(List<Integer> sink, AtomicBoolean closed, Exception closeFailure) {
      this.sink = sink;
      this.closed = closed;
      this.closeFailure = closeFailure;
    }

    @Override
    public void accept(List<Integer> batch) {
      sink.addAll(batch);
    }

    @Override
    public void close() throws Exception {
      closed.set(true);
      if (closeFailure != null) {
        throw closeFailure;
      }
    }
  }
}
