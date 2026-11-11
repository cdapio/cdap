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

package io.cdap.cdap.common.service;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Services}.
 */
public class ServicesTest {

  @Test
  public void testStartNonNewServiceThrowsExecutionException() throws Exception {
    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() {
      }

      @Override
      protected void shutDown() {
      }
    };
    Services.startAndWait(service, 5, TimeUnit.SECONDS);
    Assert.assertEquals(Service.State.RUNNING, service.state());

    try {
      Services.startAndWait(service, 5, TimeUnit.SECONDS);
      Assert.fail("Expected ExecutionException when starting a service that is not NEW");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalStateException);
    }
  }

  @Test
  public void testStartInterruptedThrowsInterruptedException() throws Exception {
    CountDownLatch startingLatch = new CountDownLatch(1);
    CountDownLatch blockLatch = new CountDownLatch(1);
    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        startingLatch.countDown();
        blockLatch.await();
      }

      @Override
      protected void shutDown() {
      }
    };

    Thread caller = Thread.currentThread();
    Thread interrupter = new Thread(() -> {
      try {
        if (startingLatch.await(5, TimeUnit.SECONDS)) {
          caller.interrupt();
        }
      } catch (InterruptedException ignored) {
      }
    });
    interrupter.start();

    try {
      Services.startAndWait(service, 10, TimeUnit.SECONDS);
      Assert.fail("Expected InterruptedException");
    } catch (InterruptedException expected) {
      // Clear interrupt flag if set and release startup latch
      Thread.interrupted();
    } finally {
      blockLatch.countDown();
      interrupter.join(5000);
    }
  }
}
