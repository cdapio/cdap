/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.common.service;

import co.cask.cdap.api.retry.RetriesExhaustedException;
import co.cask.cdap.common.utils.Tasks;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * Unit test class for the {@link AbstractRetryableScheduledService}.
 */
public class RetryableScheduledServiceTest {

  @Test
  public void testNormalIteration() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(5);

    Service service = new AbstractRetryableScheduledService(RetryStrategies.noRetry()) {
      @Override
      protected long runTask() {
        latch.countDown();
        return 1L;
      }
    };

    service.start();
    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
    service.stopAndWait();
  }

  @Test
  public void testRetryExhausted() throws InterruptedException, ExecutionException, TimeoutException {
    Service service = new AbstractRetryableScheduledService(RetryStrategies.noRetry()) {
      @Override
      protected long runTask() throws Exception {
        throw new Exception("Task failed");
      }
    };

    service.start();
    // Wait for the service to fail
    Tasks.waitFor(Service.State.FAILED, service::state, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    try {
      service.stopAndWait();
    } catch (Exception e) {
      // The root cause should be the one throw from the runTask. It should suppressed the retry exhausted exception.
      Throwable rootCause = Throwables.getRootCause(e);
      Assert.assertEquals("Task failed", rootCause.getMessage());

      Throwable suppressed = Stream.of(rootCause.getSuppressed()).findFirst().orElseThrow(IllegalStateException::new);
      Assert.assertTrue(suppressed instanceof RetriesExhaustedException);
    }
  }

  @Test
  public void testNoRetry() throws InterruptedException, ExecutionException, TimeoutException {
    Service service = new AbstractRetryableScheduledService(RetryStrategies.fixDelay(10L, TimeUnit.MILLISECONDS)) {
      @Override
      protected long runTask() throws Exception {
        throw new Exception("Task failed");
      }

      @Override
      protected boolean shouldRetry(Exception ex) {
        return false;
      }
    };

    service.start();
    // Wait for the service to fail
    Tasks.waitFor(Service.State.FAILED, service::state, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    try {
      service.stopAndWait();
    } catch (Exception e) {
      // The root cause should be the one throw from the runTask.
      Throwable rootCause = Throwables.getRootCause(e);
      Assert.assertEquals("Task failed", rootCause.getMessage());
    }
  }

  @Test
  public void testFailureRetry() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(3);
    Service service = new AbstractRetryableScheduledService(RetryStrategies.fixDelay(1L, TimeUnit.MILLISECONDS)) {

      private int failureCount = 5;

      @Override
      protected long runTask() throws Exception {
        if (--failureCount % 2 == 0) {
          throw new Exception("Task failed");
        }
        latch.countDown();
        return 1L;
      }
    };
    service.start();
    Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
    service.stopAndWait();
  }
}
