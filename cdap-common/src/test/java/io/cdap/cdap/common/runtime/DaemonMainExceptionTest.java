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

package io.cdap.cdap.common.runtime;

import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.common.utils.Tasks;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test class for {@link DaemonMain} lifecycle methods throwing exception.
 */
@RunWith(Parameterized.class)
public class DaemonMainExceptionTest {

  @Parameterized.Parameters(name = "{index}: DaemonMainExceptionTest(throwMethod = {0})")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
      {"init"},
      {"start"}
    });
  }

  private final String throwMethod;

  public DaemonMainExceptionTest(String throwMethod) {
    this.throwMethod = throwMethod;
  }

  @Test
  public void testException() throws Exception {
    try {
      new TestDaemon(throwMethod).doMain(new String[0]);
    } catch (Exception e) {
      // Expected
    }
    // Wait for all non-daemon threads to terminate, except the main thread.
    Tasks.waitFor(true, () -> {
      Set<Thread> threads = Thread.getAllStackTraces().keySet();
      return threads.stream().filter(t -> t != Thread.currentThread()).allMatch(Thread::isDaemon);
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  /**
   * A test class for testing exception in different lifecycle methods.
   */
  private static final class TestDaemon extends DaemonMain {
    private static final Logger LOG = LoggerFactory.getLogger(TestDaemon.class);

    private final CountDownLatch initLatch = new CountDownLatch(1);
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final String throwMethod;

    private TestDaemon(String throwMethod) {
      this.throwMethod = throwMethod;
    }

    @Override
    public void init(String[] args) throws Exception {
      LOG.info("init");
      startThread("init", initLatch);
      if ("init".equals(throwMethod)) {
        throw new Exception("ex from init");
      }
    }

    @Override
    public void start() throws Exception {
      LOG.info("start");
      startThread("start", startLatch);
      if ("start".equals(throwMethod)) {
        throw new Exception("ex from start");
      }
    }

    @Override
    public void stop() {
      LOG.info("stop");
      startLatch.countDown();
    }

    @Override
    public void destroy() {
      LOG.info("destroy");
      initLatch.countDown();
    }

    private void startThread(String name, CountDownLatch latch) {
      Thread t = new Thread(() -> {
        Uninterruptibles.awaitUninterruptibly(latch);
      });
      t.setName(name);
      t.start();
    }
  }
}
