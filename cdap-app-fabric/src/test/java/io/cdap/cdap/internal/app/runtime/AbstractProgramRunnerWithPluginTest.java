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

package io.cdap.cdap.internal.app.runtime;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.utils.Tasks;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.twill.common.Threads;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractProgramRunnerWithPlugin}.
 */
public class AbstractProgramRunnerWithPluginTest {

  @Test
  public void testCloseAllQuietlyClosesEveryItemWhenFirstThrows() {
    TestRunnerWithPlugin runner = new TestRunnerWithPlugin(CConfiguration.create());
    AtomicInteger firstClosed = new AtomicInteger();
    AtomicInteger secondClosed = new AtomicInteger();
    AtomicInteger thirdClosed = new AtomicInteger();

    List<Closeable> closeables = Arrays.asList(
        () -> {
          firstClosed.incrementAndGet();
          throw new IOException("first close failed");
        },
        secondClosed::incrementAndGet,
        () -> {
          thirdClosed.incrementAndGet();
          throw new IOException("third close failed");
        });

    runner.invokeCloseAllQuietly(closeables);
    Assert.assertEquals(1, firstClosed.get());
    Assert.assertEquals(1, secondClosed.get());
    Assert.assertEquals(1, thirdClosed.get());
  }

  @Test
  public void testRuntimeServiceListenerClosesAllOnTerminated() throws Exception {
    TestRunnerWithPlugin runner = new TestRunnerWithPlugin(CConfiguration.create());
    AtomicInteger firstClosed = new AtomicInteger();
    AtomicInteger secondClosed = new AtomicInteger();

    List<Closeable> closeables = Arrays.asList(
        () -> {
          firstClosed.incrementAndGet();
          throw new IOException("first close failed");
        },
        secondClosed::incrementAndGet);

    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() {
        // no-op
      }

      @Override
      protected void shutDown() {
        // no-op
      }
    };
    service.addListener(runner.createListener(closeables), Threads.SAME_THREAD_EXECUTOR);

    service.startAndWait();
    Assert.assertEquals(0, firstClosed.get());
    Assert.assertEquals(0, secondClosed.get());

    service.stopAndWait();
    Tasks.waitFor(1, secondClosed::get, 5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
    Assert.assertEquals(1, firstClosed.get());
    Assert.assertEquals(1, secondClosed.get());
  }

  @Test
  public void testRuntimeServiceListenerClosesAllOnFailed() throws Exception {
    TestRunnerWithPlugin runner = new TestRunnerWithPlugin(CConfiguration.create());
    AtomicInteger firstClosed = new AtomicInteger();
    AtomicInteger secondClosed = new AtomicInteger();

    List<Closeable> closeables = Arrays.asList(
        () -> {
          firstClosed.incrementAndGet();
          throw new IOException("first close failed");
        },
        secondClosed::incrementAndGet);

    Service failingService = new AbstractIdleService() {
      @Override
      protected void startUp() {
        throw new IllegalStateException("startup failure");
      }

      @Override
      protected void shutDown() {
        // no-op
      }
    };
    failingService.addListener(runner.createListener(closeables), Threads.SAME_THREAD_EXECUTOR);

    Assert.assertThrows(RuntimeException.class, failingService::startAndWait);
    Tasks.waitFor(1, secondClosed::get, 5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
    Assert.assertEquals(1, firstClosed.get());
    Assert.assertEquals(1, secondClosed.get());
  }

  private static final class TestRunnerWithPlugin extends AbstractProgramRunnerWithPlugin {

    private TestRunnerWithPlugin(CConfiguration cConf) {
      super(cConf);
    }

    private Service.Listener createListener(Iterable<Closeable> closeables) {
      return createRuntimeServiceListener(closeables);
    }

    private void invokeCloseAllQuietly(Iterable<Closeable> closeables) {
      closeAllQuietly(closeables);
    }

    @Override
    public ProgramController run(Program program, ProgramOptions options) {
      throw new UnsupportedOperationException();
    }
  }
}
