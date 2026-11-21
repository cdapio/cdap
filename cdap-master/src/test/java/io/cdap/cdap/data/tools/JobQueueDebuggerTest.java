/*
 * Copyright © 2017-2023 Cask Data, Inc.
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

package io.cdap.cdap.data.tools;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.queue.JobQueue;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link JobQueueDebugger}.
 */
public class JobQueueDebuggerTest {

  @Test
  public void testInjector() throws Exception {
    JobQueueDebugger.createDebugger();
    // should not throw exception
  }

  @Test
  public void testStartStopAndScanJobQueue() throws Exception {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAsync().awaitRunning();
    try {
      ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      TransactionRunner txRunner = Mockito.mock(TransactionRunner.class);
      CConfiguration cConf = CConfiguration.create();

      JobQueueDebugger debugger = new JobQueueDebugger(cConf, zkClient, txRunner);
      debugger.startAsync().awaitRunning();
      Assert.assertEquals(Service.State.RUNNING, debugger.state());
      Assert.assertEquals(Service.State.RUNNING, zkClient.state());

      debugger.stopAsync().awaitTerminated();
      Assert.assertEquals(Service.State.TERMINATED, debugger.state());
      Assert.assertEquals(Service.State.TERMINATED, zkClient.state());

      // Exercise JobQueueScanner.scanJobQueue Stopwatch loop
      Class<?> scannerClass = null;
      Class<?> statsClass = null;
      for (Class<?> inner : JobQueueDebugger.class.getDeclaredClasses()) {
        if ("JobQueueScanner".equals(inner.getSimpleName())) {
          scannerClass = inner;
        } else if ("JobStatistics".equals(inner.getSimpleName())) {
          statsClass = inner;
        }
      }
      Assert.assertNotNull(scannerClass);
      Assert.assertNotNull(statsClass);

      Constructor<?> scannerCtor =
          scannerClass.getDeclaredConstructor(CConfiguration.class, TransactionRunner.class);
      scannerCtor.setAccessible(true);
      Object scanner = scannerCtor.newInstance(cConf, txRunner);

      Constructor<?> statsCtor = statsClass.getDeclaredConstructor(boolean.class);
      statsCtor.setAccessible(true);
      Object stats = statsCtor.newInstance(false);

      Job j1 = mockJob(Job.State.PENDING_TRIGGER, 100L);
      Job j2 = mockJob(Job.State.PENDING_CONSTRAINT, 200L);
      Job j3 = mockJob(Job.State.PENDING_LAUNCH, 300L);

      AtomicBoolean iteratorClosed = new AtomicBoolean(false);
      Iterator<Job> delegate = Arrays.asList(j1, j2, j3).iterator();
      CloseableIterator<Job> jobIter = new AbstractCloseableIterator<Job>() {
        @Override
        protected Job computeNext() {
          return delegate.hasNext() ? delegate.next() : endOfData();
        }

        @Override
        public void close() {
          iteratorClosed.set(true);
        }
      };

      JobQueue jobQueue = Mockito.mock(JobQueue.class);
      Mockito.when(jobQueue.getJobs(0, null)).thenReturn(jobIter);

      Method scanJobQueue =
          scannerClass.getDeclaredMethod("scanJobQueue", JobQueue.class, int.class, statsClass);
      scanJobQueue.setAccessible(true);
      boolean hasMore = (Boolean) scanJobQueue.invoke(scanner, jobQueue, 0, stats);
      Assert.assertFalse(hasMore);
      Assert.assertTrue(iteratorClosed.get());

      Method getTotal = statsClass.getDeclaredMethod("getTotal");
      getTotal.setAccessible(true);
      Assert.assertEquals(3, ((Integer) getTotal.invoke(stats)).intValue());
    } finally {
      zkServer.stopAsync().awaitTerminated();
    }
  }

  private static Job mockJob(Job.State state, long creationTime) {
    Job job = Mockito.mock(Job.class);
    Mockito.when(job.getState()).thenReturn(state);
    Mockito.when(job.getCreationTime()).thenReturn(creationTime);
    return job;
  }
}
