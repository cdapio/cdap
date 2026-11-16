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

package io.cdap.cdap.sourcecontrol.worker;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SourceControlTask}.
 */
public class SourceControlTaskTest {

  @Test
  public void testRunStartsOperationRunnerBeforeDoRun() throws Exception {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    AtomicReference<Service.State> stateDuringDoRun = new AtomicReference<>();

    SourceControlTask task = new SourceControlTask(
        CConfiguration.create(),
        discoveryService,
        discoveryService,
        new NoOpMetricsCollectionService()) {
      @Override
      protected void doRun(RunnableTaskContext context) {
        stateDuringDoRun.set(inMemoryOperationRunner.state());
      }
    };

    RunnableTaskContext mockContext = Mockito.mock(RunnableTaskContext.class);
    try {
      task.run(mockContext);
      Assert.assertEquals(Service.State.RUNNING, stateDuringDoRun.get());
    } finally {
      if (task.inMemoryOperationRunner.isRunning()) {
        task.inMemoryOperationRunner.stopAndWait();
      }
    }
  }
}
