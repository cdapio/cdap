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

package io.cdap.cdap.internal.app.worker.manager;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.distributed.MockMasterEnvironment;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.master.environment.MasterEnvironments;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link TaskWorkerManagerTwillRunnable}.
 */
public class TaskWorkerManagerTwillRunnableTest {

  @After
  public void tearDown() {
    MasterEnvironments.setMasterEnvironment(null);
  }

  /**
   * Catches missing bindings in the manager's minimal module set before they fail a pod at startup.
   */
  @Test
  public void testInjector() {
    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());

    Injector injector = TaskWorkerManagerTwillRunnable.createInjector(CConfiguration.create(),
        new Configuration());

    Assert.assertNotNull(injector.getInstance(LogAppenderInitializer.class));
    // One service per pod: it owns the lease table and the listening port.
    TaskWorkerManagerService service = injector.getInstance(TaskWorkerManagerService.class);
    Assert.assertNotNull(service);
    Assert.assertSame(service, injector.getInstance(TaskWorkerManagerService.class));
  }

  /**
   * The manager gets the per-pod client, and everything else keeps the ClusterIP client.
   */
  @Test
  public void testInjectorBindsBothDiscoveryClients() {
    MockMasterEnvironment masterEnv = new MockMasterEnvironment();
    MasterEnvironments.setMasterEnvironment(masterEnv);

    Injector injector = TaskWorkerManagerTwillRunnable.createInjector(CConfiguration.create(),
        new Configuration());

    Assert.assertSame(masterEnv.getEndpointsDiscoveryServiceClientSupplier().get(),
        injector.getInstance(Key.get(DiscoveryServiceClient.class,
            Names.named(TaskWorkerManagerService.WORKER_DISCOVERY))));
    Assert.assertSame(masterEnv.getDiscoveryServiceClientSupplier().get(),
        injector.getInstance(DiscoveryServiceClient.class));
  }

  /**
   * Without a master environment the injector fails instead of falling back to ZooKeeper.
   */
  @Test
  public void testInjectorRequiresMasterEnvironment() {
    MasterEnvironments.setMasterEnvironment(null);

    try {
      TaskWorkerManagerTwillRunnable.createInjector(CConfiguration.create(), new Configuration());
      Assert.fail("Expected IllegalStateException without a MasterEnvironment");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("MasterEnvironment"));
    }
  }
}
