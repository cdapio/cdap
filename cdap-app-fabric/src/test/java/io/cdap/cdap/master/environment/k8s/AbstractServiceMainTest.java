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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.spi.data.StorageProvider;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractServiceMain}.
 */
public class AbstractServiceMainTest {

  @Test
  public void testStartForwardAndStopReverseWithExceptionIsolation() throws Exception {
    TestServiceMain main = new TestServiceMain();
    List<String> events = Collections.synchronizedList(new ArrayList<>());

    Service service1 = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        Thread.sleep(40L);
        events.add("start-1");
      }

      @Override
      protected void shutDown() {
        events.add("stop-1");
      }
    };

    Service failingStopService2 = new AbstractIdleService() {
      @Override
      protected void startUp() {
        events.add("start-2");
      }

      @Override
      protected void shutDown() {
        events.add("stop-2");
        throw new IllegalStateException("stop-2 failed");
      }
    };

    Service service3 = new AbstractIdleService() {
      @Override
      protected void startUp() {
        events.add("start-3");
      }

      @Override
      protected void shutDown() {
        events.add("stop-3");
      }
    };

    AutoCloseable failingCloseable = () -> {
      events.add("close-1");
      throw new IOException("close-1 failed");
    };
    AutoCloseable normalCloseable = () -> events.add("close-2");

    Field servicesField = AbstractServiceMain.class.getDeclaredField("services");
    servicesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<Service> services = (List<Service>) servicesField.get(main);
    services.add(service1);
    services.add(failingStopService2);
    services.add(service3);

    Field closeablesField = AbstractServiceMain.class.getDeclaredField("closeableResources");
    closeablesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<AutoCloseable> closeables = (List<AutoCloseable>) closeablesField.get(main);
    closeables.add(failingCloseable);
    closeables.add(normalCloseable);

    main.start();
    Assert.assertEquals(Service.State.RUNNING, service1.state());
    Assert.assertEquals(Service.State.RUNNING, failingStopService2.state());
    Assert.assertEquals(Service.State.RUNNING, service3.state());

    main.stop();
    Assert.assertEquals(Service.State.TERMINATED, service3.state());
    Assert.assertEquals(Service.State.FAILED, failingStopService2.state());
    Assert.assertEquals(Service.State.TERMINATED, service1.state());

    List<String> expected = new ArrayList<>();
    Collections.addAll(
        expected,
        "start-1", "start-2", "start-3",
        "stop-3", "stop-2", "stop-1",
        "close-1", "close-2");
    Assert.assertEquals(expected, events);
  }

  @Test
  public void testDestroyClosesStorageProviderAndMasterEnv() throws Exception {
    TestServiceMain main = new TestServiceMain();
    StorageProvider storageProvider = Mockito.mock(StorageProvider.class);
    Mockito.doThrow(new RuntimeException("storage close error")).when(storageProvider).close();
    MasterEnvironment masterEnv = Mockito.mock(MasterEnvironment.class);

    Field storageField = AbstractServiceMain.class.getDeclaredField("storageProvider");
    storageField.setAccessible(true);
    storageField.set(main, storageProvider);

    Field envField = AbstractServiceMain.class.getDeclaredField("masterEnv");
    envField.setAccessible(true);
    envField.set(main, masterEnv);

    main.destroy();
    Mockito.verify(storageProvider).close();
    Mockito.verify(masterEnv).destroy();
  }

  private static final class TestServiceMain extends AbstractServiceMain<EnvironmentOptions> {

    @Override
    protected List<Module> getServiceModules(
        MasterEnvironment masterEnv, EnvironmentOptions options, CConfiguration cConf) {
      return Collections.emptyList();
    }

    @Override
    protected void addServices(
        Injector injector,
        List<? super Service> services,
        List<? super AutoCloseable> closeableResources,
        MasterEnvironment masterEnv,
        MasterEnvironmentContext masterEnvContext,
        EnvironmentOptions options) {
      // No-op for unit test.
    }

    @Nullable
    @Override
    protected LoggingContext getLoggingContext(EnvironmentOptions options) {
      return null;
    }
  }
}
