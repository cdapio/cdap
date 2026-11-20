/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.common.twill;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.internal.RunIds;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link AbstractMasterTwillRunnable}.
 */
public class AbstractMasterTwillRunnableTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private Thread.UncaughtExceptionHandler originalUncaughtHandler;
  private File cConfFile;
  private File hConfFile;

  @Before
  public void setUp() throws IOException {
    originalUncaughtHandler = Thread.getDefaultUncaughtExceptionHandler();
    cConfFile = TEMP_FOLDER.newFile();
    hConfFile = TEMP_FOLDER.newFile();
    String emptyXml = "<configuration></configuration>";
    Files.write(cConfFile.toPath(), emptyXml.getBytes(StandardCharsets.UTF_8));
    Files.write(hConfFile.toPath(), emptyXml.getBytes(StandardCharsets.UTF_8));
  }

  @After
  public void tearDown() {
    Thread.setDefaultUncaughtExceptionHandler(originalUncaughtHandler);
  }

  @Test
  public void testLifecycleStartStopAndDestroyOrdering() throws Exception {
    List<String> events = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch extraStarted = new CountDownLatch(1);
    Service extraService = new AbstractIdleService() {
      @Override
      protected void startUp() {
        events.add("start:extra");
        extraStarted.countDown();
      }

      @Override
      protected void shutDown() {
        events.add("stop:extra");
      }
    };

    TestMasterTwillRunnable runnable = new TestMasterTwillRunnable(
        "master-runnable", cConfFile.getAbsolutePath(), hConfFile.getAbsolutePath(),
        events, Collections.singletonList(extraService), null);

    TwillRunnableSpecification spec = runnable.configure();
    Assert.assertEquals("master-runnable", spec.getName());

    runnable.initialize(createContext(spec));
    Assert.assertNotNull(runnable.exposedConfiguration());
    Assert.assertNotNull(runnable.exposedCdapConf());

    Thread runThread = new Thread(runnable::run);
    runThread.start();
    Assert.assertTrue(extraStarted.await(5, TimeUnit.SECONDS));

    // Interrupt via stop() and verify run() exits cleanly.
    runnable.stop();
    runThread.join(5000);
    Assert.assertFalse(runThread.isAlive());

    runnable.destroy();
    Assert.assertEquals(
        Arrays.asList(
            "start:zk", "start:kafka", "start:broker", "start:metrics", "start:extra",
            "stop:extra", "stop:metrics", "stop:broker", "stop:kafka", "stop:zk"),
        events);
  }

  @Test
  public void testRunExitsWhenAnyServiceTerminates() throws Exception {
    List<String> events = Collections.synchronizedList(new ArrayList<>());
    Service selfTerminatingService = new AbstractExecutionThreadService() {
      @Override
      protected void run() {
        // Exit immediately so terminated() callback completes run().
      }
    };

    TestMasterTwillRunnable runnable = new TestMasterTwillRunnable(
        "term-runnable", cConfFile.getAbsolutePath(), hConfFile.getAbsolutePath(),
        events, Collections.singletonList(selfTerminatingService), null);
    runnable.initialize(createContext(runnable.configure()));

    // Should return promptly without needing stop() because selfTerminatingService terminates.
    runnable.run();
    runnable.destroy();
    Assert.assertEquals(Service.State.TERMINATED, selfTerminatingService.state());
  }

  @Test
  public void testRunPropagatesUncheckedAndCheckedServiceFailures() throws Exception {
    IllegalStateException uncheckedFailure = new IllegalStateException("service runtime boom");
    Service uncheckedFailingService = new AbstractExecutionThreadService() {
      @Override
      protected void run() {
        throw uncheckedFailure;
      }
    };

    TestMasterTwillRunnable uncheckedRunnable = new TestMasterTwillRunnable(
        "fail-unchecked", cConfFile.getAbsolutePath(), hConfFile.getAbsolutePath(),
        Collections.synchronizedList(new ArrayList<>()),
        Collections.singletonList(uncheckedFailingService), null);
    uncheckedRunnable.initialize(createContext(uncheckedRunnable.configure()));

    try {
      uncheckedRunnable.run();
      Assert.fail("Expected IllegalStateException to be propagated unwrapped");
    } catch (IllegalStateException caught) {
      Assert.assertSame(uncheckedFailure, caught);
    }

    IOException checkedFailure = new IOException("checked service failure");
    Service checkedFailingService = new AbstractExecutionThreadService() {
      @Override
      protected void run() throws Exception {
        throw checkedFailure;
      }
    };

    TestMasterTwillRunnable checkedRunnable = new TestMasterTwillRunnable(
        "fail-checked", cConfFile.getAbsolutePath(), hConfFile.getAbsolutePath(),
        Collections.synchronizedList(new ArrayList<>()),
        Collections.singletonList(checkedFailingService), null);
    checkedRunnable.initialize(createContext(checkedRunnable.configure()));

    try {
      checkedRunnable.run();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException caught) {
      Assert.assertSame(checkedFailure, caught.getCause());
    }
  }

  @Test
  public void testInitializePropagatesUncheckedExceptionUnwrapped() {
    IllegalArgumentException initError = new IllegalArgumentException("bad init");
    TestMasterTwillRunnable runnable = new TestMasterTwillRunnable(
        "fail-init", cConfFile.getAbsolutePath(), hConfFile.getAbsolutePath(),
        new ArrayList<>(), Collections.emptyList(), initError);

    try {
      runnable.initialize(createContext(runnable.configure()));
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException caught) {
      Assert.assertSame(initError, caught);
    }
  }

  private static TwillContext createContext(TwillRunnableSpecification spec) {
    RunId runId = RunIds.generate();
    RunId appRunId = RunIds.generate();
    return new TwillContext() {
      @Override
      public RunId getRunId() {
        return runId;
      }

      @Override
      public RunId getApplicationRunId() {
        return appRunId;
      }

      @Override
      public int getInstanceCount() {
        return 1;
      }

      @Override
      public InetAddress getHost() {
        return InetAddress.getLoopbackAddress();
      }

      @Override
      public String[] getArguments() {
        return new String[0];
      }

      @Override
      public String[] getApplicationArguments() {
        return new String[0];
      }

      @Override
      public TwillRunnableSpecification getSpecification() {
        return spec;
      }

      @Override
      public int getInstanceId() {
        return 0;
      }

      @Override
      public int getVirtualCores() {
        return 1;
      }

      @Override
      public int getMaxMemoryMB() {
        return 512;
      }

      @Override
      public ServiceDiscovered discover(String name) {
        return null;
      }

      @Override
      public Cancellable electLeader(String name, ElectionHandler participantHandler) {
        return null;
      }

      @Override
      public Lock createLock(String name) {
        return null;
      }

      @Override
      public Cancellable announce(String serviceName, int port) {
        return null;
      }

      @Override
      public Cancellable announce(String serviceName, int port, byte[] payload) {
        return null;
      }
    };
  }

  private static <T extends Service> T createServiceProxy(Class<T> iface, String label,
      List<String> events) {
    Service backing = new AbstractIdleService() {
      @Override
      protected void startUp() {
        events.add("start:" + label);
      }

      @Override
      protected void shutDown() {
        events.add("stop:" + label);
      }
    };

    AtomicReference<Object> selfRef = new AtomicReference<>();
    Object proxy = Proxy.newProxyInstance(
        iface.getClassLoader(),
        new Class<?>[]{iface},
        (p, method, args) -> {
          if ("equals".equals(method.getName()) && method.getParameterCount() == 1) {
            return p == args[0];
          }
          if ("hashCode".equals(method.getName()) && method.getParameterCount() == 0) {
            return System.identityHashCode(p);
          }
          if ("toString".equals(method.getName()) && method.getParameterCount() == 0) {
            return iface.getSimpleName() + "(" + label + ")";
          }
          return method.invoke(backing, args);
        });
    selfRef.set(proxy);
    return iface.cast(proxy);
  }

  private static final class TestMasterTwillRunnable extends AbstractMasterTwillRunnable {
    private final List<String> events;
    private final List<Service> extraServices;
    private final RuntimeException initFailure;

    TestMasterTwillRunnable(String name, String cConfName, String hConfName,
        List<String> events, List<Service> extraServices, RuntimeException initFailure) {
      super(name, cConfName, hConfName);
      this.events = events;
      this.extraServices = extraServices;
      this.initFailure = initFailure;
    }

    org.apache.hadoop.conf.Configuration exposedConfiguration() {
      return getConfiguration();
    }

    io.cdap.cdap.common.conf.CConfiguration exposedCdapConf() {
      return getCConfiguration();
    }

    @Override
    protected void addServices(List<? super Service> services) {
      services.addAll(extraServices);
    }

    @Override
    protected Injector doInit(TwillContext context) {
      if (initFailure != null) {
        throw initFailure;
      }
      return Guice.createInjector(new AbstractModule() {
        @Override
        protected void configure() {
          bind(ZKClientService.class)
              .toInstance(createServiceProxy(ZKClientService.class, "zk", events));
          bind(KafkaClientService.class)
              .toInstance(createServiceProxy(KafkaClientService.class, "kafka", events));
          bind(BrokerService.class)
              .toInstance(createServiceProxy(BrokerService.class, "broker", events));
          bind(MetricsCollectionService.class)
              .toInstance(createServiceProxy(MetricsCollectionService.class, "metrics", events));
        }
      });
    }
  }
}
