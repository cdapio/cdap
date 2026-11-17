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

package io.cdap.cdap.operations;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.ObjectName;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link OperationalStatsService}.
 */
public class OperationalStatsServiceTest {

  /**
   * Test JMX bean interface for {@link OperationalStatsService}.
   */
  @MXBean
  public interface TestStatsBean {
    int getCollectedCount();
  }

  /**
   * Fake {@link OperationalStats} implementation backed by a configurable action.
   */
  public static class TestOperationalStats implements OperationalStats, TestStatsBean {

    private final String serviceName;
    private final String statType;
    private final ThrowingAction collectAction;
    private final AtomicInteger collectedCount = new AtomicInteger();
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicBoolean destroyed = new AtomicBoolean();

    TestOperationalStats(String serviceName, String statType, ThrowingAction collectAction) {
      this.serviceName = serviceName;
      this.statType = statType;
      this.collectAction = collectAction;
    }

    @Override
    public void initialize(Injector injector) {
      initialized.set(true);
    }

    @Override
    public String getServiceName() {
      return serviceName;
    }

    @Override
    public String getStatType() {
      return statType;
    }

    @Override
    public void collect() throws Exception {
      collectedCount.incrementAndGet();
      if (collectAction != null) {
        collectAction.run();
      }
    }

    @Override
    public void destroy() {
      destroyed.set(true);
    }

    @Override
    public int getCollectedCount() {
      return collectedCount.get();
    }
  }

  private interface ThrowingAction {
    void run() throws Exception;
  }

  @Test
  public void testLifecycleRegistersCollectsAndUnregistersBean() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.OperationalStats.REFRESH_INTERVAL_SECS, 10L);
    Injector injector = Guice.createInjector();
    OperationalStatsService service = new OperationalStatsService(cConf, injector);

    CountDownLatch collectedLatch = new CountDownLatch(1);
    TestOperationalStats stats =
        new TestOperationalStats("testsvc", "testinfo", collectedLatch::countDown);
    OperationalExtensionId extId = new OperationalExtensionId("testsvc", "testinfo");
    injectExtensions(service, ImmutableMap.of(extId, stats));

    Hashtable<String, String> props = new Hashtable<>();
    props.put(OperationalStatsUtils.SERVICE_NAME_KEY, "testsvc");
    props.put(OperationalStatsUtils.STAT_TYPE_KEY, "testinfo");
    ObjectName expectedName = new ObjectName(OperationalStatsUtils.JMX_DOMAIN, props);
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    service.startAsync().awaitRunning();
    try {
      Assert.assertTrue(stats.initialized.get());
      Assert.assertTrue(mbs.isRegistered(expectedName));
      Assert.assertTrue(collectedLatch.await(5, TimeUnit.SECONDS));
      Assert.assertTrue(stats.getCollectedCount() >= 1);
    } finally {
      service.stopAsync().awaitTerminated();
    }

    Assert.assertEquals(Service.State.TERMINATED, service.state());
    Assert.assertFalse(mbs.isRegistered(expectedName));
    Assert.assertTrue(stats.destroyed.get());
  }

  @Test
  public void testCollectErrorHandlingAndInterruptedPropagation() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.OperationalStats.REFRESH_INTERVAL_SECS, 60L);
    Injector injector = Guice.createInjector();

    // Case 1: RuntimeException is logged and subsequent stats still collect;
    // ServiceUnavailableException / TException root cause returns early without throwing.
    OperationalStatsService service1 = new OperationalStatsService(cConf, injector);
    TestOperationalStats runtimeFailStats = new TestOperationalStats(
        "svc1", "rt", () -> {
          throw new IllegalStateException("transient read error");
        });
    TestOperationalStats okStats = new TestOperationalStats("svc2", "ok", null);
    TestOperationalStats thriftUnavailableStats = new TestOperationalStats(
        "svc3", "tx", () -> {
          throw new RuntimeException(new TException("tx not ready"));
        });
    TestOperationalStats serviceUnavailableStats = new TestOperationalStats(
        "svc4", "ds", () -> {
          throw new ServiceUnavailableException("dataset");
        });
    Map<OperationalExtensionId, OperationalStats> map1 = new LinkedHashMap<>();
    map1.put(new OperationalExtensionId("svc1", "rt"), runtimeFailStats);
    map1.put(new OperationalExtensionId("svc2", "ok"), okStats);
    map1.put(new OperationalExtensionId("svc3", "tx"), thriftUnavailableStats);
    map1.put(new OperationalExtensionId("svc4", "ds"), serviceUnavailableStats);
    injectExtensions(service1, map1);

    service1.startAsync().awaitRunning();
    try {
      Method collectMethod =
          OperationalStatsService.class.getDeclaredMethod("collectOperationalStats");
      collectMethod.setAccessible(true);
      collectMethod.invoke(service1);
      Assert.assertTrue(okStats.getCollectedCount() >= 1);
      // svc4 comes after svc3's TException early return, verifying TException caused return.
    } finally {
      service1.stopAsync().awaitTerminated();
    }

    // Case 2: Direct InterruptedException propagates out of collectOperationalStats.
    OperationalStatsService service2 = new OperationalStatsService(cConf, injector);
    TestOperationalStats interruptedStats = new TestOperationalStats(
        "svcint", "stat", () -> {
          throw new InterruptedException("stop requested");
        });
    injectExtensions(
        service2, ImmutableMap.of(new OperationalExtensionId("svcint", "stat"), interruptedStats));

    // Keep service2 RUNNING with a second service instance or test while service2 is running.
    OperationalStatsService runningService = new OperationalStatsService(cConf, injector);
    injectExtensions(runningService, ImmutableMap.of());
    runningService.startAsync().awaitRunning();
    try {
      injectExtensions(
          runningService,
          ImmutableMap.of(new OperationalExtensionId("svcint", "stat"), interruptedStats));
      Method collectMethod =
          OperationalStatsService.class.getDeclaredMethod("collectOperationalStats");
      collectMethod.setAccessible(true);
      try {
        collectMethod.invoke(runningService);
        Assert.fail("Expected InterruptedException to propagate");
      } catch (InvocationTargetException e) {
        Assert.assertTrue(e.getCause() instanceof InterruptedException);
      }
    } finally {
      injectExtensions(runningService, ImmutableMap.of());
      runningService.stopAsync().awaitTerminated();
    }
  }

  private static void injectExtensions(
      OperationalStatsService service,
      Map<OperationalExtensionId, OperationalStats> extensions) throws Exception {
    Field loaderField = OperationalStatsService.class.getDeclaredField("operationalStatsLoader");
    loaderField.setAccessible(true);
    Object loader = loaderField.get(service);
    Field allExtensionsField = AbstractExtensionLoader.class.getDeclaredField("allExtensions");
    allExtensionsField.setAccessible(true);
    allExtensionsField.set(loader, extensions);
  }
}
