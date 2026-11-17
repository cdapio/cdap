/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch.distributed;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DistributedMapReduceTaskContextProvider}.
 */
public class DistributedMapReduceTaskContextProviderTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testStartAndStopServicesInOrderAndCloseLogAppenderQuietly() throws Exception {
    DistributedMapReduceTaskContextProvider provider = createIsolatedProvider();

    List<String> events = new ArrayList<>();
    Service service1 = createTrackingService("s1", events, false, false);
    Service service2 = createTrackingService("s2", events, false, false);
    Deque<Service> customServices = new LinkedList<>(Arrays.asList(service1, service2));
    setField(provider, "coreServices", customServices);

    AtomicBoolean logClosed = new AtomicBoolean(false);
    LogAppenderInitializer failingCloseInitializer = new LogAppenderInitializer(null) {
      @Override
      public void initialize() {
        // no-op
      }

      @Override
      public void close() {
        logClosed.set(true);
        sneakyThrow(new IOException("log appender close io failure"));
      }
    };
    setField(provider, "logAppenderInitializer", failingCloseInitializer);

    provider.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, provider.state());
    Assert.assertEquals(Service.State.RUNNING, service1.state());
    Assert.assertEquals(Service.State.RUNNING, service2.state());

    provider.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, provider.state());
    Assert.assertTrue(logClosed.get());
    Assert.assertEquals(
        Arrays.asList("start:s1", "start:s2", "stop:s2", "stop:s1"), events);
  }

  @Test
  public void testStartUpFailureRollsBackStartedServicesAndSuppressesStopExceptions()
      throws Exception {
    DistributedMapReduceTaskContextProvider provider = createIsolatedProvider();

    List<String> events = new ArrayList<>();
    Service service1 = createTrackingService("s1", events, false, true);
    Service service2 = createTrackingService("s2", events, true, false);
    Deque<Service> customServices = new LinkedList<>(Arrays.asList(service1, service2));
    setField(provider, "coreServices", customServices);
    setField(provider, "logAppenderInitializer", Mockito.mock(LogAppenderInitializer.class));

    try {
      provider.startAsync().awaitRunning();
      Assert.fail("Expected Service startAndWait to fail when service2 fails to start");
    } catch (RuntimeException expected) {
      Assert.assertEquals(Service.State.FAILED, provider.state());
    }
    Assert.assertEquals(Arrays.asList("start:s1", "start:s2", "stop:s1"), events);
  }

  private DistributedMapReduceTaskContextProvider createIsolatedProvider() throws Exception {
    final Gson gson = new GsonBuilder()
        .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
        .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
        .create();

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tempFolder.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Transaction.TX_ENABLED, false);
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "http://127.0.0.1:12345");

    ProgramId programId = NamespaceId.DEFAULT.app("mrApp").mr("mrProg");
    ProgramOptions options = new SimpleProgramOptions(
        programId,
        new BasicArguments(ImmutableMap.of(
            ProgramOptionConstants.RUN_ID, RunIds.generate().getId(),
            ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name())),
        new BasicArguments());

    Configuration hConf = new Configuration();
    hConf.set("cdap.mapreduce.program.id", gson.toJson(programId));
    hConf.set("cdap.mapreduce.program.options", gson.toJson(options, ProgramOptions.class));

    return new DistributedMapReduceTaskContextProvider(
        cConf, hConf, Mockito.mock(MapReduceClassLoader.class));
  }

  private Service createTrackingService(
      String name, List<String> events, boolean failOnStart, boolean failOnStop) {
    return new AbstractIdleService() {
      @Override
      protected void startUp() {
        events.add("start:" + name);
        if (failOnStart) {
          throw new IllegalStateException("startup failure for " + name);
        }
      }

      @Override
      protected void shutDown() {
        events.add("stop:" + name);
        if (failOnStop) {
          throw new IllegalStateException("shutdown failure for " + name);
        }
      }
    };
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = DistributedMapReduceTaskContextProvider.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
    throw (E) e;
  }
}
