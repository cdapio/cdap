/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.AbstractProgramRuntimeService;
import io.cdap.cdap.app.runtime.NoOpProgramStateWriter;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringProvisioner;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.profile.Profile;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;


/**
 * Unit test for {@link ProgramRunStatusMonitorService}
 */
public class ProgramRunStatusMonitorServiceTest extends AppFabricTestBase {
  private static final String SYSTEM_METRIC_PREFIX = "system.";
  private static Store store;
  private static CConfiguration cConf;
  private static MetricsCollectionService metricsCollectionService;

  @BeforeClass
  public static void setup() {
    store = getInjector().getInstance(DefaultStore.class);
    cConf = getInjector().getInstance(CConfiguration.class);
    metricsCollectionService = getInjector().getInstance(MetricsCollectionService.class);
  }

  @After
  public void clearStore() {
    store.removeAll(NamespaceId.DEFAULT);
  }

  @Test
  public void testStoppingProgramsBeyondTerminateTimeAreKilled() throws Exception {
    AtomicInteger sourceId = new AtomicInteger(0);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    // set up a workflow for a program in Stopping state
    Map<String, String> wfSystemArg = ImmutableMap.of(
      ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name(),
      SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    ProgramRunId wfId = NamespaceId.DEFAULT.app("test").workflow("testWF").run(randomRunId());
    store.setProvisioning(wfId, Collections.emptyMap(), wfSystemArg,
                          Bytes.toBytes(sourceId.getAndIncrement()), artifactId);
    store.setProvisioned(wfId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setStart(wfId, null, Collections.emptyMap(), Bytes.toBytes(sourceId.getAndIncrement()));
    store.setRunning(wfId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), null,
                     Bytes.toBytes(sourceId.getAndIncrement()));
    long currentTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    store.setStopping(wfId, Bytes.toBytes(sourceId.getAndIncrement()), currentTimeInSecs, currentTimeInSecs);
    CountDownLatch latch = new CountDownLatch(1);
    ProgramRuntimeService testService = new AbstractProgramRuntimeService(
      cConf, null, new NoOpProgramStateWriter(), null) {

      @Override
      protected boolean isDistributed() {
        return false;
      }

      @Nullable
      public RuntimeInfo lookup(ProgramId programId, RunId runId) {
        return getRuntimeInfo(programId, latch);
      }
    };
    ProgramRunStatusMonitorService programRunStatusMonitorService
      = new ProgramRunStatusMonitorService(cConf, store, testService,
                                           metricsCollectionService, new NoOpProgramStateWriter(),
                                           5, 3, 2, 2);
    programRunStatusMonitorService.startAndWait();
    Assert.assertEquals(1, latch.getCount());
    programRunStatusMonitorService.terminatePrograms();
    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    MetricStore metricStore = getInjector().getInstance(MetricStore.class);
    ProfileId myProfile = NamespaceId.SYSTEM.profile("native");
    Tasks.waitFor(true, () -> getMetric(metricStore, wfId, myProfile, new HashMap<>(),
                               SYSTEM_METRIC_PREFIX + Constants.Metrics.Program.PROGRAM_FORCE_TERMINATED_RUNS) > 0,
                  10, TimeUnit.SECONDS);
    metricStore.deleteAll();
  }

  private long getMetric(MetricStore metricStore, ProgramRunId programRunId, ProfileId profileId,
                         Map<String, String> additionalTags, String metricName) {
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name())
      .put(Constants.Metrics.Tag.PROFILE, profileId.getProfile())
      .put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .put(Constants.Metrics.Tag.APP, programRunId.getApplication())
      .putAll(additionalTags)
      .build();
    MetricDataQuery query = new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
                                                tags, new ArrayList<>());
    Collection<MetricTimeSeries> result = metricStore.query(query);
    if (result.isEmpty()) {
      return 0;
    }
    List<TimeValue> timeValues = result.iterator().next().getTimeValues();
    if (timeValues.isEmpty()) {
      return 0;
    }
    return timeValues.get(0).getValue();
  }

  @Test
  public void testStoppingLocalTetheredProgramsBeyondTerminateTimeAreKilled() throws Exception {
    AtomicInteger sourceId = new AtomicInteger(0);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    // Set up a workflow for a tethered program in Stopping state. This program is run in tethered mode on
    // behalf of a remote instance.
    Map<String, String> wfSystemArg = ImmutableMap.of(
      ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name(),
      SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName(),
      SystemArguments.PROFILE_PROVISIONER, Profile.NATIVE.getProvisioner().getName(),
      ProgramOptionConstants.PEER_NAME, "peer");
    ProgramRunId wfId = NamespaceId.DEFAULT.app("test").workflow("testWF").run(randomRunId());
    store.setProvisioning(wfId, Collections.emptyMap(), wfSystemArg,
                          Bytes.toBytes(sourceId.getAndIncrement()), artifactId);
    store.setProvisioned(wfId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setStart(wfId, null, Collections.emptyMap(), Bytes.toBytes(sourceId.getAndIncrement()));
    store.setRunning(wfId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), null,
                     Bytes.toBytes(sourceId.getAndIncrement()));
    long currentTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    store.setStopping(wfId, Bytes.toBytes(sourceId.getAndIncrement()), currentTimeInSecs, currentTimeInSecs);
    CountDownLatch latch = new CountDownLatch(1);
    ProgramRuntimeService testService = new AbstractProgramRuntimeService(
      cConf, null, new NoOpProgramStateWriter(), null) {

      @Override
      protected boolean isDistributed() {
        return false;
      }

      @Nullable
      public RuntimeInfo lookup(ProgramId programId, RunId runId) {
        return getRuntimeInfo(programId, latch);
      }
    };
    ProgramRunStatusMonitorService programRunStatusMonitorService
      = new ProgramRunStatusMonitorService(cConf, store, testService,
                                           metricsCollectionService, new NoOpProgramStateWriter(),
                                           5, 3, 2, 2);
    programRunStatusMonitorService.startAndWait();
    Assert.assertEquals(1, latch.getCount());
    programRunStatusMonitorService.terminatePrograms();
    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    MetricStore metricStore = getInjector().getInstance(MetricStore.class);
    ProfileId myProfile = NamespaceId.SYSTEM.profile("native");
    Tasks.waitFor(true, () -> getMetric(metricStore, wfId, myProfile, new HashMap<>(),
                                        SYSTEM_METRIC_PREFIX +
                                          Constants.Metrics.Program.PROGRAM_FORCE_TERMINATED_RUNS) > 0,
                  10, TimeUnit.SECONDS);
    metricStore.deleteAll();
  }

  @Test
  public void testStoppingRemoteTetheredProgramsBeyondTerminateTimeAreKilled() throws Exception {
    AtomicInteger sourceId = new AtomicInteger(0);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    // Set up a workflow for a tethered program in Stopping state. This program is run on a remote instance.
    Map<String, String> wfSystemArg = ImmutableMap.of(
      ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name(),
      SystemArguments.PROFILE_NAME, NamespaceId.SYSTEM.profile("tethering-profile").getScopedName(),
      SystemArguments.PROFILE_PROVISIONER, TetheringProvisioner.TETHERING_NAME);
    ProgramRunId wfId = NamespaceId.DEFAULT.app("test").workflow("testWF").run(randomRunId());
    store.setProvisioning(wfId, Collections.emptyMap(), wfSystemArg,
                          Bytes.toBytes(sourceId.getAndIncrement()), artifactId);
    store.setProvisioned(wfId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setStart(wfId, null, Collections.emptyMap(), Bytes.toBytes(sourceId.getAndIncrement()));
    long currentTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    long runningTimeInSecs = currentTimeInSecs - 100;
    long stoppingTimeInSecs = currentTimeInSecs - 50;
    long terminateTimeInSecs = currentTimeInSecs - 10;
    store.setRunning(wfId, runningTimeInSecs, null,
                     Bytes.toBytes(sourceId.getAndIncrement()));
    store.setStopping(wfId, Bytes.toBytes(sourceId.getAndIncrement()), stoppingTimeInSecs, terminateTimeInSecs);
    CountDownLatch latch = new CountDownLatch(1);
    ProgramRuntimeService testService = getInjector().getInstance(ProgramRuntimeService.class);
    ProgramStateWriter psw = getProgramStateWriter(latch);
    ProgramRunStatusMonitorService programRunStatusMonitorService
      = new ProgramRunStatusMonitorService(cConf, store, testService, metricsCollectionService, psw,
                                           5, 3, 2, 2);
    programRunStatusMonitorService.startAndWait();
    Assert.assertEquals(1, latch.getCount());
    programRunStatusMonitorService.terminatePrograms();
    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    MetricStore metricStore = getInjector().getInstance(MetricStore.class);
    ProfileId myProfile = NamespaceId.SYSTEM.profile("tethering-profile");
    Tasks.waitFor(true, () -> getMetric(metricStore, wfId, myProfile, new HashMap<>(),
                                        SYSTEM_METRIC_PREFIX +
                                          Constants.Metrics.Program.PROGRAM_FORCE_TERMINATED_RUNS) > 0,
                  10, TimeUnit.SECONDS);
    metricStore.deleteAll();
  }

  @Test
  public void testStoppingProgramsWithFutureTerminateTimeAreInProgress() {
    AtomicInteger sourceId = new AtomicInteger(0);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();

    // set up a workflow
    Map<String, String> wfSystemArg = ImmutableMap.of(
      ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name(),
      SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    ProgramRunId wfId = NamespaceId.DEFAULT.app("test").workflow("testWF").run(randomRunId());
    store.setProvisioning(wfId, Collections.emptyMap(), wfSystemArg,
                          Bytes.toBytes(sourceId.getAndIncrement()), artifactId);
    store.setProvisioned(wfId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setStart(wfId, null, Collections.emptyMap(), Bytes.toBytes(sourceId.getAndIncrement()));
    store.setRunning(wfId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), null,
                     Bytes.toBytes(sourceId.getAndIncrement()));
    long currentTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    store.setStopping(wfId, Bytes.toBytes(sourceId.getAndIncrement()), currentTimeInSecs,
                      currentTimeInSecs + currentTimeInSecs);
    CountDownLatch latch = new CountDownLatch(1);
    ProgramRuntimeService testService = new AbstractProgramRuntimeService(
      cConf, null, new NoOpProgramStateWriter(), null) {

      @Override
      protected boolean isDistributed() {
        return false;
      }

      @Nullable
      public RuntimeInfo lookup(ProgramId programId, RunId runId) {
        return getRuntimeInfo(programId, latch);
      }
    };
    ProgramRunStatusMonitorService programRunStatusMonitorService
      = new ProgramRunStatusMonitorService(cConf, store, testService,
                                           metricsCollectionService, new NoOpProgramStateWriter(),
                                           5, 3, 2, 2);
    Assert.assertEquals(1, latch.getCount());
    Assert.assertTrue(programRunStatusMonitorService.terminatePrograms().isEmpty());
  }

  @Test
  public void testRunningProgramsAreInProgress() {
    AtomicInteger sourceId = new AtomicInteger(0);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();

    // set up a workflow
    Map<String, String> wfSystemArg = ImmutableMap.of(
      ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name(),
      SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    ProgramRunId wfId = NamespaceId.DEFAULT.app("test").workflow("testWF").run(randomRunId());
    store.setProvisioning(wfId, Collections.emptyMap(), wfSystemArg,
                          Bytes.toBytes(sourceId.getAndIncrement()), artifactId);
    store.setProvisioned(wfId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setStart(wfId, null, Collections.emptyMap(), Bytes.toBytes(sourceId.getAndIncrement()));
    store.setRunning(wfId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), null,
                     Bytes.toBytes(sourceId.getAndIncrement()));
    CountDownLatch latch = new CountDownLatch(1);
    ProgramRuntimeService testService = new AbstractProgramRuntimeService(
      cConf, null, new NoOpProgramStateWriter(), null) {

      @Override
      protected boolean isDistributed() {
        return false;
      }

      @Nullable
      public RuntimeInfo lookup(ProgramId programId, RunId runId) {
        return getRuntimeInfo(programId, latch);
      }
    };
    ProgramRunStatusMonitorService programRunStatusMonitorService
      = new ProgramRunStatusMonitorService(cConf, store, testService, metricsCollectionService,
                                           new NoOpProgramStateWriter(),
                                           5, 3, 2, 2);
    Assert.assertEquals(1, latch.getCount());
    Assert.assertTrue(programRunStatusMonitorService.terminatePrograms().isEmpty());
  }

  private ProgramRuntimeService.RuntimeInfo getRuntimeInfo(ProgramId programId, CountDownLatch latch) {
    return new ProgramRuntimeService.RuntimeInfo() {
      public ProgramController getController() {
        return getProgramController(latch);
      }

      @Override
      public ProgramType getType() {
        return null;
      }

      @Override
      public ProgramId getProgramId() {
        return programId;
      }

      @Nullable
      @Override
      public RunId getTwillRunId() {
        return null;
      }
    };
  }

  private ProgramController getProgramController(CountDownLatch latch) {
    return new ProgramController() {
      @Override
      public ProgramRunId getProgramRunId() {
        return null;
      }

      @Override
      public RunId getRunId() {
        return null;
      }

      @Override
      public ListenableFuture<ProgramController> suspend() {
        return null;
      }

      @Override
      public ListenableFuture<ProgramController> resume() {
        return null;
      }

      public ListenableFuture<ProgramController> stop() {
        return null;
      }

      @Override
      public ListenableFuture<ProgramController> stop(long timeout, TimeUnit timeoutUnit) {
        return null;
      }

      @Override
      public void kill() {
        latch.countDown();
      }

      @Override
      public State getState() {
        return null;
      }

      @Override
      public Throwable getFailureCause() {
        return null;
      }

      @Override
      public Cancellable addListener(Listener listener, Executor executor) {
        return null;
      }

      @Override
      public ListenableFuture<ProgramController> command(String name, Object value) {
        return null;
      }
    };
  }

  private ProgramStateWriter getProgramStateWriter(CountDownLatch latch) {
    return new ProgramStateWriter() {
      @Override
      public void start(ProgramRunId programRunId, ProgramOptions programOptions, @Nullable String twillRunId,
                        ProgramDescriptor programDescriptor) {
        // no-op
      }

      @Override
      public void running(ProgramRunId programRunId, @Nullable String twillRunId) {
        // no-op
      }

      @Override
      public void stop(ProgramRunId programRunId, int gracefulShutdownSecs) {
        // no-op
      }

      @Override
      public void completed(ProgramRunId programRunId) {
        // no-op
      }

      @Override
      public void killed(ProgramRunId programRunId) {
        latch.countDown();
      }

      @Override
      public void error(ProgramRunId programRunId, Throwable failureCause) {
        // no-op
      }

      @Override
      public void suspend(ProgramRunId programRunId) {
        // no-op
      }

      @Override
      public void resume(ProgramRunId programRunId) {
        // no-op
      }

      @Override
      public void reject(ProgramRunId programRunId, ProgramOptions programOptions, ProgramDescriptor programDescriptor,
                         String userId, Throwable cause) {
        // no-op
      }
    };
  }

  private RunId randomRunId() {
    long startTime = ThreadLocalRandom.current().nextLong(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
    return RunIds.generate(startTime);
  }
}
