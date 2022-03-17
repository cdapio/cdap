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
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.AbstractProgramRuntimeService;
import io.cdap.cdap.app.runtime.NoOpProgramStateWriter;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
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
  private static Store store;
  private static CConfiguration cConf;

  @BeforeClass
  public static void setup() {
    store = getInjector().getInstance(DefaultStore.class);
    cConf = getInjector().getInstance(CConfiguration.class);
  }

  @After
  public void clearStore() {
    store.removeAll(NamespaceId.DEFAULT);
  }

  @Test
  public void testStoppingProgramsBeyondTerminateTimeAreKilled() {
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
      cConf, null, null, new NoOpProgramStateWriter(), null, null, null) {
      @Nullable
      public RuntimeInfo lookup(ProgramId programId, RunId runId) {
        return getRuntimeInfo(programId, latch);
      }
    };
    ProgramRunStatusMonitorService programRunStatusMonitorService
      = new ProgramRunStatusMonitorService(cConf, store, testService, 5, 3);
    Assert.assertEquals(1, latch.getCount());
    programRunStatusMonitorService.terminatePrograms();
    Assert.assertEquals(0, latch.getCount());
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
      cConf, null, null, new NoOpProgramStateWriter(), null, null, null) {
      @Nullable
      public RuntimeInfo lookup(ProgramId programId, RunId runId) {
        return getRuntimeInfo(programId, latch);
      }
    };
    ProgramRunStatusMonitorService programRunStatusMonitorService
      = new ProgramRunStatusMonitorService(cConf, store, testService, 5, 3);
    Assert.assertEquals(1, latch.getCount());
    programRunStatusMonitorService.terminatePrograms();
    Assert.assertEquals(1, latch.getCount());
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
      cConf, null, null, new NoOpProgramStateWriter(), null, null, null) {
      @Nullable
      public RuntimeInfo lookup(ProgramId programId, RunId runId) {
        return getRuntimeInfo(programId, latch);
      }
    };
    ProgramRunStatusMonitorService programRunStatusMonitorService
      = new ProgramRunStatusMonitorService(cConf, store, testService, 5, 3);
    Assert.assertEquals(1, latch.getCount());
    programRunStatusMonitorService.terminatePrograms();
    Assert.assertEquals(1, latch.getCount());
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
          latch.countDown();
          return null;
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

  private RunId randomRunId() {
    long startTime = ThreadLocalRandom.current().nextLong(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
    return RunIds.generate(startTime);
  }
}
