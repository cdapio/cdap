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

package io.cdap.cdap.app.runtime.spark;

import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.spark.SparkHttpServiceHandlerSpecification;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.internal.dataset.DatasetCreationSpec;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SparkRuntimeContext}.
 */
public class SparkRuntimeContextTest {

  @Test
  public void testToStringAndCloseSwallowsCloseableIoException() {
    final AtomicBoolean closeCalled = new AtomicBoolean(false);
    Closeable failingCloseable = new Closeable() {
      @Override
      public void close() throws IOException {
        closeCalled.set(true);
        throw new IOException("simulated close failure");
      }
    };

    ProgramId programId = new ProgramId("ns1", "app1", ProgramType.SPARK, "sparkProg");
    RunId runId = RunIds.fromString("11111111-2222-3333-4444-555555555555");
    SparkRuntimeContext context = createRuntimeContext(
        CConfiguration.create(), new Configuration(), programId, runId,
        Collections.<SparkHttpServiceHandlerSpecification>emptyList(), null, failingCloseable);

    String expected = "SparkRuntimeContext{id=" + programId + ", runId=" + runId.getId() + "}";
    Assert.assertEquals(expected, context.toString());

    context.close();
    Assert.assertTrue(closeCalled.get());
  }

  /**
   * Creates a lightweight {@link SparkRuntimeContext} for fast unit testing across Spark runtime packages.
   */
  public static SparkRuntimeContext createRuntimeContext(CConfiguration cConf, Configuration hConf) {
    ProgramId programId = new ProgramId("default", "testApp", ProgramType.SPARK, "testSpark");
    return createRuntimeContext(
        cConf, hConf, programId, RunIds.generate(),
        Collections.<SparkHttpServiceHandlerSpecification>emptyList(), null, new Closeable() {
          @Override
          public void close() {
            // no-op
          }
        });
  }

  /**
   * Populates {@link SparkRuntimeContextConfig} entries in {@code hConf} for cross-package tests.
   */
  public static void initContextConfig(Configuration hConf, SparkRuntimeContext context) {
    new SparkRuntimeContextConfig(hConf).set(context, null);
  }

  /**
   * Creates a customizable {@link SparkRuntimeContext} for unit tests.
   */
  public static SparkRuntimeContext createRuntimeContext(
      CConfiguration cConf,
      Configuration hConf,
      ProgramId programId,
      RunId runId,
      List<SparkHttpServiceHandlerSpecification> handlers,
      @Nullable WorkflowProgramInfo workflowInfo,
      Closeable closeable) {
    return createRuntimeContext(
        cConf, hConf, programId, runId, handlers, workflowInfo, closeable,
        Mockito.mock(LocationFactory.class), Mockito.mock(AppStateStoreProvider.class));
  }

  /**
   * Creates a customizable {@link SparkRuntimeContext} with explicit {@link LocationFactory}
   * and {@link AppStateStoreProvider} for unit tests.
   */
  public static SparkRuntimeContext createRuntimeContext(
      CConfiguration cConf,
      Configuration hConf,
      ProgramId programId,
      RunId runId,
      List<SparkHttpServiceHandlerSpecification> handlers,
      @Nullable WorkflowProgramInfo workflowInfo,
      Closeable closeable,
      LocationFactory locationFactory,
      AppStateStoreProvider appStateStoreProvider) {
    Program program = Mockito.mock(Program.class);
    SparkSpecification sparkSpec = new SparkSpecification(
        "TestSparkClass", programId.getProgram(), "test description", "TestMainClass",
        Collections.<String>emptySet(), Collections.<String, String>emptyMap(),
        new Resources(1024, 1), new Resources(1024, 1), new Resources(1024, 1),
        handlers, Collections.<String, Plugin>emptyMap());

    ArtifactId artifactId = new ArtifactId("testArtifact", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
        programId.getApplication(),
        programId.getVersion(),
        "6.12.0",
        "test application",
        null,
        artifactId,
        Collections.<String, String>emptyMap(),
        Collections.<String, DatasetCreationSpec>emptyMap(),
        Collections.<String, MapReduceSpecification>emptyMap(),
        Collections.singletonMap(programId.getProgram(), sparkSpec),
        Collections.<String, WorkflowSpecification>emptyMap(),
        Collections.<String, ServiceSpecification>emptyMap(),
        Collections.<String, ScheduleCreationSpec>emptyMap(),
        Collections.<String, WorkerSpecification>emptyMap(),
        Collections.<String, Plugin>emptyMap());

    Mockito.when(program.getId()).thenReturn(programId);
    Mockito.when(program.getName()).thenReturn(programId.getProgram());
    Mockito.when(program.getNamespaceId()).thenReturn(programId.getNamespace());
    Mockito.when(program.getApplicationId()).thenReturn(programId.getApplication());
    Mockito.when(program.getType()).thenReturn(ProgramType.SPARK);
    Mockito.when(program.getMainClassName()).thenReturn("TestMainClass");
    Mockito.when(program.getApplicationSpecification()).thenReturn(appSpec);
    Mockito.when(program.getClassLoader()).thenReturn(SparkRuntimeContextTest.class.getClassLoader());

    MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
    Mockito.when(metricsContext.childContext(Mockito.anyMap())).thenReturn(metricsContext);
    Mockito.when(metricsContext.childContext(Mockito.anyString(), Mockito.anyString())).thenReturn(metricsContext);
    MetricsCollectionService metricsService = Mockito.mock(MetricsCollectionService.class);
    Mockito.when(metricsService.getContext(Mockito.anyMap())).thenReturn(metricsContext);

    Map<String, String> sysArgs = new HashMap<>();
    sysArgs.put(ProgramOptionConstants.RUN_ID, runId.getId());
    sysArgs.put(ProgramOptionConstants.ARTIFACT_ID, programId.getNamespace() + ":testArtifact:1.0.0");
    sysArgs.put(ProgramOptionConstants.LOGICAL_START_TIME, "1000");
    Arguments systemArgs = new BasicArguments(sysArgs);
    Arguments userArgs = new BasicArguments(Collections.<String, String>emptyMap());
    ProgramOptions programOptions = new SimpleProgramOptions(programId, systemArgs, userArgs);

    ServiceAnnouncer serviceAnnouncer = new ServiceAnnouncer() {
      @Override
      public Cancellable announce(String serviceName, int port) {
        return new Cancellable() {
          @Override
          public void cancel() {
            // no-op
          }
        };
      }

      @Override
      public Cancellable announce(String serviceName, int port, byte[] payload) {
        return announce(serviceName, port);
      }
    };

    return new SparkRuntimeContext(
        hConf, program, programOptions, cConf, "127.0.0.1",
        new ConstantTransactionSystemClient(),
        Mockito.mock(DatasetFramework.class),
        metricsService,
        workflowInfo,
        null,
        Mockito.mock(SecureStore.class),
        Mockito.mock(SecureStoreManager.class),
        Mockito.mock(AccessEnforcer.class),
        Mockito.mock(AuthenticationContext.class),
        Mockito.mock(MessagingService.class),
        serviceAnnouncer,
        Mockito.mock(PluginFinder.class),
        locationFactory,
        Mockito.mock(MetadataReader.class),
        Mockito.mock(MetadataPublisher.class),
        Mockito.mock(NamespaceQueryAdmin.class),
        Mockito.mock(FieldLineageWriter.class),
        Mockito.mock(RemoteClientFactory.class),
        closeable,
        appStateStoreProvider);
  }
}
