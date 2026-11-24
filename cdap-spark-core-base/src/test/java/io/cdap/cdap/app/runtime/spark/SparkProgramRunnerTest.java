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
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.spark.SparkHttpServiceHandlerSpecification;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SparkProgramRunner}.
 */
public class SparkProgramRunnerTest {

  @Test
  public void testRunPropagatesUncheckedDirectlyAndWrapsCheckedException() throws Exception {
    SparkProgramRunner runner = createSparkProgramRunner();
    ProgramId programId = new ProgramId("default", "app", ProgramType.SPARK, "sparkProg");
    Program program = createMockProgram(programId);

    // 1. Missing HOST triggers Preconditions IllegalArgumentException; outer catch must rethrow it directly.
    Map<String, String> noHostArgs = new HashMap<>();
    noHostArgs.put(ProgramOptionConstants.RUN_ID, RunIds.generate().getId());
    ProgramOptions missingHostOptions = new SimpleProgramOptions(
        programId, new BasicArguments(noHostArgs), new BasicArguments(Collections.<String, String>emptyMap()));
    try {
      runner.run(program, missingHostOptions);
      Assert.fail("Expected IllegalArgumentException when HOST is missing");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("No hostname"));
    }

    // 2. Checked ClassNotFoundException from program.getMainClass() must be wrapped in RuntimeException
    //    after closing closeables cleanly.
    ClassNotFoundException expectedCnfe = new ClassNotFoundException("MissingSparkMainClass");
    Mockito.when(program.getMainClass()).thenThrow(expectedCnfe);

    Map<String, String> validArgs = new HashMap<>();
    validArgs.put(ProgramOptionConstants.RUN_ID, RunIds.generate().getId());
    validArgs.put(ProgramOptionConstants.HOST, "127.0.0.1");
    validArgs.put(ProgramOptionConstants.ARTIFACT_ID, "default:art:1.0.0");
    ProgramOptions validOptions = new SimpleProgramOptions(
        programId, new BasicArguments(validArgs), new BasicArguments(Collections.<String, String>emptyMap()));

    try {
      runner.run(program, validOptions);
      Assert.fail("Expected RuntimeException wrapping ClassNotFoundException");
    } catch (RuntimeException caught) {
      Assert.assertSame(expectedCnfe, caught.getCause());
    }

    // 3. close() starts the delayed daemon cleanup thread without throwing.
    runner.close();
  }

  private static SparkProgramRunner createSparkProgramRunner() {
    MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
    Mockito.when(metricsContext.childContext(Mockito.anyMap())).thenReturn(metricsContext);
    MetricsCollectionService metricsService = Mockito.mock(MetricsCollectionService.class);
    Mockito.when(metricsService.getContext(Mockito.anyMap())).thenReturn(metricsContext);

    return new SparkProgramRunner(
        CConfiguration.create(),
        new Configuration(),
        Mockito.mock(LocationFactory.class),
        Mockito.mock(TransactionSystemClient.class),
        Mockito.mock(DatasetFramework.class),
        metricsService,
        Mockito.mock(SecureStore.class),
        Mockito.mock(SecureStoreManager.class),
        Mockito.mock(AccessEnforcer.class),
        Mockito.mock(AuthenticationContext.class),
        Mockito.mock(MessagingService.class),
        Mockito.mock(ServiceAnnouncer.class),
        Mockito.mock(PluginFinder.class),
        Mockito.mock(MetadataReader.class),
        Mockito.mock(MetadataPublisher.class),
        Mockito.mock(FieldLineageWriter.class),
        Mockito.mock(NamespaceQueryAdmin.class),
        Mockito.mock(RemoteClientFactory.class),
        Mockito.mock(CommonNettyHttpServiceFactory.class),
        Mockito.mock(AppStateStoreProvider.class));
  }

  private static Program createMockProgram(ProgramId programId) {
    Program program = Mockito.mock(Program.class);
    ApplicationSpecification appSpec = Mockito.mock(ApplicationSpecification.class);
    SparkSpecification sparkSpec = new SparkSpecification(
        "TestSpark", programId.getProgram(), "desc", "MissingSparkMainClass",
        Collections.<String>emptySet(), Collections.<String, String>emptyMap(),
        new Resources(1024, 1), new Resources(1024, 1), new Resources(1024, 1),
        Collections.<SparkHttpServiceHandlerSpecification>emptyList(), Collections.<String, Plugin>emptyMap());
    Mockito.when(appSpec.getSpark()).thenReturn(Collections.singletonMap(programId.getProgram(), sparkSpec));
    Mockito.when(program.getId()).thenReturn(programId);
    Mockito.when(program.getName()).thenReturn(programId.getProgram());
    Mockito.when(program.getType()).thenReturn(ProgramType.SPARK);
    Mockito.when(program.getApplicationSpecification()).thenReturn(appSpec);
    Mockito.when(program.getClassLoader()).thenReturn(SparkProgramRunnerTest.class.getClassLoader());
    return program;
  }
}
