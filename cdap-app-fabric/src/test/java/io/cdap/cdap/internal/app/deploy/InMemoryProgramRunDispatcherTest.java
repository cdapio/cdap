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

package io.cdap.cdap.internal.app.deploy;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.app.deploy.ProgramRunDispatcherContext;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactMeta;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.impersonation.ImpersonatedOpType;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link InMemoryProgramRunDispatcher}.
 */
public class InMemoryProgramRunDispatcherTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static String sha256Utf16LeHex(String... parts) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    for (String part : parts) {
      digest.update(part.getBytes(StandardCharsets.UTF_16LE));
    }
    byte[] hash = digest.digest();
    StringBuilder sb = new StringBuilder(hash.length * 2);
    for (byte b : hash) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }

  private ApplicationSpecification createAppSpecWithPlugin(
      io.cdap.cdap.api.artifact.ArtifactId pluginApiArtifactId) {
    PluginClass pluginClass = PluginClass.builder()
        .setName("pName")
        .setType("pType")
        .setDescription("desc")
        .setClassName("com.example.PluginImpl")
        .setProperties(Collections.emptyMap())
        .build();
    Plugin plugin = new Plugin(
        Collections.emptyList(),
        pluginApiArtifactId,
        pluginClass,
        PluginProperties.builder().build());
    ApplicationSpecification spec = Mockito.mock(ApplicationSpecification.class);
    Mockito.when(spec.getPlugins()).thenReturn(Collections.singletonMap("plugin1", plugin));
    return spec;
  }

  @Test
  public void testArtifactHashCalculationAndCloseableCleanup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.AppFabric.ARTIFACTS_COMPUTE_HASH, true);
    cConf.setBoolean(Constants.AppFabric.ARTIFACTS_COMPUTE_HASH_SNAPSHOT, false);
    cConf.setInt(Constants.AppFabric.ARTIFACTS_COMPUTE_HASH_TIME_BUCKET_DAYS, 0);

    List<String> closedOrder = new ArrayList<>();
    AtomicReference<ProgramOptions> capturedOptions = new AtomicReference<>();
    ProgramController expectedController = Mockito.mock(ProgramController.class);

    CloseableProgramRunner runner = new CloseableProgramRunner(
        capturedOptions, expectedController, closedOrder);
    ProgramRunnerFactory runnerFactory = programType -> runner;

    File appJar = TEMP_FOLDER.newFile("app-1.0.0.jar");
    File pluginJar = TEMP_FOLDER.newFile("plugin-2.0.0.jar");

    io.cdap.cdap.api.artifact.ArtifactId appApiArtifact =
        new io.cdap.cdap.api.artifact.ArtifactId(
            "my-app-artifact", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    io.cdap.cdap.api.artifact.ArtifactId pluginApiArtifact =
        new io.cdap.cdap.api.artifact.ArtifactId(
            "my-plugin-artifact", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM);

    ArtifactDetail appDetail = new ArtifactDetail(
        new ArtifactDescriptor(
            NamespaceId.DEFAULT.getNamespace(), appApiArtifact, Locations.toLocation(appJar)),
        new ArtifactMeta(ArtifactClasses.builder()
            .addApp(new ApplicationClass("com.example.MyApp", "", null))
            .build()));
    ArtifactDetail pluginDetail = new ArtifactDetail(
        new ArtifactDescriptor(
            NamespaceId.SYSTEM.getNamespace(), pluginApiArtifact, Locations.toLocation(pluginJar)),
        new ArtifactMeta(ArtifactClasses.builder().build()));

    ArtifactRepository repo = Mockito.mock(ArtifactRepository.class);
    ArtifactId appProtoArtifact = NamespaceId.DEFAULT.artifact("my-app-artifact", "1.0.0");
    ArtifactId pluginProtoArtifact = NamespaceId.SYSTEM.artifact("my-plugin-artifact", "2.0.0");
    Mockito.when(repo.getArtifact(Id.Artifact.fromEntityId(appProtoArtifact)))
        .thenReturn(appDetail);
    Mockito.when(repo.getArtifact(Id.Artifact.fromEntityId(pluginProtoArtifact)))
        .thenReturn(pluginDetail);

    Program executableProgram = Mockito.mock(Program.class);
    Mockito.doAnswer(invocation -> {
      closedOrder.add("program");
      throw new IOException("program close failure");
    }).when(executableProgram).close();

    InMemoryProgramRunDispatcher dispatcher = new InMemoryProgramRunDispatcher(
        cConf, runnerFactory, null,
        new LocalLocationFactory(TEMP_FOLDER.newFolder()), null, repo, null) {
      @Override
      protected Program createProgram(CConfiguration conf, ProgramRunner programRunner,
          ProgramDescriptor programDescriptor, ArtifactDetail artifactDetail,
          File tempDir, boolean isTetheredRun) {
        return executableProgram;
      }
    };

    ProgramId programId =
        NamespaceId.DEFAULT.app("testApp").program(ProgramType.WORKER, "worker1");
    ApplicationSpecification appSpec = createAppSpecWithPlugin(pluginApiArtifact);
    ProgramDescriptor descriptor = new ProgramDescriptor(programId, appSpec, appProtoArtifact);
    Map<String, String> sysArgs = ImmutableMap.of(ProgramOptionConstants.IS_PREVIEW, "true");
    ProgramOptions options = new SimpleProgramOptions(
        programId, new BasicArguments(sysArgs), new BasicArguments(Collections.emptyMap()));
    RunId runId = RunIds.generate();

    ProgramRunDispatcherContext context =
        new ProgramRunDispatcherContext(descriptor, options, runId, false);
    ProgramController actualController = dispatcher.dispatchProgram(context);
    Assert.assertSame(expectedController, actualController);

    ProgramOptions actualOptions = capturedOptions.get();
    Assert.assertNotNull(actualOptions);

    String expectedProgramHash = sha256Utf16LeHex(
        NamespaceId.DEFAULT.getNamespace(), "my-app-artifact", "USER", "1.0.0");
    String expectedPluginHash = sha256Utf16LeHex(
        pluginProtoArtifact.getParent().toString(),
        pluginProtoArtifact.getArtifact(),
        pluginProtoArtifact.getVersion());

    Assert.assertEquals(expectedProgramHash,
        actualOptions.getArguments().getOption(ProgramOptionConstants.PROGRAM_JAR_HASH));
    Assert.assertEquals(expectedPluginHash,
        actualOptions.getArguments().getOption(ProgramOptionConstants.PLUGIN_DIR_HASH));

    // Execute cleanup tasks in LIFO order; IOException from close() must be swallowed quietly.
    context.executeCleanupTasks();
    Assert.assertEquals(2, closedOrder.size());
    Assert.assertEquals("program", closedOrder.get(0));
    Assert.assertEquals("runner", closedOrder.get(1));
  }

  @Test
  public void testDistributedPluginCopyImpersonationExceptions() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    File appJar = TEMP_FOLDER.newFile("app-dist.jar");
    File pluginJar = TEMP_FOLDER.newFile("plugin-dist.jar");
    io.cdap.cdap.api.artifact.ArtifactId appApiArtifact =
        new io.cdap.cdap.api.artifact.ArtifactId(
            "app-dist", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    io.cdap.cdap.api.artifact.ArtifactId pluginApiArtifact =
        new io.cdap.cdap.api.artifact.ArtifactId(
            "plugin-dist", new ArtifactVersion("1.0.0"), ArtifactScope.USER);

    ArtifactDetail appDetail = new ArtifactDetail(
        new ArtifactDescriptor(
            NamespaceId.DEFAULT.getNamespace(), appApiArtifact, Locations.toLocation(appJar)),
        new ArtifactMeta(ArtifactClasses.builder()
            .addApp(new ApplicationClass("com.example.App", "", null))
            .build()));
    ArtifactDetail pluginDetail = new ArtifactDetail(
        new ArtifactDescriptor(
            NamespaceId.DEFAULT.getNamespace(), pluginApiArtifact, Locations.toLocation(pluginJar)),
        new ArtifactMeta(ArtifactClasses.builder().build()));

    ArtifactRepository repo = Mockito.mock(ArtifactRepository.class);
    ArtifactId appProtoArtifact = NamespaceId.DEFAULT.artifact("app-dist", "1.0.0");
    ArtifactId pluginProtoArtifact = NamespaceId.DEFAULT.artifact("plugin-dist", "1.0.0");
    Mockito.when(repo.getArtifact(Id.Artifact.fromEntityId(appProtoArtifact)))
        .thenReturn(appDetail);
    Mockito.when(repo.getArtifact(Id.Artifact.fromEntityId(pluginProtoArtifact)))
        .thenReturn(pluginDetail);

    AtomicReference<Exception> impersonationFailure = new AtomicReference<>();
    Impersonator impersonator = new Impersonator() {
      @Override
      public <T> T doAs(NamespacedEntityId entityId, Callable<T> callable) throws Exception {
        Exception ex = impersonationFailure.get();
        if (ex != null) {
          throw ex;
        }
        return callable.call();
      }

      @Override
      public <T> T doAs(NamespacedEntityId entityId, Callable<T> callable,
          ImpersonatedOpType impersonatedOpType) throws Exception {
        return doAs(entityId, callable);
      }

      @Override
      public UserGroupInformation getUGI(NamespacedEntityId entityId) {
        throw new UnsupportedOperationException();
      }
    };

    Program dummyProgram = Mockito.mock(Program.class);
    ProgramController dummyController = Mockito.mock(ProgramController.class);
    ProgramRunnerFactory runnerFactory = type -> (program, opts) -> dummyController;

    InMemoryProgramRunDispatcher dispatcher = new InMemoryProgramRunDispatcher(
        cConf, runnerFactory, impersonator,
        new LocalLocationFactory(TEMP_FOLDER.newFolder()), null, repo, null) {
      @Override
      protected Program createProgram(CConfiguration conf, ProgramRunner programRunner,
          ProgramDescriptor programDescriptor, ArtifactDetail artifactDetail,
          File tempDir, boolean isTetheredRun) {
        return dummyProgram;
      }
    };

    ProgramId programId =
        NamespaceId.DEFAULT.app("testApp").program(ProgramType.WORKER, "worker1");
    ApplicationSpecification appSpec = createAppSpecWithPlugin(pluginApiArtifact);
    ProgramDescriptor descriptor = new ProgramDescriptor(programId, appSpec, appProtoArtifact);
    ProgramOptions options = new SimpleProgramOptions(
        programId,
        new BasicArguments(ImmutableMap.of(ProgramOptionConstants.IS_PREVIEW, "true")),
        new BasicArguments(Collections.emptyMap()));

    // 1. Checked IOException must propagate directly as IOException.
    IOException expectedIo = new IOException("hdfs copy failed");
    impersonationFailure.set(expectedIo);
    ProgramRunDispatcherContext ctx1 =
        new ProgramRunDispatcherContext(descriptor, options, RunIds.generate(), true);
    try {
      dispatcher.dispatchProgram(ctx1);
      Assert.fail("Expected IOException");
    } catch (IOException actualIo) {
      Assert.assertSame(expectedIo, actualIo);
    } finally {
      ctx1.executeCleanupTasks();
    }

    // 2. Unchecked RuntimeException must propagate unwrapped.
    IllegalStateException expectedRuntime = new IllegalStateException("ugi runtime failure");
    impersonationFailure.set(expectedRuntime);
    ProgramRunDispatcherContext ctx2 =
        new ProgramRunDispatcherContext(descriptor, options, RunIds.generate(), true);
    try {
      dispatcher.dispatchProgram(ctx2);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actualRuntime) {
      Assert.assertSame(expectedRuntime, actualRuntime);
    } finally {
      ctx2.executeCleanupTasks();
    }

    // 3. FileAlreadyExistsException is logged and tolerated.
    impersonationFailure.set(new FileAlreadyExistsException("plugin-dist.jar"));
    ProgramRunDispatcherContext ctx3 =
        new ProgramRunDispatcherContext(descriptor, options, RunIds.generate(), true);
    try {
      Assert.assertSame(dummyController, dispatcher.dispatchProgram(ctx3));
    } finally {
      ctx3.executeCleanupTasks();
    }
  }

  private static final class CloseableProgramRunner implements ProgramRunner, Closeable {

    private final AtomicReference<ProgramOptions> capturedOptions;
    private final ProgramController controller;
    private final List<String> closedOrder;

    private CloseableProgramRunner(AtomicReference<ProgramOptions> capturedOptions,
        ProgramController controller, List<String> closedOrder) {
      this.capturedOptions = capturedOptions;
      this.controller = controller;
      this.closedOrder = closedOrder;
    }

    @Override
    public ProgramController run(Program program, ProgramOptions options) {
      capturedOptions.set(options);
      return controller;
    }

    @Override
    public void close() throws IOException {
      closedOrder.add("runner");
      throw new IOException("runner close failure");
    }
  }
}
