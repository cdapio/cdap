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

package io.cdap.cdap.internal.app.runtime.service;

import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.AeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.io.File;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ServiceProgramRunner}.
 */
public class ServiceProgramRunnerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  /**
   * Minimal HTTP handler for {@link DummyService}.
   */
  public static final class PingHandler extends AbstractHttpServiceHandler {
    @GET
    @Path("/ping")
    public void ping(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }
  }

  /**
   * Minimal CDAP {@link AbstractService} for lifecycle testing.
   */
  public static final class DummyService extends AbstractService {
    @Override
    protected void configure() {
      setName("DummyService");
      addHandler(new PingHandler());
    }
  }

  @Test
  public void testRunHandlesFailureWhenPluginInstantiatorIsNull() throws Exception {
    CConfiguration cConf = createCConf();
    IllegalStateException expected = new IllegalStateException("artifact manager failed");
    ArtifactManagerFactory failingFactory = (ns, retryStrategy) -> {
      throw expected;
    };

    ServiceProgramRunner runner = createRunner(cConf, failingFactory);
    Program program = createTestProgram(cConf, TEMP_FOLDER.newFile());
    ProgramOptions options = createOptions(null);

    try {
      runner.run(program, options);
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException thrown) {
      Assert.assertSame(expected, thrown);
    } finally {
      program.close();
    }
  }

  @Test
  public void testRunClosesNonNullPluginInstantiatorOnFailure() throws Exception {
    CConfiguration cConf = createCConf();
    File pluginsDir = TEMP_FOLDER.newFolder("plugins");
    IllegalStateException expected = new IllegalStateException("artifact manager failed with plugins");
    ArtifactManagerFactory failingFactory = (ns, retryStrategy) -> {
      throw expected;
    };

    ServiceProgramRunner runner = createRunner(cConf, failingFactory);
    Program program = createTestProgram(cConf, TEMP_FOLDER.newFile());
    ProgramOptions options = createOptions(pluginsDir);

    try {
      runner.run(program, options);
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException thrown) {
      Assert.assertSame(expected, thrown);
    } finally {
      program.close();
    }

    File appFabricTemp = new File(
        cConf.get(Constants.CFG_LOCAL_DATA_DIR), cConf.get(Constants.AppFabric.TEMP_DIR));
    File[] subDirs = appFabricTemp.listFiles();
    if (subDirs != null) {
      for (File subDir : subDirs) {
        String[] remaining = subDir.list();
        Assert.assertTrue(remaining == null || remaining.length == 0);
      }
    }
  }

  @Test
  public void testRunStartsAndStopsServiceComponent() throws Exception {
    CConfiguration cConf = createCConf();
    ArtifactManagerFactory factory = (ns, retryStrategy) -> Mockito.mock(ArtifactManager.class);
    ServiceProgramRunner runner = createRunner(cConf, factory);
    Program program = createTestProgram(cConf, TEMP_FOLDER.newFile());
    ProgramOptions options = createOptions(TEMP_FOLDER.newFolder("plugins-live"));

    try {
      CountDownLatch aliveLatch = new CountDownLatch(1);
      ProgramController controller = runner.run(program, options);
      controller.addListener(new AbstractListener() {
        @Override
        public void init(ProgramController.State currentState, Throwable cause) {
          if (currentState == ProgramController.State.ALIVE) {
            aliveLatch.countDown();
          }
        }

        @Override
        public void alive() {
          aliveLatch.countDown();
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      Assert.assertTrue(aliveLatch.await(10, TimeUnit.SECONDS));
      controller.command(ProgramOptionConstants.INSTANCES, 2).get(5, TimeUnit.SECONDS);
      controller.stop().get(10, TimeUnit.SECONDS);
      Assert.assertEquals(ProgramController.State.KILLED, controller.getState());
    } finally {
      program.close();
    }
  }

  private CConfiguration createCConf() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    return cConf;
  }

  private ServiceProgramRunner createRunner(
      CConfiguration cConf, ArtifactManagerFactory artifactManagerFactory) {
    NoOpMetricsCollectionService metrics = new NoOpMetricsCollectionService();
    InMemoryDiscoveryService discovery = new InMemoryDiscoveryService();
    ServiceAnnouncer announcer = new ServiceAnnouncer() {
      @Override
      public Cancellable announce(String serviceName, int port) {
        return () -> { };
      }

      @Override
      public Cancellable announce(String serviceName, int port, byte[] payload) {
        return () -> { };
      }
    };

    CommonNettyHttpServiceFactory nettyFactory = new CommonNettyHttpServiceFactory(
        cConf, metrics, Mockito.mock(AuditLogWriter.class), Mockito.mock(AeadCipher.class));

    return new ServiceProgramRunner(
        cConf,
        metrics,
        Mockito.mock(DatasetFramework.class),
        discovery,
        new ConstantTransactionSystemClient(),
        announcer,
        Mockito.mock(SecureStore.class),
        Mockito.mock(SecureStoreManager.class),
        Mockito.mock(MessagingService.class),
        artifactManagerFactory,
        Mockito.mock(MetadataReader.class),
        Mockito.mock(MetadataPublisher.class),
        Mockito.mock(NamespaceQueryAdmin.class),
        Mockito.mock(PluginFinder.class),
        Mockito.mock(FieldLineageWriter.class),
        Mockito.mock(TransactionRunner.class),
        Mockito.mock(PreferencesFetcher.class),
        Mockito.mock(RemoteClientFactory.class),
        Mockito.mock(ContextAccessEnforcer.class),
        nettyFactory,
        Mockito.mock(AppStateStoreProvider.class),
        SConfiguration.create(),
        Mockito.mock(AeadCipher.class));
  }

  private ProgramOptions createOptions(File pluginsDir) throws Exception {
    Map<String, String> systemArgs = new HashMap<>();
    systemArgs.put(ProgramOptionConstants.INSTANCE_ID, "0");
    systemArgs.put(ProgramOptionConstants.INSTANCES, "1");
    systemArgs.put(ProgramOptionConstants.RUN_ID, RunIds.generate().getId());
    systemArgs.put(ProgramOptionConstants.ARTIFACT_ID, "default:artifact:1.0.0");
    systemArgs.put(ProgramOptionConstants.HOST, InetAddress.getLoopbackAddress().getHostName());
    if (pluginsDir != null) {
      systemArgs.put(ProgramOptionConstants.PLUGIN_DIR, pluginsDir.getAbsolutePath());
    }
    ProgramId programId = NamespaceId.DEFAULT.app("TestApp").service("DummyService");
    return new SimpleProgramOptions(programId, new BasicArguments(systemArgs), new BasicArguments());
  }

  private Program createTestProgram(CConfiguration cConf, File jarFile) throws Exception {
    File unpackedDir = TEMP_FOLDER.newFolder();
    ProgramClassLoader programClassLoader =
        new ProgramClassLoader(cConf, unpackedDir, ServiceProgramRunnerTest.class.getClassLoader());
    HttpServiceHandlerSpecification handlerSpec = new HttpServiceHandlerSpecification(
        PingHandler.class.getName(),
        "PingHandler",
        "desc",
        Collections.emptyMap(),
        Collections.emptySet(),
        Collections.emptyList());
    ServiceSpecification serviceSpec = new ServiceSpecification(
        DummyService.class.getName(),
        "DummyService",
        "desc",
        Collections.singletonMap("PingHandler", handlerSpec),
        new Resources(),
        1,
        Collections.emptyMap());
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
        "TestApp",
        "1.0.0",
        "desc",
        null,
        new ArtifactId("artifact", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("DummyService", serviceSpec),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap());
    ProgramId programId = NamespaceId.DEFAULT.app("TestApp").service("DummyService");
    Location jarLocation = Locations.toLocation(jarFile);

    return new Program() {
      @Override
      public String getMainClassName() {
        return DummyService.class.getName();
      }

      @SuppressWarnings("unchecked")
      @Override
      public <T> Class<T> getMainClass() {
        return (Class<T>) DummyService.class;
      }

      @Override
      public ProgramType getType() {
        return ProgramType.SERVICE;
      }

      @Override
      public ProgramId getId() {
        return programId;
      }

      @Override
      public String getName() {
        return programId.getProgram();
      }

      @Override
      public String getNamespaceId() {
        return programId.getNamespace();
      }

      @Override
      public String getApplicationId() {
        return programId.getApplication();
      }

      @Override
      public ApplicationSpecification getApplicationSpecification() {
        return appSpec;
      }

      @Override
      public Location getJarLocation() {
        return jarLocation;
      }

      @Override
      public ClassLoader getClassLoader() {
        return programClassLoader;
      }

      @Override
      public void close() throws java.io.IOException {
        programClassLoader.close();
      }
    };
  }
}
