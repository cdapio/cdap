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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.ServiceSocksProxy;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.ServiceSocksProxyAuthenticator;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillController;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link RemoteExecutionTwillRunnerService}.
 */
public class RemoteExecutionTwillRunnerServiceTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testSocksProxyStartStopAndServiceProxySecretLifecycle() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tempFolder.newFolder("data").getAbsolutePath());
    cConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 5000L);

    LocalLocationFactory locationFactory =
        new LocalLocationFactory(tempFolder.newFolder("locations"));
    ProvisioningService provisioningService = Mockito.mock(ProvisioningService.class);
    Mockito.when(provisioningService.getRuntimeJobManager(Mockito.any(), Mockito.any()))
        .thenReturn(Optional.empty());

    RemoteExecutionTwillRunnerService runnerService = new RemoteExecutionTwillRunnerService(
        cConf,
        new Configuration(),
        new InMemoryDiscoveryService(),
        locationFactory,
        provisioningService,
        Mockito.mock(ProgramStateWriter.class),
        Mockito.mock(TransactionRunner.class),
        Mockito.mock(AccessTokenCodec.class),
        Mockito.mock(TokenManager.class));

    runnerService.doInitialize();
    try {
      ServiceSocksProxy socksProxy = getField(runnerService, "serviceSocksProxy");
      final ServiceSocksProxyAuthenticator authenticator =
          getField(runnerService, "serviceSocksProxyAuthenticator");
      Assert.assertEquals(Service.State.NEW, socksProxy.state());

      Location keysDir = locationFactory.create("keys");
      Assert.assertTrue(keysDir.mkdirs());
      writeText(keysDir.append(Constants.RuntimeMonitor.PUBLIC_KEY), "ssh-rsa AAAAB3Nza test@host");
      writeText(keysDir.append(Constants.RuntimeMonitor.PRIVATE_KEY), "PRIVATE-KEY-CONTENT");

      ProgramRunId runId = NamespaceId.DEFAULT.app("app").spark("sparkJob").run(RunIds.generate());
      Method secretGenMethod = RemoteExecutionTwillRunnerService.class.getDeclaredMethod(
          "generateAndSaveServiceProxySecret", ProgramRunId.class, Location.class);
      secretGenMethod.setAccessible(true);
      Location secretLocation = (Location) secretGenMethod.invoke(runnerService, runId, keysDir);
      Assert.assertTrue(secretLocation.exists());
      String generatedSecret = new String(
          com.google.common.io.ByteStreams.toByteArray(secretLocation.getInputStream()),
          StandardCharsets.UTF_8);
      Assert.assertEquals(40, generatedSecret.length());

      Node masterNode = new Node(
          "master-1", Node.Type.MASTER, "127.0.0.1", 1000L, Collections.emptyMap());
      Cluster cluster = new Cluster(
          "test-cluster",
          ClusterStatus.RUNNING,
          Collections.singletonList(masterNode),
          Collections.singletonMap(Constants.RuntimeMonitor.SSH_USER, "cdap"));

      Gson gson = new Gson();
      Map<String, String> systemArgs = new HashMap<>();
      systemArgs.put(ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name());
      systemArgs.put(ProgramOptionConstants.SECURE_KEYS_DIR, gson.toJson(keysDir.toURI()));
      systemArgs.put(ProgramOptionConstants.CLUSTER, gson.toJson(cluster));
      systemArgs.put(SystemArguments.PROFILE_NAME, "default");
      systemArgs.put(SystemArguments.PROFILE_PROVISIONER, "gce");

      RunRecordDetail runRecord = RunRecordDetail.builder()
          .setProgramRunId(runId)
          .setStartTime(System.currentTimeMillis())
          .setArtifactId(new ArtifactId("art", new ArtifactVersion("1.0"), ArtifactScope.USER))
          .setStatus(ProgramRunStatus.RUNNING)
          .setProperties(Collections.singletonMap("runtimeArgs", "{}"))
          .setSystemArgs(systemArgs)
          .setProfileId(NamespaceId.DEFAULT.profile("gce"))
          .setSourceId(new byte[MessageId.RAW_ID_SIZE])
          .build();

      TwillController controller = runnerService.createTwillControllerFromRunRecord(runRecord);
      Assert.assertNotNull(controller);

      // Creating an SSH controller starts serviceSocksProxy via startAndWait()
      Assert.assertEquals(Service.State.RUNNING, socksProxy.state());
      Assert.assertTrue(socksProxy.getBindAddress().getPort() > 0);

      // Wait for SSHRemoteExecutionService listener running() to register the proxy secret
      Tasks.waitFor(
          true,
          () -> authenticator.authenticate(generatedSecret, generatedSecret),
          5,
          TimeUnit.SECONDS,
          10,
          TimeUnit.MILLISECONDS);

      // Stopping runnerService stops serviceSocksProxy via stopAndWait() and releases controllers,
      // which stops SSHRemoteExecutionService and removes the proxy secret via terminated()/failed()
      runnerService.stop();
      Assert.assertEquals(Service.State.TERMINATED, socksProxy.state());
      Tasks.waitFor(
          false,
          () -> authenticator.authenticate(generatedSecret, generatedSecret),
          5,
          TimeUnit.SECONDS,
          10,
          TimeUnit.MILLISECONDS);
    } finally {
      runnerService.stop();
    }
  }

  @Test
  public void testMonitorControllerStartsExecutionServiceOnStartupCompletion() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tempFolder.newFolder("data2").getAbsolutePath());
    cConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 100L);

    RemoteExecutionTwillRunnerService runnerService = new RemoteExecutionTwillRunnerService(
        cConf,
        new Configuration(),
        new InMemoryDiscoveryService(),
        new LocalLocationFactory(tempFolder.newFolder("loc2")),
        Mockito.mock(ProvisioningService.class),
        Mockito.mock(ProgramStateWriter.class),
        Mockito.mock(TransactionRunner.class),
        Mockito.mock(AccessTokenCodec.class),
        Mockito.mock(TokenManager.class));

    runnerService.doInitialize();
    try {
      ScheduledExecutorService scheduler = getField(runnerService, "scheduler");
      ProgramRunId runId = NamespaceId.DEFAULT.app("app").spark("sparkJob").run(RunIds.generate());
      RemoteProcessController processController = Mockito.mock(RemoteProcessController.class);
      Mockito.when(processController.isRunning()).thenReturn(true);

      RemoteExecutionService executionService = new RemoteExecutionService(
          cConf, runId, scheduler, processController, Mockito.mock(ProgramStateWriter.class));
      CompletableFuture<Void> startupCompletion = new CompletableFuture<>();
      RemoteExecutionTwillController controller = new RemoteExecutionTwillController(
          cConf, runId, startupCompletion, processController, scheduler, executionService, false);

      runnerService.addController(runId, controller);
      runnerService.monitorController(runId, startupCompletion, controller, executionService);
      Assert.assertEquals(Service.State.NEW, executionService.state());

      startupCompletion.complete(null);
      Tasks.waitFor(
          Service.State.RUNNING,
          executionService::state,
          5,
          TimeUnit.SECONDS,
          10,
          TimeUnit.MILLISECONDS);
    } finally {
      runnerService.stop();
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T getField(Object target, String fieldName) throws Exception {
    Field field = RemoteExecutionTwillRunnerService.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (T) field.get(target);
  }

  private static void writeText(Location location, String content) throws Exception {
    try (OutputStream out = location.getOutputStream()) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
  }
}
