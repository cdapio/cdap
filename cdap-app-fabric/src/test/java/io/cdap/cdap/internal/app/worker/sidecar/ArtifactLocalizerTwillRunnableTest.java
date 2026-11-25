/*
 * Copyright © 2022-2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.util.concurrent.Service;
import com.sun.net.httpserver.HttpServer;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Configuration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.distributed.MockMasterEnvironment;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.security.auth.TokenManager;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.twill.api.TwillContext;
import org.apache.twill.common.Cancellable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Tests for {@link ArtifactLocalizerTwillRunnable}.
 */
public class ArtifactLocalizerTwillRunnableTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDoInitializeRunStopAndDestroy() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, temporaryFolder.newFolder().getAbsolutePath());
    cConf.setInt(Constants.ArtifactLocalizer.PORT, 0);
    cConf.set(Constants.ArtifactLocalizer.PRELOAD_LIST, "");
    cConf.setInt(Constants.ArtifactLocalizer.PRELOAD_VERSION_LIMIT, 1);

    File temporaryCConfFile = temporaryFolder.newFile("cConf.xml");
    File temporaryHConfFile = temporaryFolder.newFile("hConf.xml");
    try (FileOutputStream fileOutputStream = new FileOutputStream(temporaryCConfFile.getAbsolutePath())) {
      cConf.writeXml(fileOutputStream);
    }
    try (FileOutputStream fileOutputStream = new FileOutputStream(temporaryHConfFile.getAbsolutePath())) {
      new Configuration().writeXml(fileOutputStream);
    }

    HttpServer mockAppFabric =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    byte[] emptyArrayJson = "[]".getBytes(StandardCharsets.UTF_8);
    mockAppFabric.createContext("/", exchange -> {
      exchange.sendResponseHeaders(200, emptyArrayJson.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(emptyArrayJson);
      }
    });
    mockAppFabric.start();

    MockMasterEnvironment masterEnv = new MockMasterEnvironment();
    Cancellable reg = masterEnv.getDiscoveryServiceSupplier().get().register(
        URIScheme.HTTP.createDiscoverable(
            Constants.Service.APP_FABRIC_HTTP, mockAppFabric.getAddress()));
    MasterEnvironments.setMasterEnvironment(masterEnv);
    try {
      ArtifactLocalizerTwillRunnable runnable = new ArtifactLocalizerTwillRunnable(
          temporaryCConfFile.getAbsolutePath(), temporaryHConfFile.getAbsolutePath());
      runnable.doInitialize();

      Field tokenManagerField =
          ArtifactLocalizerTwillRunnable.class.getDeclaredField("tokenManager");
      tokenManagerField.setAccessible(true);
      TokenManager tokenManager = (TokenManager) tokenManagerField.get(runnable);
      Assert.assertEquals(Service.State.RUNNING, tokenManager.state());

      Field serviceField =
          ArtifactLocalizerTwillRunnable.class.getDeclaredField("artifactLocalizerService");
      serviceField.setAccessible(true);
      ArtifactLocalizerService localizerService =
          (ArtifactLocalizerService) serviceField.get(runnable);

      CountDownLatch runCompleted = new CountDownLatch(1);
      Thread runnerThread = new Thread(() -> {
        try {
          runnable.run();
        } finally {
          runCompleted.countDown();
        }
      });
      runnerThread.start();

      Tasks.waitFor(Service.State.RUNNING, localizerService::state, 5, TimeUnit.SECONDS);

      runnable.stop();
      Assert.assertTrue(runCompleted.await(15, TimeUnit.SECONDS));
      Assert.assertEquals(Service.State.TERMINATED, localizerService.state());

      runnable.destroy();
      Assert.assertEquals(Service.State.TERMINATED, tokenManager.state());
    } finally {
      reg.cancel();
      mockAppFabric.stop(0);
      MasterEnvironments.setMasterEnvironment(null);
    }
  }

  @Test
  public void testInitializePropagatesRuntimeExceptionUnwrapped() {
    ArtifactLocalizerTwillRunnable runnable =
        new ArtifactLocalizerTwillRunnable("missing-cConf.xml", "missing-hConf.xml");
    TwillContext context = Mockito.mock(TwillContext.class);
    Mockito.when(context.getSpecification()).thenReturn(null);
    try {
      runnable.initialize(context);
      Assert.fail("Expected NullPointerException");
    } catch (NullPointerException expected) {
      Assert.assertNull(expected.getCause());
    }
  }
}
