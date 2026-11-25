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

package io.cdap.cdap.internal.app.worker;

import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.File;
import java.io.IOException;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ConfiguratorTask}.
 */
public class ConfiguratorTaskTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testConfiguratorTaskInjector() {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();

    Injector injector = ConfiguratorTask.createInjector(
        CConfiguration.create(),
        discoveryService,
        discoveryService,
        new NoOpMetricsCollectionService());
    Assert.assertNotNull(injector.getInstance(ConfiguratorTask.ConfiguratorTaskRunner.class));
  }

  @Test
  public void testConfigureUnwrapsExecutionExceptionCauses() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("my-art", "1.0.0");
    File unpackedDir = TEMP_FOLDER.newFolder();
    ArtifactLocalizerClient localizerClient = Mockito.mock(ArtifactLocalizerClient.class);
    Mockito.when(localizerClient.getUnpackedArtifactLocation(artifactId)).thenReturn(unpackedDir);

    ArtifactRepository artifactRepo = Mockito.mock(ArtifactRepository.class);
    AppDeploymentInfo info = AppDeploymentInfo.builder()
        .setArtifactId(artifactId)
        .setArtifactLocation(Locations.toLocation(unpackedDir))
        .setNamespaceId(NamespaceId.DEFAULT)
        .setApplicationClass(new ApplicationClass("io.cdap.cdap.DummyApp", "desc", null))
        .setAppName("app1")
        .build();

    ConfiguratorTask.ConfiguratorTaskRunner runner = new ConfiguratorTask.ConfiguratorTaskRunner(
        Mockito.mock(Impersonator.class),
        Mockito.mock(PluginFinder.class),
        artifactRepo,
        cConf,
        localizerClient,
        Mockito.mock(RemoteClientFactory.class));

    // 1. Checked IOException cause inside ExecutionException is propagated directly
    IOException ioFailure = new IOException("failed to open artifact classloader");
    Mockito.when(artifactRepo.createArtifactClassLoader(Mockito.any(), Mockito.any()))
        .thenThrow(ioFailure);
    try {
      runner.configure(info);
      Assert.fail("Expected IOException");
    } catch (IOException actual) {
      Assert.assertSame(ioFailure, actual);
    }

    // 2. Unchecked RuntimeException cause inside ExecutionException is propagated directly
    IllegalStateException uncheckedFailure = new IllegalStateException("bad classloader state");
    Mockito.reset(artifactRepo);
    Mockito.when(artifactRepo.createArtifactClassLoader(Mockito.any(), Mockito.any()))
        .thenThrow(uncheckedFailure);
    try {
      runner.configure(info);
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException actual) {
      Assert.assertSame(uncheckedFailure, actual);
    }

    // 3. Error cause inside ExecutionException is propagated directly
    AssertionError fatalError = new AssertionError("fatal classloader error");
    Mockito.reset(artifactRepo);
    Mockito.when(artifactRepo.createArtifactClassLoader(Mockito.any(), Mockito.any()))
        .thenThrow(fatalError);
    try {
      runner.configure(info);
      Assert.fail("Expected AssertionError");
    } catch (AssertionError actual) {
      Assert.assertSame(fatalError, actual);
    }
  }
}
