/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.gateway.handlers.FileFetcherHttpHandlerInternal;
import io.cdap.cdap.internal.app.runtime.artifact.app.inspection.InspectionApp;
import io.cdap.cdap.internal.app.worker.TaskWorkerHttpHandlerInternal;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import io.cdap.cdap.security.authorization.DefaultAuthorizationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.NettyHttpService;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;
import java.util.jar.Manifest;

public class RemoteArtifactInspectTaskTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static CConfiguration cConf;
  private static LocationFactory locationFactory;
  private static NettyHttpService httpService;
  private static InMemoryDiscoveryService discoveryService;
  private static RemoteArtifactInspector remoteArtifactInspector;
  private static RemoteClientFactory remoteClientFactory;

  @BeforeClass
  public static void setup() throws Exception {
    // Create cConf with TaskWorker enabled.
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, 10001);
    cConf.setBoolean(Constants.Security.ENABLED, false);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);

    // Create Location factory used for storing artifact in preparation of inspection remotely.
    locationFactory = new LocalLocationFactory(tmpFolder.newFolder());

    // Create discovery service used for submitting remote inspection task.
    discoveryService = new InMemoryDiscoveryService();
    MasterEnvironments.setMasterEnvironment(new TestMasterEnvironment(discoveryService));

    // File fetcher http handler for task worker to download artifact from for inspection.
    FileFetcherHttpHandlerInternal fileFetcherHttpHandlerInternal = Guice.createInjector(
      new ConfigModule(cConf),
      new LocalLocationModule()).getInstance(FileFetcherHttpHandlerInternal.class);

    // Start up http service
    httpService = new CommonNettyHttpServiceBuilder(cConf, "RemoteArtifactInspectTaskTest")
      .setHttpHandlers(
        new TaskWorkerHttpHandlerInternal(cConf, className -> {
        }),
        fileFetcherHttpHandlerInternal)
      .build();
    httpService.start();

    discoveryService.register(URIScheme.createDiscoverable(Constants.Service.TASK_WORKER, httpService));
    discoveryService.register(URIScheme.createDiscoverable(Constants.Service.APP_FABRIC_HTTP, httpService));

    remoteClientFactory = new RemoteClientFactory(discoveryService, null);

    // Create remote inspector
    remoteArtifactInspector = new RemoteArtifactInspector(cConf, new LocalLocationFactory(), remoteClientFactory);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    httpService.stop();
    MasterEnvironments.setMasterEnvironment(null);
  }

  private static File createJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(tmpFolder.newFolder()),
                                                              cls, manifest);
    DirUtils.mkdirs(destFile.getParentFile());
    Locations.linkOrCopyOverwrite(deploymentJar, destFile);
    return destFile;
  }

  @Test
  public void testRemoteArtifactInspector()
    throws IOException, UnauthorizedException, UnsupportedTypeException, InvalidArtifactException {
    // Create an artifact for inspection
    File artifactFile = getAppFile();
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "InspectionApp", "1.0.0");

    // Run remote inspection
    ArtifactClassesWithMetadata metadata =
      remoteArtifactInspector.inspectArtifact(artifactId, artifactFile, null,
                                              null, Collections.EMPTY_SET);
    // Validation
    Set<ApplicationClass> expectedApps = ImmutableSet.of(new ApplicationClass(
      InspectionApp.class.getName(), "",
      new ReflectionSchemaGenerator(false).generate(InspectionApp.AConfig.class),
      new Requirements(Collections.emptySet(), Collections.singleton("cdc"))));
    Assert.assertEquals(expectedApps, metadata.getArtifactClasses().getApps());
  }

  @Test(expected = InvalidArtifactException.class)
  public void testRemoteArtifactInspectorWithAdditionalPluginClasses()
    throws IOException, UnauthorizedException, UnsupportedTypeException, InvalidArtifactException {
    // Create an artifact for inspection
    File artifactFile = getAppFile();
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "InspectionApp", "1.0.0");

    // PluginClass contains a plugin that is not present in the artifact
    PluginClass pluginClass =
      PluginClass.builder().setName("plugin_name").setType("plugin_type")
        .setDescription("").setClassName("non-existing-class")
        .setConfigFieldName("pluginConf").setProperties(ImmutableMap.of()).build();

    // Inspects the jar and ensures that an exception is thrown as the plugin above is not present.
    remoteArtifactInspector.inspectArtifact(artifactId, artifactFile, null,
                                            null, ImmutableSet.of(pluginClass));
  }

  private File getAppFile() throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, InspectionApp.class.getPackage().getName());
    return createJar(InspectionApp.class, new File(tmpFolder.newFolder(), "InspectionApp-1.0.0.jar"), manifest);
  }

  private static final class TestMasterEnvironment implements MasterEnvironment {
    private final InMemoryDiscoveryService discoveryService;

    private TestMasterEnvironment(InMemoryDiscoveryService discoveryService) {
      this.discoveryService = discoveryService;
    }

    @Override
    public MasterEnvironmentRunnable createRunnable(MasterEnvironmentRunnableContext context,
                                                    Class<? extends MasterEnvironmentRunnable> runnableClass) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
      return "TestMasterEnv";
    }

    @Override
    public Supplier<DiscoveryService> getDiscoveryServiceSupplier() {
      return () -> discoveryService;
    }

    @Override
    public Supplier<DiscoveryServiceClient> getDiscoveryServiceClientSupplier() {
      return () -> discoveryService;
    }

    @Override
    public Supplier<TwillRunnerService> getTwillRunnerSupplier() {
      throw new UnsupportedOperationException();
    }
  }
}
