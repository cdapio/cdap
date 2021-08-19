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

package io.cdap.cdap.internal.app.deploy;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.gateway.handlers.ArtifactHttpHandlerInternal;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.AppSpecInfo;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactMeta;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.worker.ConfiguratorTask;
import io.cdap.cdap.internal.app.worker.TaskWorkerHttpHandlerInternal;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizer;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerHttpHandlerInternal;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Unit test for {@link RemoteConfigurator} and {@link ConfiguratorTask}.
 */
public class RemoteConfiguratorTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Map<ArtifactId, ArtifactDetail> artifacts = new HashMap<>();

  private static CConfiguration cConf;
  private static NettyHttpService httpService;
  private static RemoteClientFactory remoteClientFactory;
  private static MetricsCollectionService metricsCollectionService;

  @BeforeClass
  public static void init() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    MasterEnvironments.setMasterEnvironment(new TestMasterEnvironment(discoveryService));

    NamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    namespaceAdmin.create(NamespaceMeta.SYSTEM);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);

    remoteClientFactory = new RemoteClientFactory(discoveryService,
                                                  new DefaultInternalAuthenticator(new AuthenticationTestContext()));
    httpService = new CommonNettyHttpServiceBuilder(cConf, "test")
      .setHttpHandlers(
        new TaskWorkerHttpHandlerInternal(cConf, className -> { }, new NoOpMetricsCollectionService()),
        new ArtifactHttpHandlerInternal(new TestArtifactRepository(cConf), namespaceAdmin),
        new ArtifactLocalizerHttpHandlerInternal(new ArtifactLocalizer(cConf, remoteClientFactory))
      )
      .setPort(cConf.getInt(Constants.ArtifactLocalizer.PORT))
      .setChannelPipelineModifier(new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline pipeline) {
          pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
        }
      })
      .build();
    httpService.start();

    discoveryService.register(URIScheme.createDiscoverable(Constants.Service.TASK_WORKER, httpService));
    discoveryService.register(URIScheme.createDiscoverable(Constants.Service.APP_FABRIC_HTTP, httpService));
    metricsCollectionService = new NoOpMetricsCollectionService();
  }

  @AfterClass
  public static void finish() throws Exception {
    httpService.stop();
    MasterEnvironments.setMasterEnvironment(null);
  }

  @After
  public void cleanup() {
    artifacts.clear();
  }

  @Test
  public void testRemoteConfigurator() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, AllProgramsApp.class);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact(AllProgramsApp.class.getSimpleName(), "1.0.0");

    artifacts.put(artifactId, new ArtifactDetail(new ArtifactDescriptor(artifactId.getNamespace(),
                                                                        artifactId.toApiArtifactId(), appJar),
                                                 new ArtifactMeta(ArtifactClasses.builder().build())));

    AppDeploymentInfo info = new AppDeploymentInfo(artifactId, appJar, NamespaceId.DEFAULT,
                                                   new ApplicationClass(AllProgramsApp.class.getName(), "", null),
                                                   null, null, null);

    Configurator configurator = new RemoteConfigurator(cConf, metricsCollectionService, info, remoteClientFactory);

    // Extract response from the configurator.
    ListenableFuture<ConfigResponse> result = configurator.config();
    ConfigResponse response = result.get(10, TimeUnit.SECONDS);
    Assert.assertNotNull(response);

    AppSpecInfo appSpecInfo = response.getAppSpecInfo();
    if (appSpecInfo == null) {
      throw new IllegalStateException("Failed to deploy application");
    }
    ApplicationSpecification specification = appSpecInfo.getAppSpec();
    Assert.assertNotNull(specification);
    Assert.assertEquals(AllProgramsApp.NAME, specification.getName()); // Simple checks.

    ApplicationSpecification expectedSpec = Specifications.from(new AllProgramsApp());
    for (ProgramType programType : ProgramType.values()) {
      Assert.assertEquals(expectedSpec.getProgramsByType(programType), specification.getProgramsByType(programType));
    }
    Assert.assertEquals(expectedSpec.getDatasets(), specification.getDatasets());
  }

  @Test(expected = ExecutionException.class)
  public void testMissingArtifact() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, AllProgramsApp.class);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact(AllProgramsApp.class.getSimpleName(), "1.0.0");

    // Don't update the artifacts map so that the fetching of artifact would fail.

    AppDeploymentInfo info = new AppDeploymentInfo(artifactId, appJar, NamespaceId.DEFAULT,
                                                   new ApplicationClass(AllProgramsApp.class.getName(), "", null),
                                                   null, null, null);

    Configurator configurator = new RemoteConfigurator(cConf, metricsCollectionService, info, remoteClientFactory);

    // Expect the future.get would throw an exception
    configurator.config().get(10, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void testBadAppConfig() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, ConfigTestApp.class);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact(ConfigTestApp.class.getSimpleName(), "1.0.0");

    artifacts.put(artifactId, new ArtifactDetail(new ArtifactDescriptor(artifactId.getNamespace(),
                                                                        artifactId.toApiArtifactId(), appJar),
                                                 new ArtifactMeta(ArtifactClasses.builder().build())));

    AppDeploymentInfo info = new AppDeploymentInfo(artifactId, appJar, NamespaceId.DEFAULT,
                                                   new ApplicationClass(ConfigTestApp.class.getName(), "", null),
                                                   "BadApp", null, GSON.toJson("invalid"));

    Configurator configurator = new RemoteConfigurator(cConf, metricsCollectionService, info, remoteClientFactory);

    // Expect the future.get would throw an exception
    configurator.config().get(10, TimeUnit.SECONDS);
  }

  /**
   * A {@link MasterEnvironment} for testing.
   */
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
      return "test";
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

  /**
   * A {@link ArtifactRepository} that only provides the {@link ArtifactRepositoryReader#newInputStream(Id.Artifact)}
   * method for testing.
   */
  private static final class TestArtifactRepository extends DefaultArtifactRepository {

    TestArtifactRepository(CConfiguration cConf) {
      super(cConf, null, null, null, null, null);
    }

    @Override
    public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
      ArtifactDetail artifactDetail = artifacts.get(artifactId.toEntityId());
      if (artifactDetail == null) {
        throw new NotFoundException("Artifact not found " + artifactId);
      }
      return artifactDetail;
    }
  }
}
