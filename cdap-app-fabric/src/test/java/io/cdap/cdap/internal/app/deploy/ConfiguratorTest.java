/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.runtime.DummyProgramRunnerFactory;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.AppSpecInfo;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.AuthorizationArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.LocalPluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Tests the configurators.
 *
 * NOTE: Until we can build the JAR it's difficult to test other configurators
 * {@link io.cdap.cdap.internal.app.deploy.InMemoryConfigurator}
 */
public class ConfiguratorTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static CConfiguration conf;
  private static AccessEnforcer authEnforcer;
  private static AuthenticationContext authenticationContext;

  @BeforeClass
  public static void setup() throws IOException {
    conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(new ConfigModule(conf),
                                             new AuthorizationTestModule(),
                                             new AuthorizationEnforcementModule().getInMemoryModules(),
                                             new AuthenticationContextModules().getNoOpModule(),
                                             new AbstractModule() {
                                               @Override
                                               protected void configure() {
                                                 bind(MetricsCollectionService.class)
                                                   .to(NoOpMetricsCollectionService.class);
                                               }
                                             });
    authEnforcer = injector.getInstance(AccessEnforcer.class);
    authenticationContext = injector.getInstance(AuthenticationContext.class);
  }

  @Test
  public void testInMemoryConfigurator() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, AllProgramsApp.class);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, AllProgramsApp.class.getSimpleName(), "1.0.0");
    CConfiguration cConf = CConfiguration.create();
    ArtifactRepository baseArtifactRepo = new DefaultArtifactRepository(conf,
                                                                        null,
                                                                        null,
                                                                        null,
                                                                        new DummyProgramRunnerFactory(),
                                                                        new DefaultImpersonator(cConf, null));
    ArtifactRepository artifactRepo = new AuthorizationArtifactRepository(baseArtifactRepo,
                                                                          authEnforcer, authenticationContext);
    PluginFinder pluginFinder = new LocalPluginFinder(artifactRepo);

    AppDeploymentInfo appDeploymentInfo = new AppDeploymentInfo(artifactId.toEntityId(), appJar,
      NamespaceId.DEFAULT, new ApplicationClass(AllProgramsApp.class.getName(), "", null),
      null, null, null);

    // Create a configurator that is testable. Provide it a application.
    Configurator configurator = new InMemoryConfigurator(conf, pluginFinder, new DefaultImpersonator(cConf, null),
                                                         artifactRepo, null, appDeploymentInfo);
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

  @Test
  public void testAppWithConfig() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, ConfigTestApp.class);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, ConfigTestApp.class.getSimpleName(), "1.0.0");
    CConfiguration cConf = CConfiguration.create();
    ArtifactRepository baseArtifactRepo = new DefaultArtifactRepository(conf,
                                                                        null,
                                                                        null,
                                                                        null,
                                                                        new DummyProgramRunnerFactory(),
                                                                        new DefaultImpersonator(cConf, null));
    ArtifactRepository artifactRepo = new AuthorizationArtifactRepository(baseArtifactRepo,
                                                                          authEnforcer, authenticationContext);
    PluginFinder pluginFinder = new LocalPluginFinder(artifactRepo);
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("myTable");

    AppDeploymentInfo appDeploymentInfo = new AppDeploymentInfo(artifactId.toEntityId(), appJar,
      NamespaceId.DEFAULT, new ApplicationClass(ConfigTestApp.class.getName(), "", null),
      null, null, new Gson().toJson(config));

    // Create a configurator that is testable. Provide it an application.
    Configurator configurator = new InMemoryConfigurator(conf, pluginFinder, new DefaultImpersonator(cConf, null),
                                                         artifactRepo, null, appDeploymentInfo);

    ListenableFuture<ConfigResponse> result = configurator.config();
    ConfigResponse response = result.get(10, TimeUnit.SECONDS);
    Assert.assertNotNull(response);

    AppSpecInfo appSpecInfo = response.getAppSpecInfo();
    if (appSpecInfo == null) {
      throw new IllegalStateException("Failed to deploy application");
    }
    ApplicationSpecification specification = appSpecInfo.getAppSpec();
    Assert.assertNotNull(specification);
    Assert.assertEquals(1, specification.getDatasets().size());
    Assert.assertTrue(specification.getDatasets().containsKey("myTable"));

    // Create a deployment info without the app configuration
    appDeploymentInfo = new AppDeploymentInfo(artifactId.toEntityId(), appJar,
      NamespaceId.DEFAULT, new ApplicationClass(ConfigTestApp.class.getName(), "", null),
      null, null, null);

    Configurator configuratorWithoutConfig = new InMemoryConfigurator(conf, pluginFinder,
                                                                      new DefaultImpersonator(cConf, null),
                                                                      artifactRepo, null, appDeploymentInfo);
    result = configuratorWithoutConfig.config();
    response = result.get(10, TimeUnit.SECONDS);
    Assert.assertNotNull(response);

    appSpecInfo = response.getAppSpecInfo();
    if (appSpecInfo == null) {
      throw new IllegalStateException("Failed to deploy application");
    }
    specification = appSpecInfo.getAppSpec();
    Assert.assertNotNull(specification);
    Assert.assertEquals(1, specification.getDatasets().size());
    Assert.assertTrue(specification.getDatasets().containsKey(ConfigTestApp.DEFAULT_TABLE));
    Assert.assertNotNull(specification.getProgramSchedules().get(ConfigTestApp.SCHEDULE_NAME));

    ProgramStatusTrigger trigger = (ProgramStatusTrigger) specification.getProgramSchedules()
                                                                       .get(ConfigTestApp.SCHEDULE_NAME).getTrigger();
    Assert.assertEquals(trigger.getProgramId().getProgram(), ConfigTestApp.WORKFLOW_NAME);
  }
}
