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

package co.cask.cdap.internal.app.deploy;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.app.deploy.ConfigResponse;
import co.cask.cdap.app.deploy.Configurator;
import co.cask.cdap.app.runtime.DummyProgramRunnerFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.deploy.pipeline.AppSpecInfo;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.AuthorizationArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import co.cask.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.DefaultImpersonator;
import co.cask.cdap.security.impersonation.EntityImpersonator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
 * {@link co.cask.cdap.internal.app.deploy.InMemoryConfigurator} &
 * {@link co.cask.cdap.internal.app.deploy.SandboxConfigurator}
 */
public class ConfiguratorTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static CConfiguration conf;
  private static AuthorizationEnforcer authEnforcer;
  private static AuthenticationContext authenticationContext;

  @BeforeClass
  public static void setup() throws IOException {
    conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(new ConfigModule(conf),
                                             new AuthorizationTestModule(),
                                             new AuthorizationEnforcementModule().getInMemoryModules(),
                                             new AuthenticationContextModules().getNoOpModule());
    authEnforcer = injector.getInstance(AuthorizationEnforcer.class);
    authenticationContext = injector.getInstance(AuthenticationContext.class);
  }

  @Test
  public void testInMemoryConfigurator() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, AllProgramsApp.class);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, AllProgramsApp.class.getSimpleName(), "1.0.0");
    CConfiguration cConf = CConfiguration.create();
    ArtifactRepository baseArtifactRepo = new DefaultArtifactRepository(conf, null, null,
                                                                        new DummyProgramRunnerFactory(),
                                                                        new DefaultImpersonator(cConf, null));
    ArtifactRepository artifactRepo = new AuthorizationArtifactRepository(baseArtifactRepo,
                                                                          authEnforcer, authenticationContext);


    // Create a configurator that is testable. Provide it a application.
    try (CloseableClassLoader artifactClassLoader =
           artifactRepo.createArtifactClassLoader(
             appJar, new EntityImpersonator(artifactId.getNamespace().toEntityId(),
                                            new DefaultImpersonator(cConf, null)))) {
      Configurator configurator = new InMemoryConfigurator(conf, Id.Namespace.DEFAULT, artifactId,
                                                           AllProgramsApp.class.getName(), artifactRepo,
                                                           artifactClassLoader, null, null, "");
      // Extract response from the configurator.
      ListenableFuture<ConfigResponse> result = configurator.config();
      ConfigResponse response = result.get(10, TimeUnit.SECONDS);
      Assert.assertNotNull(response);

      // Deserialize the JSON spec back into Application object.
      AppSpecInfo appSpecInfo = GSON.fromJson(response.getResponse(), AppSpecInfo.class);
      ApplicationSpecification specification = appSpecInfo.getAppSpec();
      Assert.assertNotNull(specification);
      Assert.assertEquals(AllProgramsApp.NAME, specification.getName()); // Simple checks.

      ApplicationSpecification expectedSpec = Specifications.from(new AllProgramsApp());
      for (ProgramType programType : ProgramType.values()) {
        Assert.assertEquals(expectedSpec.getProgramsByType(programType), specification.getProgramsByType(programType));
      }
      Assert.assertEquals(expectedSpec.getDatasets(), specification.getDatasets());
    }
  }

  @Test
  public void testAppWithConfig() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, ConfigTestApp.class);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, ConfigTestApp.class.getSimpleName(), "1.0.0");
    CConfiguration cConf = CConfiguration.create();
    ArtifactRepository baseArtifactRepo = new DefaultArtifactRepository(conf, null, null,
                                                                        new DummyProgramRunnerFactory(),
                                                                        new DefaultImpersonator(cConf, null));
    ArtifactRepository artifactRepo = new AuthorizationArtifactRepository(baseArtifactRepo,
                                                                          authEnforcer, authenticationContext);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("myTable");
    // Create a configurator that is testable. Provide it an application.
    try (CloseableClassLoader artifactClassLoader =
           artifactRepo.createArtifactClassLoader(
             appJar, new EntityImpersonator(artifactId.getNamespace().toEntityId(),
                                            new DefaultImpersonator(cConf, null)))) {
      Configurator configuratorWithConfig =
        new InMemoryConfigurator(conf, Id.Namespace.DEFAULT, artifactId, ConfigTestApp.class.getName(),
                                 artifactRepo, artifactClassLoader, null, null, new Gson().toJson(config));

      ListenableFuture<ConfigResponse> result = configuratorWithConfig.config();
      ConfigResponse response = result.get(10, TimeUnit.SECONDS);
      Assert.assertNotNull(response);

      AppSpecInfo appSpecInfo = GSON.fromJson(response.getResponse(), AppSpecInfo.class);
      ApplicationSpecification specification = appSpecInfo.getAppSpec();
      Assert.assertNotNull(specification);
      Assert.assertEquals(1, specification.getDatasets().size());
      Assert.assertTrue(specification.getDatasets().containsKey("myTable"));

      Configurator configuratorWithoutConfig = new InMemoryConfigurator(
        conf, Id.Namespace.DEFAULT, artifactId, ConfigTestApp.class.getName(),
        artifactRepo, artifactClassLoader, null, null, null);
      result = configuratorWithoutConfig.config();
      response = result.get(10, TimeUnit.SECONDS);
      Assert.assertNotNull(response);

      specification = GSON.fromJson(response.getResponse(), AppSpecInfo.class).getAppSpec();
      Assert.assertNotNull(specification);
      Assert.assertEquals(1, specification.getDatasets().size());
      Assert.assertTrue(specification.getDatasets().containsKey(ConfigTestApp.DEFAULT_TABLE));
      Assert.assertNotNull(specification.getProgramSchedules().get(ConfigTestApp.SCHEDULE_NAME));

      ProgramStatusTrigger trigger = (ProgramStatusTrigger) specification.getProgramSchedules()
                                                                         .get(ConfigTestApp.SCHEDULE_NAME).getTrigger();
      Assert.assertEquals(trigger.getProgramId().getProgram(), ConfigTestApp.WORKFLOW_NAME);
    }
  }
}
