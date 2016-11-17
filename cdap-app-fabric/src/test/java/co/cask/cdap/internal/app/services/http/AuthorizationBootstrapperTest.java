/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services.http;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.test.TestRunner;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceEnsurer;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.SystemArtifactLoader;
import co.cask.cdap.internal.app.runtime.artifact.app.plugin.PluginTestApp;
import co.cask.cdap.internal.app.runtime.artifact.app.plugin.PluginTestRunnable;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizationBootstrapper;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tephra.TransactionManager;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;

/**
 * Tests authorization for default namespace, system artifacts, system datasets, etc
 */
@RunWith(TestRunner.class)
public class AuthorizationBootstrapperTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final ArtifactId SYSTEM_ARTIFACT = NamespaceId.SYSTEM.artifact("system-artifact", "1.0.0");
  private static final Principal ADMIN_USER = new Principal("alice", Principal.PrincipalType.USER);

  private static AuthorizationBootstrapper authorizationBootstrapper;
  private static TransactionManager txManager;
  private static DatasetService datasetService;
  private static DefaultNamespaceEnsurer defaultNamespaceEnsurer;
  private static SystemArtifactLoader systemArtifactLoader;
  private static NamespaceQueryAdmin namespaceQueryAdmin;
  private static NamespaceAdmin namespaceAdmin;
  private static AuthorizationEnforcementService authorizationEnforcementService;
  private static ArtifactRepository artifactRepository;
  private static DatasetFramework dsFramework;
  private static DiscoveryServiceClient discoveryServiceClient;
  private static InstanceId instanceId;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.CACHE_ENABLED, false);
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                              InMemoryAuthorizer.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, deploymentJar.toURI().getPath());
    // make Alice an admin user, so she can create namespaces
    cConf.set(Constants.Security.Authorization.ADMIN_USERS, ADMIN_USER.getName());
    instanceId = new InstanceId(cConf.get(Constants.INSTANCE_NAME));
    // setup a system artifact
    File systemArtifactsDir = TMP_FOLDER.newFolder();
    cConf.set(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR, systemArtifactsDir.getAbsolutePath());
    createSystemArtifact(systemArtifactsDir);
    Injector injector = Guice.createInjector(new AppFabricTestModule(cConf));
    namespaceQueryAdmin = injector.getInstance(NamespaceQueryAdmin.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    defaultNamespaceEnsurer = new DefaultNamespaceEnsurer(namespaceAdmin);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    txManager = injector.getInstance(TransactionManager.class);
    datasetService = injector.getInstance(DatasetService.class);
    authorizationEnforcementService = injector.getInstance(AuthorizationEnforcementService.class);
    authorizationEnforcementService.startAndWait();
    systemArtifactLoader = injector.getInstance(SystemArtifactLoader.class);
    authorizationBootstrapper = injector.getInstance(AuthorizationBootstrapper.class);
    artifactRepository = injector.getInstance(ArtifactRepository.class);
    dsFramework = injector.getInstance(DatasetFramework.class);
  }

  @Test
  public void test() throws Exception {
    final Principal systemUser = new Principal(
      UserGroupInformation.getCurrentUser().getShortUserName(), Principal.PrincipalType.USER
    );
    // initial state: no privileges for system or admin users
    Predicate<EntityId> systemUserFilter = authorizationEnforcementService.createFilter(systemUser);
    Predicate<EntityId> adminUserFilter = authorizationEnforcementService.createFilter(ADMIN_USER);
    Assert.assertFalse(systemUserFilter.apply(instanceId));
    Assert.assertFalse(systemUserFilter.apply(NamespaceId.SYSTEM));
    Assert.assertFalse(adminUserFilter.apply(NamespaceId.DEFAULT));

    // privileges should be granted after running bootstrap
    authorizationBootstrapper.run();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Predicate<EntityId> systemUserFilter = authorizationEnforcementService.createFilter(systemUser);
        Predicate<EntityId> adminUserFilter = authorizationEnforcementService.createFilter(ADMIN_USER);
        return systemUserFilter.apply(instanceId) && systemUserFilter.apply(NamespaceId.SYSTEM) &&
          adminUserFilter.apply(NamespaceId.DEFAULT);
      }
    }, 10, TimeUnit.SECONDS);

    txManager.startAndWait();
    datasetService.startAndWait();
    waitForService(Constants.Service.DATASET_MANAGER);
    defaultNamespaceEnsurer.startAndWait();
    systemArtifactLoader.startAndWait();
    waitForService(defaultNamespaceEnsurer);
    waitForService(systemArtifactLoader);
    // ensure that the default namespace was created, and that the system user has privileges to access it
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          return namespaceQueryAdmin.exists(NamespaceId.DEFAULT);
        } catch (Exception e) {
          return false;
        }
      }
    }, 10, TimeUnit.SECONDS);
    Assert.assertTrue(defaultNamespaceEnsurer.isRunning());
    // ensure that the system artifact was deployed, and that the system user has privileges to access it
    // this will throw an ArtifactNotFoundException if the artifact was not deployed, and UnauthorizedException if
    // the user does not have required privileges
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          artifactRepository.getArtifact(SYSTEM_ARTIFACT.toId());
          return true;
        } catch (Exception e) {
          return false;
        }
      }
    }, 20, TimeUnit.SECONDS);
    Assert.assertTrue(systemArtifactLoader.isRunning());
    // ensure that system datasets can be created by the system user
    Dataset systemDataset = DatasetsUtil.getOrCreateDataset(
      dsFramework, NamespaceId.SYSTEM.dataset("system-dataset"), Table.class.getName(), DatasetProperties.EMPTY,
      Collections.<String, String>emptyMap(), this.getClass().getClassLoader());
    Assert.assertNotNull(systemDataset);
    // as part of bootstrapping, admin users were also granted admin privileges on the CDAP instance, so they can
    // create namespaces
    SecurityRequestContext.setUserId(ADMIN_USER.getName());
    namespaceAdmin.create(new NamespaceMeta.Builder().setName("success").build());
    SecurityRequestContext.setUserId("bob");
    try {
      namespaceAdmin.create(new NamespaceMeta.Builder().setName("failure").build());
      Assert.fail("Bob should not have been able to create a namespace since he is not an admin user");
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  @AfterClass
  public static void teardown() {
    datasetService.stopAndWait();
    txManager.stopAndWait();
    authorizationEnforcementService.stopAndWait();
  }

  private void waitForService(final AbstractService service) throws Exception {
    Tasks.waitFor(Service.State.RUNNING, new Callable<Service.State>() {
      @Override
      public Service.State call() throws Exception {
        return service.state();
      }
    }, 10, TimeUnit.SECONDS);
  }

  private void waitForService(String discoverableName) throws InterruptedException {
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(discoveryServiceClient.discover(discoverableName));
    Preconditions.checkNotNull(endpointStrategy.pick(10, TimeUnit.SECONDS),
                               "%s service is not up after 10 seconds", discoverableName);
  }

  private static File createAppJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                              cls, manifest);
    DirUtils.mkdirs(destFile.getParentFile());
    Files.copy(Locations.newInputSupplier(deploymentJar), destFile);
    return destFile;
  }

  private static void createSystemArtifact(File systemArtifactsDir) throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, PluginTestRunnable.class.getPackage().getName());
    File systemArtifact = new File(
      systemArtifactsDir, String.format("%s-%s.jar", SYSTEM_ARTIFACT.getArtifact(), SYSTEM_ARTIFACT.getVersion())
    );
    createAppJar(PluginTestApp.class, systemArtifact, manifest);

  }
}
