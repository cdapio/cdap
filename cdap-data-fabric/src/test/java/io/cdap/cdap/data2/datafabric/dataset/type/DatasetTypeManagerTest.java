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

package io.cdap.cdap.data2.datafabric.dataset.type;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.datafabric.dataset.service.TestModule1;
import io.cdap.cdap.data2.datafabric.dataset.service.TestModule2;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import java.io.File;
import java.util.concurrent.Callable;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DatasetTypeManager} unchecked exception rethrow and classloader cleanup.
 */
public class DatasetTypeManagerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final IllegalStateException MODULE_INIT_FAILURE =
      new IllegalStateException("unchecked module init failure");

  private static CConfiguration cConf;
  private static LocationFactory locationFactory;
  private static TransactionManager txManager;
  private static TransactionRunner txRunner;

  /**
   * DatasetModule that throws a deterministic unchecked {@link RuntimeException}.
   */
  public static class UncheckedFailingModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      throw MODULE_INIT_FAILURE;
    }
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new LocalLocationModule(),
        new InMemoryDiscoveryModule(),
        new TransactionInMemoryModule(),
        new SystemDatasetRuntimeModule().getInMemoryModules(),
        new NamespaceAdminTestModule(),
        new AuthorizationTestModule(),
        new AuthorizationEnforcementModule().getInMemoryModules(),
        new AuthenticationContextModules().getMasterModule(),
        new StorageModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class)
                .to(NoOpMetricsCollectionService.class)
                .in(Scopes.SINGLETON);
          }
        });

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
    txRunner = injector.getInstance(TransactionRunner.class);
    locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
  }

  @AfterClass
  public static void tearDownClass() {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }

  @Test
  public void testAddDeleteAndDeleteModulesRethrowExactUncheckedRuntimeExceptionInstance()
      throws Exception {
    NamespaceId ns = new NamespaceId("rethrowNs");
    DatasetModuleId mod1 = ns.datasetModule("mod1");

    // 1a. addModule: unchecked RuntimeException thrown inside module.register() must NOT be double-wrapped
    DatasetTypeManager realTxManager = new DatasetTypeManager(
        cConf, locationFactory, new DefaultImpersonator(cConf, null), txRunner);
    try {
      realTxManager.addModule(mod1, UncheckedFailingModule.class.getName(), null, false);
      Assert.fail("Expected exact MODULE_INIT_FAILURE instance");
    } catch (RuntimeException actual) {
      Assert.assertSame(MODULE_INIT_FAILURE, actual);
    }

    // 1b. addModule: unchecked RuntimeException from TransactionRunner must NOT be double-wrapped
    final IllegalArgumentException txAddFailure = new IllegalArgumentException("tx add failure");
    TransactionRunner failingRunner = runnable -> {
      throw new TransactionException("tx error", txAddFailure);
    };
    DatasetTypeManager txFailingManager = new DatasetTypeManager(
        cConf, locationFactory, new DefaultImpersonator(cConf, null), failingRunner);
    try {
      txFailingManager.addModule(mod1, TestModule1.class.getName(), null, false);
      Assert.fail("Expected exact txAddFailure instance");
    } catch (RuntimeException actual) {
      Assert.assertSame(txAddFailure, actual);
    }

    // Add a real module with a jar location so deleteModule and deleteModules exercise impersonator.doAs()
    Location jar1 = AppJarHelper.createDeploymentJar(locationFactory, TestModule1.class);
    realTxManager.addModule(mod1, TestModule1.class.getName(), jar1, false);
    DatasetModuleMeta storedMeta = realTxManager.getModule(mod1);
    Assert.assertNotNull(storedMeta);

    // 2. deleteModule: unchecked RuntimeException inside impersonator.doAs must NOT be double-wrapped
    final IllegalStateException deleteFailure = new IllegalStateException("impersonator delete boom");
    Impersonator failingImpersonator = Mockito.mock(Impersonator.class);
    Mockito.when(failingImpersonator.doAs(Mockito.eq(mod1), Mockito.any(Callable.class)))
        .thenThrow(deleteFailure);
    Mockito.when(failingImpersonator.doAs(Mockito.eq(ns), Mockito.any(Callable.class)))
        .thenThrow(deleteFailure);

    DatasetTypeManager impFailingManager =
        new DatasetTypeManager(cConf, locationFactory, failingImpersonator, txRunner);
    try {
      impFailingManager.deleteModule(mod1);
      Assert.fail("Expected exact deleteFailure instance from deleteModule");
    } catch (RuntimeException actual) {
      Assert.assertSame(deleteFailure, actual);
    }

    // 3. deleteModules: unchecked RuntimeException inside impersonator.doAs must NOT be double-wrapped
    try {
      impFailingManager.deleteModules(ns);
      Assert.fail("Expected exact deleteFailure instance from deleteModules");
    } catch (RuntimeException actual) {
      Assert.assertSame(deleteFailure, actual);
    }

    // Clean up module
    Assert.assertTrue(realTxManager.deleteModule(mod1));
  }

  @Test
  public void testAddModuleClosesClassLoaderAndUnpackedFolderQuietlyAndHandlesDependencies()
      throws Exception {
    File tempRoot = TEMP_FOLDER.newFolder();
    CConfiguration localConf = CConfiguration.copy(cConf);
    localConf.set(Constants.CFG_LOCAL_DATA_DIR, tempRoot.getAbsolutePath());
    File unpackBase = new File(tempRoot, localConf.get(Constants.AppFabric.TEMP_DIR));

    DatasetTypeManager manager = new DatasetTypeManager(
        localConf, locationFactory, new DefaultImpersonator(localConf, null), txRunner);

    NamespaceId ns = new NamespaceId("cleanupNs");
    DatasetModuleId mod1 = ns.datasetModule("module1");
    DatasetModuleId mod2 = ns.datasetModule("module2");
    DatasetModuleId badMod = ns.datasetModule("badMod");

    Location jar1 = AppJarHelper.createDeploymentJar(locationFactory, TestModule1.class);
    Location jar2 = AppJarHelper.createDeploymentJar(locationFactory, TestModule2.class);
    Location badJar = AppJarHelper.createDeploymentJar(locationFactory, UncheckedFailingModule.class);

    // Deploy mod1 and dependent mod2 from JARs (exercises cl.close(), classLoaderFolder.close(),
    // and DependencyTrackingRegistry.get())
    manager.addModule(mod1, TestModule1.class.getName(), jar1, false);
    manager.addModule(mod2, TestModule2.class.getName(), jar2, false);
    Assert.assertNotNull(manager.getTypeInfo(ns.datasetType("datasetType2")));

    // Deploying a failing JAR must ALSO close cl and delete the unpacked temp folder in finally
    try {
      manager.addModule(badMod, UncheckedFailingModule.class.getName(), badJar, false);
      Assert.fail("Expected failure from UncheckedFailingModule");
    } catch (RuntimeException expected) {
      // UncheckedFailingModule loaded from DirectoryClassLoader is a separate Class instance or same;
      // verify message matches and not double-wrapped in RuntimeException
      Assert.assertEquals(IllegalStateException.class.getName(), expected.getClass().getName());
      Assert.assertNull(expected.getCause());
    }

    // ClassLoaderFolder.close() deletes each unpacked temporary directory inside unpackBase
    File[] remainingUnpackedDirs = unpackBase.listFiles();
    Assert.assertNotNull(remainingUnpackedDirs);
    Assert.assertEquals(0, remainingUnpackedDirs.length);

    // Conflict when deleting mod1 while mod2 depends on it
    try {
      manager.deleteModule(mod1);
      Assert.fail("Expected DatasetModuleConflictException");
    } catch (DatasetModuleConflictException expected) {
      Assert.assertTrue(expected.getMessage().contains("module1"));
    }

    manager.deleteModules(ns);
    Assert.assertTrue(manager.getModules(ns).isEmpty());
  }
}
