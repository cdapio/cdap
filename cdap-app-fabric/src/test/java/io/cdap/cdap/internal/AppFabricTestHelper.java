/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.NamespaceAlreadyExistsException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.CConfigurationUtil;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import io.cdap.cdap.internal.app.deploy.LocalApplicationManager;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.services.ProgramNotificationSubscriberService;
import io.cdap.cdap.internal.app.services.ProgramStopSubscriberService;
import io.cdap.cdap.internal.guice.AppFabricTestModule;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.metadata.MetadataService;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tephra.TransactionManager;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * This is helper class to make calls to AppFabricHttpHandler methods directly.
 * TODO: remove it, see CDAP-5
 */
public class AppFabricTestHelper {
  public static final TempFolder TEMP_FOLDER = new TempFolder();

  public static CConfiguration configuration;
  private static Injector injector;
  private static MetadataStorage metadataStorage;
  private static List<Service> services;

  public static Injector getInjector() {
    return getInjector(CConfiguration.create());
  }

  public static Injector getInjector(CConfiguration conf) {
    return getInjector(conf, new AbstractModule() {
      @Override
      protected void configure() {
        // no overrides
      }
    });
  }

  public static Injector getInjector(CConfiguration cConf, Module overrides) {
    return getInjector(cConf, null, overrides);
  }

  public static synchronized Injector getInjector(CConfiguration conf, @Nullable SConfiguration sConf) {
    return getInjector(conf, sConf, new AbstractModule() {
      @Override
      protected void configure() {
      }
    });
  }

  public static synchronized Injector getInjector(CConfiguration conf, @Nullable SConfiguration sConf,
                                                  Module overrides) {
    if (injector == null) {
      services = new ArrayList<>();

      configuration = conf;
      if (!CConfigurationUtil.isOverridden(configuration, Constants.CFG_LOCAL_DATA_DIR)) {
        configuration.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());
      }
      configuration.set(Constants.AppFabric.REST_PORT, Integer.toString(Networks.getRandomPort()));
      configuration.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      // Speed up tests
      configuration.setLong(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS, 100L);
      configuration.setLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS, 100L);

      injector = Guice.createInjector(Modules.override(new AppFabricTestModule(configuration, sConf)).with(overrides));

      startService(injector, MessagingService.class);
      startService(injector, TransactionManager.class);

      metadataStorage = injector.getInstance(MetadataStorage.class);
      try {
        metadataStorage.createIndex();
      } catch (IOException e) {
        throw new RuntimeException("Unable to create the metadata tables.", e);
      }

      startService(injector, MetadataService.class);

      // Register the tables before services will need to use them
      try {
        StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
      } catch (IOException e) {
        throw new RuntimeException("Failed to create the system tables", e);
      }

      startService(injector, DatasetOpExecutorService.class);
      startService(injector, DatasetService.class);
      startService(injector, MetricsCollectionService.class);
      startService(injector, MetadataSubscriberService.class);
      startService(injector, ProgramNotificationSubscriberService.class);
      startService(injector, ProgramStopSubscriberService.class);

      Scheduler programScheduler = startService(injector, Scheduler.class);

      // Wait for the scheduler to be functional.
      if (programScheduler instanceof CoreSchedulerService) {
        try {
          ((CoreSchedulerService) programScheduler).waitUntilFunctional(10, TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    return injector;
  }

  /**
   * This must be called by all tests that create their injector through this class.
   */
  public static void shutdown() {
    Closeables.closeQuietly(metadataStorage);

    if (services != null) {
      Lists.reverse(services).forEach(Service::stopAndWait);
    }

    InMemoryTableService.reset();

    metadataStorage = null;
    injector = null;
    services = null;
    TEMP_FOLDER.delete();
  }

  public static byte[] createSourceId(long sourceId) {
    byte[] buffer = new byte[MessageId.RAW_ID_SIZE];
    MessageId.putRawId(sourceId, (byte) 0, 0, (byte) 0, buffer, 0);
    return buffer;
  }

  /**
   * @return an instance of {@link LocalApplicationManager}
   */
  public static Manager<AppDeploymentInfo, ApplicationWithPrograms> getLocalManager() {
    return getLocalManager(CConfiguration.create());
  }

  /**
   * @return an instance of {@link LocalApplicationManager} created from the given configuration
   */
  public static Manager<AppDeploymentInfo, ApplicationWithPrograms> getLocalManager(CConfiguration cConf) {
    ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> factory =
      getInjector(cConf).getInstance(Key.get(
        new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() { }
      ));

    return factory.create(programId -> {
      //No-op
    });
  }

  public static void ensureNamespaceExists(NamespaceId namespaceId) throws Exception {
    ensureNamespaceExists(namespaceId, CConfiguration.create());
  }

  private static <T> T startService(Injector injector, Class<T> cls) {
    T instance = injector.getInstance(cls);
    if (instance instanceof Service) {
      services.add((Service) instance);
      ((Service) instance).startAndWait();
    }
    return instance;
  }

  private static void ensureNamespaceExists(NamespaceId namespaceId, CConfiguration cConf) throws Exception {
    NamespaceAdmin namespaceAdmin = getInjector(cConf).getInstance(NamespaceAdmin.class);
    try {
      if (!namespaceAdmin.exists(namespaceId)) {
        namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespaceId).build());
      }
    } catch (NamespaceAlreadyExistsException e) {
      // There can be race between exists() and create() call.
      if (!namespaceAdmin.exists(namespaceId)) {
        throw new IllegalStateException("Failed to create namespace " + namespaceId.getNamespace(), e);
      }
    }
  }

  public static void deployApplication(Id.Namespace namespace, Class<?> applicationClz,
                                       @Nullable String config, CConfiguration cConf) throws Exception {
    deployApplication(namespace, applicationClz, config, null, cConf);
  }

  public static void deployApplication(Id.Namespace namespace, Class<?> applicationClz,
                                       @Nullable String config, @Nullable KerberosPrincipalId ownerPrincipal,
                                       CConfiguration cConf) throws Exception {
    ensureNamespaceExists(namespace.toEntityId(), cConf);
    AppFabricClient appFabricClient = getInjector(cConf).getInstance(AppFabricClient.class);
    Location deployedJar = appFabricClient.deployApplication(namespace, applicationClz, config, ownerPrincipal);
    deployedJar.delete(true);
  }


  public static ApplicationWithPrograms deployApplicationWithManager(Class<?> appClass,
                                                                     Supplier<File> folderSupplier) throws Exception {
    return deployApplicationWithManager(Id.Namespace.DEFAULT, appClass, folderSupplier);
  }

  public static ApplicationWithPrograms deployApplicationWithManager(Class<?> appClass,
                                                                     Supplier<File> folderSupplier,
                                                                     Config config) throws Exception {
    return deployApplicationWithManager(Id.Namespace.DEFAULT, appClass, folderSupplier, config);
  }

  public static ApplicationWithPrograms deployApplicationWithManager(Id.Namespace namespace, Class<?> appClass,
                                                                     Supplier<File> folderSupplier) throws Exception {
    return deployApplicationWithManager(namespace, appClass, folderSupplier, null);
  }

  public static ApplicationWithPrograms deployApplicationWithManager(Id.Namespace namespace, Class<?> appClass,
                                                                     Supplier<File> folderSupplier,
                                                                     Config config) throws Exception  {
    NamespaceId namespaceId = namespace.toEntityId();
    ensureNamespaceExists(namespaceId);
    Location deployedJar = createAppJar(appClass, folderSupplier);
    ArtifactVersion artifactVersion = new ArtifactVersion(String.format("1.0.%d", System.currentTimeMillis()));
    ArtifactId artifactId = new ArtifactId(appClass.getSimpleName(), artifactVersion, ArtifactScope.USER);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    artifactRepository.addArtifact(Id.Artifact.fromEntityId(Artifacts.toProtoArtifactId(namespaceId, artifactId)),
                                   new File(deployedJar.toURI()));
    ApplicationClass applicationClass = new ApplicationClass(appClass.getName(), "", null);
    AppDeploymentInfo info = new AppDeploymentInfo(Artifacts.toProtoArtifactId(namespaceId, artifactId),
                                                   deployedJar, namespaceId,
                                                   applicationClass, null, null,
                                                   config == null ? null : new Gson().toJson(config));
    return getLocalManager().deploy(info).get();
  }

  /**
   * Submits a program execution.
   *
   * @param app the application containing the program
   * @param programClassName name of the program class
   * @param userArgs runtime arguments
   * @param folderSupplier a Supplier of temporary folder
   * @return a {@link ProgramController} for controlling the program execution.
   */
  public static ProgramController submit(ApplicationWithPrograms app, String programClassName,
                                         Arguments userArgs, Supplier<File> folderSupplier) throws Exception {
    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    ProgramRunner runner = null;
    Program program = null;
    for (ProgramDescriptor programDescriptor : app.getPrograms()) {
      if (programDescriptor.getSpecification().getClassName().equals(programClassName)) {
        runner = runnerFactory.create(programDescriptor.getProgramId().getType());
        program = createProgram(programDescriptor, app.getArtifactLocation(), runner, folderSupplier);
        break;
      }
    }

    Assert.assertNotNull(program);

    BasicArguments systemArgs = new BasicArguments(ImmutableMap.of(
      ProgramOptionConstants.RUN_ID, RunIds.generate().getId(),
      ProgramOptionConstants.HOST, InetAddress.getLoopbackAddress().getCanonicalHostName(),
      ProgramOptionConstants.ARTIFACT_ID, Joiner.on(":").join(app.getArtifactId().toIdParts())
    ));

    return runner.run(program, new SimpleProgramOptions(program.getId(), systemArgs, userArgs));
  }

  /**
   * Creates a {@link Program}.
   *
   * @param programDescriptor contains information about the program to be created
   * @param artifactLocation location of the artifact jar
   * @param programRunner an optional program runner that will be used to run the program
   * @param folderSupplier a {@link Supplier} to provide a folder that the artifact jar will be expanded to.
   */
  private static Program createProgram(ProgramDescriptor programDescriptor,
                                       Location artifactLocation,
                                       ProgramRunner programRunner,
                                       Supplier<File> folderSupplier) throws Exception {
    CConfiguration cConf = getInjector().getInstance(CConfiguration.class);
    return Programs.create(cConf, programRunner, programDescriptor, artifactLocation,
                           BundleJarUtil.prepareClassLoaderFolder(artifactLocation, folderSupplier::get).getDir());

  }


  private static Location createAppJar(Class<?> appClass, Supplier<File> folderSupplier) throws IOException {
    LocationFactory lf = new LocalLocationFactory(DirUtils.createTempDir(folderSupplier.get()));
    return AppJarHelper.createDeploymentJar(lf, appClass);
  }

  /**
   * Enables in-memory authorization in the provided configuration. Default user is given superuser rights,
   * so that you can use it to grant permissions to others. While in
   * {@link io.cdap.cdap.internal.app.services.http.AppFabricTestBase} use
   * {@link io.cdap.cdap.security.spi.authentication.SecurityRequestContext#setUserId(String)} or
   * {@link io.cdap.cdap.internal.app.services.http.AppFabricTestBase#doAs(String, Retries.Runnable)}
   * to set user
   * for the call.
   */
  public static CConfiguration enableAuthorization(CConfiguration cConf, TemporaryFolder temporaryFolder)
    throws IOException {
    enableAuthorization(cConf::set, temporaryFolder);
    return cConf;
  }

  /**
   * More generic method of {@link #enableAuthorization(CConfiguration, TemporaryFolder)}. Allows to set
   * configuration values that enable security on any set-like method, e.g. in CConfiguration or Map.
   */
  public static void enableAuthorization(BiConsumer<String, String> confSetter, TemporaryFolder temporaryFolder)
    throws IOException {
    confSetter.accept(Constants.Security.ENABLED, Boolean.toString(true));
    confSetter.accept(Constants.Security.KERBEROS_ENABLED, Boolean.toString(false));
    confSetter.accept(Constants.Security.Authorization.ENABLED, Boolean.toString(true));
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, InMemoryAccessController.class.getName());
    LocationFactory locationFactory = new LocalLocationFactory(temporaryFolder.newFolder());
    Location externalAuthJar = AppJarHelper.createDeploymentJar(
      locationFactory, InMemoryAccessController.class, manifest);
    confSetter.accept(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
    confSetter.accept(Constants.Security.Authorization.EXTENSION_CONFIG_PREFIX + "superusers",
              UserGroupInformation.getCurrentUser().getShortUserName());
  }

}

