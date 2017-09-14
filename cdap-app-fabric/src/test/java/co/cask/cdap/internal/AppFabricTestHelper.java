/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.internal.app.deploy.LocalApplicationManager;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.internal.app.services.ProgramNotificationSubscriberService;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.scheduler.Scheduler;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionManager;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import javax.annotation.Nullable;

/**
 * This is helper class to make calls to AppFabricHttpHandler methods directly.
 * TODO: remove it, see CDAP-5
 */
public class AppFabricTestHelper {
  public static final TempFolder TEMP_FOLDER = new TempFolder();

  public static CConfiguration configuration;
  private static Injector injector;

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

  public static synchronized Injector getInjector(CConfiguration conf, @Nullable SConfiguration sConf,
                                                  Module overrides) {
    if (injector == null) {
      configuration = conf;
      configuration.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());
      configuration.set(Constants.AppFabric.REST_PORT, Integer.toString(Networks.getRandomPort()));
      configuration.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      // Speed up tests
      configuration.setLong(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS, 100L);
      configuration.setLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS, 100L);

      injector = Guice.createInjector(Modules.override(new AppFabricTestModule(configuration, sConf)).with(overrides));

      MessagingService messagingService = injector.getInstance(MessagingService.class);
      if (messagingService instanceof Service) {
        ((Service) messagingService).startAndWait();
      }
      injector.getInstance(TransactionManager.class).startAndWait();
      injector.getInstance(DatasetOpExecutor.class).startAndWait();
      injector.getInstance(DatasetService.class).startAndWait();
      injector.getInstance(StreamCoordinatorClient.class).startAndWait();
      injector.getInstance(NotificationService.class).startAndWait();
      injector.getInstance(MetricsCollectionService.class).startAndWait();
      injector.getInstance(ProgramNotificationSubscriberService.class).startAndWait();
      Scheduler programScheduler = injector.getInstance(Scheduler.class);
      if (programScheduler instanceof Service) {
        ((Service) programScheduler).startAndWait();
      }
    }
    return injector;
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
    ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> factory =
      getInjector().getInstance(Key.get(
        new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() { }
      ));

    return factory.create(new ProgramTerminator() {
      @Override
      public void stop(ProgramId programId) throws Exception {
        //No-op
      }
    });
  }

  public static void ensureNamespaceExists(NamespaceId namespaceId) throws Exception {
    ensureNamespaceExists(namespaceId, CConfiguration.create());
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
    ensureNamespaceExists(namespace.toEntityId());
    Location deployedJar = createAppJar(appClass, folderSupplier);
    ArtifactVersion artifactVersion = new ArtifactVersion(String.format("1.0.%d", System.currentTimeMillis()));
    ArtifactId artifactId = new ArtifactId(appClass.getSimpleName(), artifactVersion, ArtifactScope.USER);
    ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(artifactId, deployedJar);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    artifactRepository.addArtifact(Artifacts.toArtifactId(namespace.toEntityId(), artifactId).toId(),
                                   new File(deployedJar.toURI()));

    AppDeploymentInfo info = new AppDeploymentInfo(artifactDescriptor, namespace.toEntityId(),
                                                   appClass.getName(), null, null,
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
      ProgramOptionConstants.HOST, InetAddress.getLoopbackAddress().getCanonicalHostName()
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
                           BundleJarUtil.unJar(artifactLocation, folderSupplier.get()));

  }


  private static Location createAppJar(Class<?> appClass, Supplier<File> folderSupplier) throws IOException {
    LocationFactory lf = new LocalLocationFactory(DirUtils.createTempDir(folderSupplier.get()));
    return AppJarHelper.createDeploymentJar(lf, appClass);
  }
}

