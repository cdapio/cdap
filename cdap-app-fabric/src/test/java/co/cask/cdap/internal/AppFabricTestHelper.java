/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.namespace.NamespaceAdmin;
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
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRangeCodec;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.tephra.TransactionManager;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;
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
 *
 */
public class AppFabricTestHelper {
  public static final TempFolder TEMP_FOLDER = new TempFolder();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ArtifactRange.class, new ArtifactRangeCodec())
    .create();

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

  public static synchronized Injector getInjector(CConfiguration conf, Module overrides) {
    if (injector == null) {
      configuration = conf;
      configuration.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());
      configuration.set(Constants.AppFabric.REST_PORT, Integer.toString(Networks.getRandomPort()));
      configuration.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      injector = Guice.createInjector(Modules.override(new AppFabricTestModule(configuration)).with(overrides));
      injector.getInstance(TransactionManager.class).startAndWait();
      injector.getInstance(DatasetOpExecutor.class).startAndWait();
      injector.getInstance(DatasetService.class).startAndWait();
      injector.getInstance(SchedulerService.class).startAndWait();
      injector.getInstance(StreamCoordinatorClient.class).startAndWait();
      injector.getInstance(NotificationService.class).startAndWait();
      injector.getInstance(MetricsCollectionService.class).startAndWait();
    }
    return injector;
  }

  /**
   * @return Returns an instance of {@link LocalApplicationManager}
   */
  public static Manager<AppDeploymentInfo, ApplicationWithPrograms> getLocalManager() {
    ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> factory =
      getInjector().getInstance(Key.get(new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() {
      }));

    return factory.create(new ProgramTerminator() {
      @Override
      public void stop(Id.Program programId) throws Exception {
        //No-op
      }
    });
  }

  public static void ensureNamespaceExists(Id.Namespace namespace) throws Exception {
    ensureNamespaceExists(namespace, CConfiguration.create());
  }

  public static void ensureNamespaceExists(Id.Namespace namespace, CConfiguration cConf) throws Exception {
    NamespaceAdmin namespaceAdmin = getInjector(cConf).getInstance(NamespaceAdmin.class);
    if (!namespaceAdmin.exists(namespace)) {
      namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace).build());
    }
  }

  public static void deployApplication(Id.Namespace namespace, Class<?> applicationClz,
                                       @Nullable String config, CConfiguration cConf) throws Exception {
    ensureNamespaceExists(namespace, cConf);
    AppFabricClient appFabricClient = getInjector(cConf).getInstance(AppFabricClient.class);
    Location deployedJar = appFabricClient.deployApplication(namespace, applicationClz, config);
    deployedJar.delete(true);
  }


  public static ApplicationWithPrograms deployApplicationWithManager(Class<?> appClass,
                                                                     Supplier<File> folderSupplier) throws Exception {
    return deployApplicationWithManager(Id.Namespace.DEFAULT, appClass, folderSupplier);
  }

  public static ApplicationWithPrograms deployApplicationWithManager(Id.Namespace namespace, Class<?> appClass,
                                                                     Supplier<File> folderSupplier) throws Exception {
    ensureNamespaceExists(namespace);
    Location deployedJar = createAppJar(appClass, folderSupplier);
    ArtifactVersion artifactVersion = new ArtifactVersion(String.format("1.0.%d", System.currentTimeMillis()));
    ArtifactId artifactId = new ArtifactId(appClass.getSimpleName(), artifactVersion, ArtifactScope.USER);
    ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(artifactId, deployedJar);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    artifactRepository.addArtifact(Artifacts.toArtifactId(namespace.toEntityId(), artifactId).toId(),
                                   new File(deployedJar.toURI()));

    AppDeploymentInfo info = new AppDeploymentInfo(artifactDescriptor, namespace.toEntityId(),
                                                   appClass.getName(), null, null);
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

    co.cask.cdap.proto.id.ArtifactId artifactId = app.getArtifactId();
    ArtifactRepository artifactRepository = injector.getInstance(ArtifactRepository.class);
    BasicArguments systemArgs = new BasicArguments(ImmutableMap.of(
      ProgramOptionConstants.RUN_ID, RunIds.generate().getId(),
      ProgramOptionConstants.ARTIFACT_ID, GSON.toJson(artifactId),
      ProgramOptionConstants.ARTIFACT_META, GSON.toJson(artifactRepository.getArtifact(artifactId.toId()).getMeta()),
      ProgramOptionConstants.HOST, InetAddress.getLoopbackAddress().getCanonicalHostName()
    ));

    return runner.run(program, new SimpleProgramOptions(program.getName(), systemArgs, userArgs));
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

