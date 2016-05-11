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

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
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
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.tephra.TransactionManager;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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

import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This is helper class to make calls to AppFabricHttpHandler methods directly.
 * TODO: remove it, see CDAP-5
 *
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
                                                                     final Supplier<File> folderSupplier)
    throws Exception {
    return deployApplicationWithManager(Id.Namespace.DEFAULT, appClass, folderSupplier);
  }

  public static ApplicationWithPrograms deployApplicationWithManager(Id.Namespace namespace, Class<?> appClass,
                                                                     final Supplier<File> folderSupplier)
    throws Exception {
    ensureNamespaceExists(namespace);
    Location deployedJar = createAppJar(appClass);
    Id.Artifact artifactId = Id.Artifact.from(namespace, appClass.getSimpleName(),
                                              String.format("1.0.%d", System.currentTimeMillis()));
    AppDeploymentInfo info = new AppDeploymentInfo(artifactId, appClass.getName(), deployedJar, null);
    ApplicationWithPrograms appWithPrograms = getLocalManager().deploy(namespace, null, info).get();
    // Transform program to get loadable, as the one created in deploy pipeline is not loadable.

    final List<Program> programs = ImmutableList.copyOf(Iterables.transform(
      appWithPrograms.getPrograms(),
      new Function<Program, Program>() {
        @Override
        public Program apply(Program program) {
          try {
            return Programs.createWithUnpack(configuration, program.getJarLocation(), folderSupplier.get());
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
      }
    ));
    return new ApplicationWithPrograms(appWithPrograms) {
      @Override
      public Iterable<Program> getPrograms() {
        return programs;
      }
    };
  }

  private static Location createAppJar(Class<?> appClass) throws IOException {
    LocationFactory lf = new LocalLocationFactory(DirUtils.createTempDir(TEMP_FOLDER.getRoot()));
    return AppJarHelper.createDeploymentJar(lf, appClass);
  }
}

