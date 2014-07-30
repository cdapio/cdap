/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.test.internal;

import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.lang.jar.JarFinder;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.gateway.handlers.AppFabricHttpHandler;
import com.continuuity.internal.app.deploy.ProgramTerminator;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.schedule.SchedulerService;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.proto.Id;
import com.continuuity.proto.ProgramType;
import com.continuuity.tephra.inmemory.InMemoryTransactionManager;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This is helper class to make calls to AppFabricHttpHandler methods directly.
 * TODO: remove it, see REACTOR-676
 */
public class AppFabricTestHelper {
  public static final TempFolder TEMP_FOLDER = new TempFolder();
  public static CConfiguration configuration;
  private static Injector injector;

  public static Injector getInjector() {
    return getInjector(CConfiguration.create());
  }

  public static synchronized Injector getInjector(CConfiguration conf) {
    if (injector == null) {
      configuration = conf;
      configuration.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());
      configuration.set(Constants.AppFabric.REST_PORT, Integer.toString(Networks.getRandomPort()));
      configuration.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      injector = Guice.createInjector(new AppFabricTestModule(configuration));
      injector.getInstance(InMemoryTransactionManager.class).startAndWait();
      injector.getInstance(DatasetService.class).startAndWait();
      injector.getInstance(SchedulerService.class).startAndWait();

      LogAppenderInitializer logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      logAppenderInitializer.initialize();
    }
    return injector;
  }

  /**
   * @return Returns an instance of {@link com.continuuity.internal.app.deploy.LocalManager}
   */
  public static Manager<Location, ApplicationWithPrograms> getLocalManager() {
    ManagerFactory<Location, ApplicationWithPrograms> factory =
      getInjector().getInstance(Key.get(new TypeLiteral<ManagerFactory<Location, ApplicationWithPrograms>>() {
      }));

    return factory.create(new ProgramTerminator() {
      @Override
      public void stop(Id.Account id, Id.Program programId, ProgramType type) throws Exception {
        //No-op
      }
    });
  }

  public static void deployApplication(Class<?> application) throws Exception {
    deployApplication(application,
                      "app-" + TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS) + ".jar");
  }

  /**
   *
   */
  public static void deployApplication(Class<?> applicationClz, String fileName) throws Exception {
    AppFabricClient appFabricClient = new AppFabricClient(getInjector().getInstance(AppFabricHttpHandler.class),
                                                          getInjector().getInstance(LocationFactory.class));
    Location deployedJar = appFabricClient.deployApplication(fileName, applicationClz);
    deployedJar.delete(true);
  }

  public static ApplicationWithPrograms deployApplicationWithManager(Class<?> appClass,
                                                                     final Supplier<File> folderSupplier)
    throws Exception {

    Location deployedJar = createAppJar(appClass);
    try {
      ApplicationWithPrograms appWithPrograms = getLocalManager().deploy(DefaultId.ACCOUNT, null, deployedJar).get();
      // Transform program to get loadable, as the one created in deploy pipeline is not loadable.

      List<Program> programs = ImmutableList.copyOf(Iterables.transform(appWithPrograms.getPrograms(),
                                                                        new Function<Program, Program>() {
            @Override
            public Program apply(Program program) {
              try {
                return Programs.createWithUnpack(program.getJarLocation(), folderSupplier.get());
              } catch (IOException e) {
                throw Throwables.propagate(e);
              }
            }
          }
      ));
      return new ApplicationWithPrograms(appWithPrograms.getAppSpecLoc(), programs);
    } finally {
      deployedJar.delete(true);
    }
  }

  public static Location createAppJar(Class<?> appClass) {
    LocalLocationFactory lf = new LocalLocationFactory();
    return lf.create(JarFinder.getJar(appClass, AppFabricClient.getManifestWithMainClass(appClass)));
  }

}

