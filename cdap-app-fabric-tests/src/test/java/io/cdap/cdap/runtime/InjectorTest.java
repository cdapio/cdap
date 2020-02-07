/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class InjectorTest {

  @Test
  public void test() {
    Injector injector = Guice.createInjector(createmodule());
  }

  private List<Module> createmodule() {
    List<Module> modules = new ArrayList<>();
    modules.add(new ConfigModule(CConfiguration.create()));
//    modules.add(new MessagingClientModule());
    modules.add(new AuthenticationContextModules().getProgramContainerModule());
    modules.add(new AbstractModule() {

      @Override
      protected void configure() {
        bind(ClusterMode.class).toInstance(ClusterMode.ISOLATED);
        // TwillRunner used by the ProgramRunner is the remote execution one
        YarnConfiguration conf = new YarnConfiguration();
        LocationFactory locationFactory = new FileContextLocationFactory(conf);
        bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class)
          .toInstance(new YarnTwillRunnerService(conf, "localhost:2181", locationFactory));

      //  bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class).in(Scopes.SINGLETON);
        bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

        // A private Map binding of ProgramRunner for ProgramRunnerFactory to use
        MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder = MapBinder.newMapBinder(
          binder(), ProgramType.class, ProgramRunner.class);

        defaultProgramRunnerBinder.addBinding(ProgramType.WORKER).to(DistributedWorkerProgramRunner.class);
      }
    });

    return modules;
  }
}
