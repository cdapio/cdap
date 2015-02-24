/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data.runtime.main;

import co.cask.cdap.authorization.ACLManagerService;
import co.cask.cdap.authorization.AuthorizationModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.twill.AbstractMasterTwillRunnable;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;

import java.util.List;

/**
 * Twill runnable for running ACL manager http service.
 */
public class ACLManagerRunnable extends AbstractMasterTwillRunnable {

  private Injector injector;

  public ACLManagerRunnable(String name, String cConfName, String hConfName) {
    super(name, cConfName, hConfName);
  }

  @Override
  protected void doInit(TwillContext context) {
    try {
      CConfiguration cConf = getCConfiguration();
      Configuration hConf = getConfiguration();

      // Set the host name to the one provided by Twill
      cConf.set(Constants.Stream.ADDRESS, context.getHost().getHostName());
      // Set the worker threads to number of cores * 2 available
      cConf.setInt(Constants.Stream.WORKER_THREADS, Runtime.getRuntime().availableProcessors() * 2);
      // Set the instance id
      cConf.setInt(Constants.Stream.CONTAINER_INSTANCE_ID, context.getInstanceId());

      injector = Guice.createInjector(
        new ConfigModule(cConf, hConf),
        new IOModule(),
        new ZKClientModule(),
        new DiscoveryRuntimeModule().getDistributedModules(),
        new LocationRuntimeModule().getDistributedModules(),
        new MetricsClientRuntimeModule().getDistributedModules(),
        new DataSetsModules().getDistributedModule(),
        new AuthorizationModule().getDistributedModule()
      );

      injector.getInstance(LogAppenderInitializer.class).initialize();
      LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                         Constants.Logging.COMPONENT_NAME,
                                                                         Constants.Service.ACL_MANAGER));

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void getServices(List<? super Service> services) {
    services.add(injector.getInstance(ACLManagerService.class));
  }
}
