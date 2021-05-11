/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.dispatcher;

import com.google.inject.AbstractModule;
import io.cdap.cdap.api.task.TaskPluginContext;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactFinder;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemotePluginFinder;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.security.auth.context.MasterAuthenticationContext;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;

/**
 * PluginTaskModule
 */
public class PluginTaskModule extends AbstractModule {

  @Override
  protected void configure() {
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
    if (masterEnv != null) {
      bind(DiscoveryService.class)
        .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
      bind(DiscoveryServiceClient.class)
        .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
      bind(AuthenticationContext.class).to(MasterAuthenticationContext.class);
      bind(LocationFactory.class).to(LocalLocationFactory.class);
      bind(PluginFinder.class).to(RemotePluginFinder.class);
      bind(ArtifactFinder.class).to(RemotePluginFinder.class);
      bind(TaskPluginContext.class).to(DefaultTaskPluginContext.class);
    }
  }

}
