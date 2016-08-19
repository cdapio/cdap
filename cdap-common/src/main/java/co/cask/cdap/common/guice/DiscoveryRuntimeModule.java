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
package co.cask.cdap.common.guice;

import co.cask.cdap.common.runtime.RuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.ZKClient;

/**
 * Provides Guice bindings for DiscoveryService and DiscoveryServiceClient for different
 * runtime environments.
 */
public final class DiscoveryRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new InMemoryDiscoveryModule();
  }

  @Override
  public Module getStandaloneModules() {
    return new InMemoryDiscoveryModule();
  }

  @Override
  public Module getDistributedModules() {
    return new ZKDiscoveryModule();
  }

  private static final class InMemoryDiscoveryModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(InMemoryDiscoveryService.class).in(Singleton.class);
      bind(DiscoveryService.class).to(InMemoryDiscoveryService.class);
      bind(DiscoveryServiceClient.class).to(InMemoryDiscoveryService.class);
    }
  }

  private static final class ZKDiscoveryModule extends PrivateModule {

    @Override
    protected void configure() {
      bind(DiscoveryService.class).to(ZKDiscoveryService.class);
      bind(DiscoveryServiceClient.class)
        .annotatedWith(Names.named("local.discovery.client"))
        .to(ZKDiscoveryService.class);
      bind(DiscoveryServiceClient.class).to(ProgramDiscoveryServiceClient.class);

      expose(DiscoveryService.class);
      expose(DiscoveryServiceClient.class);
    }

    @Provides
    @Singleton
    private ZKDiscoveryService providesDiscoveryService(ZKClient zkClient) {
      return new ZKDiscoveryService(zkClient);
    }
  }
}
