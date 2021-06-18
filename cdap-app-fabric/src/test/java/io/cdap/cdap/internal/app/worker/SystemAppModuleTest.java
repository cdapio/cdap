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

package io.cdap.cdap.internal.app.worker;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactManager;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Test;

/**
 * Test for SystemAppModule
 */
public class SystemAppModuleTest {

  @Test
  public void test() {
    Injector injector = Guice.createInjector(
      new ZKClientModule(),
      new ZKDiscoveryModule(),
      new ConfigModule(new Configuration()),
      new MessagingClientModule(),
      new FactoryModuleBuilder()
        .implement(ArtifactManager.class, RemoteArtifactManager.class)
        .build(ArtifactManagerFactory.class),
      new LocalLocationModule(),
      new SecureStoreClientModule(),
      new SystemAppModule());
    injector.getInstance(ArtifactRepositoryReader.class);
    injector.getInstance(ArtifactRepository.class);
    injector.getInstance(Impersonator.class);
    injector.getInstance(PreferencesFetcher.class);
    injector.getInstance(PluginFinder.class);
    injector.getInstance(DiscoveryServiceClient.class);
    injector.getInstance(SecureStore.class);
    ArtifactManagerFactory instance = injector.getInstance(ArtifactManagerFactory.class);
    instance.create(NamespaceId.DEFAULT, RetryStrategies.noRetry());
  }
}
