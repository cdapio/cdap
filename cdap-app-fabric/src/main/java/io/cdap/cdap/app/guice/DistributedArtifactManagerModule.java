/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.util.Providers;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactManager;
import io.cdap.cdap.internal.app.runtime.artifact.RemotePluginFinder;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;

/**
 * Guice module for bindings for artifacts and plugins supports for program API.
 */
public class DistributedArtifactManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    // Bind the PluginFinder implementation
    bind(PluginFinder.class).to(RemotePluginFinder.class);
    // ArtifactLocalizerClient is only used for task worker
    OptionalBinder.newOptionalBinder(binder(), ArtifactLocalizerClient.class)
      .setBinding().toProvider(Providers.of(null));

    // Bind the ArtifactManager implementation
    install(new FactoryModuleBuilder()
              .implement(ArtifactManager.class, RemoteArtifactManager.class)
              .build(ArtifactManagerFactory.class));
  }
}
