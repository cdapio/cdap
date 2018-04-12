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

package co.cask.cdap.app.guice;

import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import co.cask.cdap.internal.app.runtime.artifact.PluginFinder;
import co.cask.cdap.internal.app.runtime.artifact.RemoteArtifactManager;
import co.cask.cdap.internal.app.runtime.artifact.RemotePluginFinder;
import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

/**
 * Guice module for bindings for artifacts and plugins supports for program API.
 */
public class DistributedArtifactManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    // Bind the PluginFinder implementation
    bind(PluginFinder.class).to(RemotePluginFinder.class);

    // Bind the ArtifactManager implementation
    install(new FactoryModuleBuilder()
              .implement(ArtifactManager.class, RemoteArtifactManager.class)
              .build(ArtifactManagerFactory.class));
  }
}
