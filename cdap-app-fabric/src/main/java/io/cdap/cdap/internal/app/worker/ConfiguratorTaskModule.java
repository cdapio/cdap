/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.worker;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.common.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;

/**
 * ConfiguratorTaskModule specifies the binding for a {@link io.cdap.cdap.internal.app.worker.ConfiguratorTask}
 */
public class ConfiguratorTaskModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(PluginFinder.class).to(RemoteWorkerPluginFinder.class);
    bind(UGIProvider.class).to(CurrentUGIProvider.class);
    bind(ArtifactRepositoryReader.class).to(RemoteArtifactRepositoryReader.class)
        .in(Scopes.SINGLETON);
    bind(ArtifactRepository.class).to(RemoteArtifactRepository.class);
  }
}

