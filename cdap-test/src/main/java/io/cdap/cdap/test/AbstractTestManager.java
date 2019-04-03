/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.test;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Class that delegates deprecated methods to their non-deprecated counterparts, and delegates methods with
 * fewer arguments to the one with more arguments.
 */
public abstract class AbstractTestManager implements TestManager {

  @Override
  public ApplicationManager deployApplication(NamespaceId namespace, Class<? extends Application> applicationClz,
                                              File... bundleEmbeddedJars) {
    return deployApplication(namespace, applicationClz, null, bundleEmbeddedJars);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent, Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    return addPluginArtifact(artifactId, toRange(parent), pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
                                           Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    return addPluginArtifact(artifactId, parents, null, pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                           @Nullable Set<PluginClass> additionalPlugins,
                                           Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    return addPluginArtifact(artifactId, toRange(parent), additionalPlugins, pluginClass, pluginClasses);
  }

  @Override
  public <T extends DatasetAdmin> T addDatasetInstance(String datasetType, DatasetId datasetId) throws Exception {
    return addDatasetInstance(datasetType, datasetId, DatasetProperties.EMPTY);
  }

  private Set<ArtifactRange> toRange(ArtifactId parent) {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(parent.getParent().getNamespace(), parent.getArtifact(),
                                  new ArtifactVersion(parent.getVersion()), true,
                                  new ArtifactVersion(parent.getVersion()), true));
    return parents;
  }
}
