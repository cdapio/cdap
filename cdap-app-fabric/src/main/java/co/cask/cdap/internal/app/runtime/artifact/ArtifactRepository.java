/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * This class manages artifact and artifact metadata. It is mainly responsible for inspecting artifacts to determine
 * metadata for the artifact.
 */
public class ArtifactRepository {
  private final ArtifactStore artifactStore;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final ArtifactInspector artifactInspector;

  @Inject
  ArtifactRepository(CConfiguration cConf, ArtifactStore artifactStore) {
    this.artifactStore = artifactStore;
    File baseUnpackDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.artifactClassLoaderFactory = new ArtifactClassLoaderFactory(baseUnpackDir);
    this.artifactInspector = new ArtifactInspector(cConf, artifactClassLoaderFactory);
  }

  @VisibleForTesting
  void clear(Id.Namespace namespace) throws IOException {
    artifactStore.clear(namespace);
  }

  /**
   * Returns a {@link SortedMap} of all plugins available for the given artifact. The keys
   * are sorted by the {@link ArtifactDescriptor} for the artifact that contains plugins available to the given
   * artifact. Only unique {@link PluginClass} are returned in the value
   * Collection, where uniqueness is determined by the plugin class type and name only.
   *
   * @param artifactId the id of the artifact to get plugins for
   * @return an unmodifiable sorted map from plugin artifact to plugins in that artifact
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   */
  public SortedMap<ArtifactDescriptor, List<PluginClass>> getPlugins(Id.Artifact artifactId) throws IOException {
    return artifactStore.getPluginClasses(artifactId);
  }

  /**
   * Returns a {@link Map.Entry} representing the plugin information for the plugin being requested.
   *
   * @param artifactId the id of the artifact to get plugins for
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param selector for selecting which plugin to use
   * @return the entry found or {@code null} if none was found
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   * @throws PluginNotExistsException if no plugins of the given type and name are available to the given artifact
   */
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(Id.Artifact artifactId, final String pluginType,
                                                               final String pluginName, PluginSelector selector)
    throws IOException, PluginNotExistsException {
    return selector.select(artifactStore.getPluginClasses(artifactId, pluginType, pluginName));
  }

  /**
   * Inspects and builds plugin information for the given artifact.
   *
   * @param artifactId the id of the artifact to inspect and store
   * @param artifactFile the artifact to inspect and store
   * @param parentArtifacts artifacts the given artifact extends.
   *                        If null, the given artifact does not extend another artifact
   * @throws IOException if there was an exception reading from the artifact store
   * @throws ArtifactRangeNotFoundException if none of the parent artifacts could be found
   */
  public void addArtifact(Id.Artifact artifactId, File artifactFile, @Nullable Set<ArtifactRange> parentArtifacts)
    throws IOException, ArtifactRangeNotFoundException, WriteConflictException,
    ArtifactAlreadyExistsException, InvalidArtifactException {

    CloseableClassLoader parentClassLoader;
    parentArtifacts = parentArtifacts == null ? ImmutableSet.<ArtifactRange>of() : parentArtifacts;
    if (parentArtifacts.isEmpty()) {
      // if this artifact doesn't extend another, use itself to create the parent classloader
      parentClassLoader = artifactClassLoaderFactory.createClassLoader(Locations.toLocation(artifactFile));
    } else {
      // otherwise, use any of the parent artifacts to create the parent classloader.
      Location parentLocation = null;
      for (ArtifactRange parentRange : parentArtifacts) {
        List<ArtifactDetail> parents = artifactStore.getArtifacts(parentRange);
        if (!parents.isEmpty()) {
          parentLocation = parents.get(0).getDescriptor().getLocation();
        }
      }
      if (parentLocation == null) {
        throw new ArtifactRangeNotFoundException(parentArtifacts);
      }
      parentClassLoader = artifactClassLoaderFactory.createClassLoader(parentLocation);
    }

    try {
      ArtifactClasses artifactClasses = artifactInspector.inspectArtifact(artifactId, artifactFile, parentClassLoader);
      ArtifactMeta meta = new ArtifactMeta(artifactClasses, parentArtifacts);
      artifactStore.write(artifactId, meta, new FileInputStream(artifactFile));
    } finally {
      parentClassLoader.close();
    }
  }
}
