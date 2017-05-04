/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSortOrder;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * Artifact store read only
 */
public class ReadOnlyArtifactRepository {
  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyArtifactRepository.class);

  private final ArtifactStore artifactStore;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  public ReadOnlyArtifactRepository(ArtifactRepository artifactRepository) {
    this.artifactStore = artifactRepository.getArtifactStore();
    this.authorizationEnforcer = artifactRepository.getAuthorizationEnforcer();
    this.authenticationContext = artifactRepository.getAuthenticationContext();
  }

  @VisibleForTesting
  @Inject
  public ReadOnlyArtifactRepository(ArtifactStore artifactStore, AuthorizationEnforcer authorizationEnforcer,
                                    AuthenticationContext authenticationContext) {
    this.artifactStore = artifactStore;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  /**
   * Get details about the given artifact. Will never return null.
   * If no such artifact exist, an exception is thrown. Namespace existence is not checked.
   *
   * @param artifactId the id of the artifact to get
   * @return details about the given artifact
   * @throws IOException if there as an exception reading from the meta store
   * @throws ArtifactNotFoundException if the given artifact does not exist
   */
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    ensureAccess(artifactId.toEntityId());
    return artifactStore.getArtifact(artifactId);
  }

  /**
   * Ensures that the logged-in user has a {@link Action privilege} on the specified dataset instance.
   *
   * @param artifactId the {@link co.cask.cdap.proto.id.ArtifactId} to check for privileges
   * @throws UnauthorizedException if the logged in user has no {@link Action privileges} on the specified dataset
   */
  private void ensureAccess(co.cask.cdap.proto.id.ArtifactId artifactId) throws Exception {
    // No authorization for system artifacts
    if (NamespaceId.SYSTEM.equals(artifactId.getParent())) {
      return;
    }
    Principal principal = authenticationContext.getPrincipal();
    Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    if (!filter.apply(artifactId)) {
      throw new UnauthorizedException(principal, artifactId);
    }
  }

  /**
   * Returns a {@link Map.Entry} representing the plugin information for the plugin being requested.
   *
   * @param namespace the namespace to get plugins from
   * @param artifactRange the artifact range to get plugins for
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param selector for selecting which plugin to use
   * @return the entry found
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws PluginNotExistsException if no plugins of the given type and name are available to the given artifact
   */
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(NamespaceId namespace, ArtifactRange artifactRange,
                                                               String pluginType, String pluginName,
                                                               PluginSelector selector)
    throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    SortedMap<ArtifactDescriptor, PluginClass> pluginClasses = artifactStore.getPluginClasses(
      namespace, artifactRange, pluginType, pluginName, null, Integer.MAX_VALUE, ArtifactSortOrder.UNORDERED);
    return getPluginEntries(pluginClasses, selector, artifactRange.getNamespace().toId(), pluginType, pluginName);
  }

  private Map.Entry<ArtifactDescriptor, PluginClass> getPluginEntries(
    SortedMap<ArtifactDescriptor, PluginClass> pluginClasses, PluginSelector selector, Id.Namespace namespace,
    String pluginType, String pluginName) throws PluginNotExistsException {
    SortedMap<ArtifactId, PluginClass> artifactIds = Maps.newTreeMap();
    for (Map.Entry<ArtifactDescriptor, PluginClass> pluginClassEntry : pluginClasses.entrySet()) {
      artifactIds.put(pluginClassEntry.getKey().getArtifactId(), pluginClassEntry.getValue());
    }
    Map.Entry<ArtifactId, PluginClass> chosenArtifact = selector.select(artifactIds);
    if (chosenArtifact == null) {
      throw new PluginNotExistsException(namespace, pluginType, pluginName);
    }

    for (Map.Entry<ArtifactDescriptor, PluginClass> pluginClassEntry : pluginClasses.entrySet()) {
      if (pluginClassEntry.getKey().getArtifactId().compareTo(chosenArtifact.getKey()) == 0) {
        return pluginClassEntry;
      }
    }
    throw new PluginNotExistsException(namespace, pluginType, pluginName);
  }

  public Set<ArtifactRange> getArtifactParentsForPlugin(String namespaceId, String pluginType, String pluginName) {
    return artifactStore.getArtifactParentsForPlugin(namespaceId, pluginType, pluginName);
  }
}
