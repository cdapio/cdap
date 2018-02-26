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

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.proto.artifact.ApplicationClassInfo;
import co.cask.cdap.proto.artifact.ApplicationClassSummary;
import co.cask.cdap.proto.artifact.ArtifactSortOrder;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.impersonation.EntityImpersonator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * /**
 * A class which has a delegation {@link ArtifactRepository} which has authorization enforce on the methods.
 */
public class AuthorizationArtifactRepository implements ArtifactRepository {
  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationArtifactRepository.class);

  private final ArtifactRepository delegate;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  public AuthorizationArtifactRepository(@Named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO)
                                           ArtifactRepository artifactRepository,
                                         AuthorizationEnforcer authorizationEnforcer,
                                         AuthenticationContext authenticationContext) {
    this.delegate = artifactRepository;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @Override
  public CloseableClassLoader createArtifactClassLoader(Location artifactLocation,
                                                        EntityImpersonator entityImpersonator) throws IOException {
    return delegate.createArtifactClassLoader(artifactLocation, entityImpersonator);
  }

  @Override
  public void clear(NamespaceId namespace) throws Exception {
    List<ArtifactSummary> artifacts = delegate.getArtifactSummaries(namespace, false);
    for (ArtifactSummary artifactSummary : artifacts) {
      authorizationEnforcer.enforce(namespace.artifact(artifactSummary.getName(), artifactSummary.getVersion()),
                                    authenticationContext.getPrincipal(), Action.ADMIN);
    }
    delegate.clear(namespace);
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, boolean includeSystem) throws Exception {
    List<ArtifactSummary> artifacts = delegate.getArtifactSummaries(namespace, includeSystem);
    return filterAuthorizedArtifacts(artifacts, namespace);
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, String name, int limit,
                                                    ArtifactSortOrder order) throws Exception {
    List<ArtifactSummary> artifacts = delegate.getArtifactSummaries(namespace, name, limit, order);
    return filterAuthorizedArtifacts(artifacts, namespace);
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(ArtifactRange range, int limit,
                                                    ArtifactSortOrder order) throws Exception {
    List<ArtifactSummary> artifacts = delegate.getArtifactSummaries(range, limit, order);
    // todo - CDAP-11560 should filter in artifact store
    return filterAuthorizedArtifacts(artifacts, new NamespaceId(range.getNamespace()));
  }

  @Override
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    ArtifactId artifact = artifactId.toEntityId();
    // No authorization for system artifacts
    if (!NamespaceId.SYSTEM.equals(artifact.getParent())) {
      // need at least one privilege to get the artifact detail
      AuthorizationUtil.ensureOnePrivilege(artifact, EnumSet.allOf(Action.class), authorizationEnforcer,
                                           authenticationContext.getPrincipal());
    }
    return delegate.getArtifact(artifactId);
  }

  @Override
  public List<ArtifactDetail> getArtifactDetails(final ArtifactRange range, int limit,
                                                 ArtifactSortOrder order) throws Exception {
    List<ArtifactDetail> artifacts = delegate.getArtifactDetails(range, limit, order);
    // No authorization for system artifacts
    if (NamespaceId.SYSTEM.getNamespace().equals(range.getNamespace())) {
      return artifacts;
    }

    final NamespaceId namespaceId = new NamespaceId(range.getNamespace());
    return AuthorizationUtil.isVisible(
      artifacts, authorizationEnforcer, authenticationContext.getPrincipal(),
      new Function<ArtifactDetail, EntityId>() {
        @Override
        public EntityId apply(ArtifactDetail input) {
          co.cask.cdap.api.artifact.ArtifactId artifactId = input.getDescriptor().getArtifactId();
          return namespaceId.artifact(artifactId.getName(), artifactId.getVersion().getVersion());
        }
      }, null);
  }

  @Override
  public List<ApplicationClassSummary> getApplicationClasses(NamespaceId namespace,
                                                             boolean includeSystem) throws IOException {
    return delegate.getApplicationClasses(namespace, includeSystem);
  }

  @Override
  public List<ApplicationClassInfo> getApplicationClasses(NamespaceId namespace, String className) throws IOException {
    return delegate.getApplicationClasses(namespace, className);
  }

  @Override
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(
    NamespaceId namespace, Id.Artifact artifactId) throws IOException, ArtifactNotFoundException {
    return delegate.getPlugins(namespace, artifactId);
  }

  @Override
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(
    NamespaceId namespace, Id.Artifact artifactId, String pluginType) throws IOException, ArtifactNotFoundException {
    return delegate.getPlugins(namespace, artifactId, pluginType);
  }

  @Override
  public SortedMap<ArtifactDescriptor, PluginClass> getPlugins(
    NamespaceId namespace, Id.Artifact artifactId, String pluginType, String pluginName,
    Predicate<ArtifactId> pluginPredicate, int limit,
    ArtifactSortOrder order) throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    return delegate.getPlugins(namespace, artifactId, pluginType, pluginName, pluginPredicate, limit, order);
  }

  @Override
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(
    NamespaceId namespace, ArtifactRange artifactRange, String pluginType, String pluginName,
    PluginSelector selector) throws ArtifactNotFoundException, IOException, PluginNotExistsException {
    return delegate.findPlugin(namespace, artifactRange, pluginType, pluginName, selector);
  }

  @Override
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile) throws Exception {
   return addArtifact(artifactId, artifactFile, null, null);
  }

  @Override
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts,
                                    @Nullable Set<PluginClass> additionalPlugins) throws Exception {
    return addArtifact(artifactId, artifactFile, parentArtifacts, additionalPlugins,
                       Collections.<String, String>emptyMap());
  }

  @Override
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts,
                                    @Nullable Set<PluginClass> additionalPlugins,
                                    Map<String, String> properties) throws Exception {
    if (artifactId.getNamespace().toEntityId().equals(NamespaceId.SYSTEM)) {
      throw new IllegalArgumentException("Cannot add artifact in system namespace");
    }
    // To add an artifact, a user must have ADMIN privilege on the artifact is being added
    // This method is used to add user app artifacts, so enforce authorization on the specified, non-system namespace
    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(artifactId.toEntityId(), principal, Action.ADMIN);
    return delegate.addArtifact(artifactId, artifactFile, parentArtifacts, additionalPlugins, properties);
  }

  @Override
  public void writeArtifactProperties(Id.Artifact artifactId, Map<String, String> properties) throws Exception {
    authorizationEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
    delegate.writeArtifactProperties(artifactId, properties);
  }

  @Override
  public void writeArtifactProperty(Id.Artifact artifactId, String key, String value) throws Exception {
    authorizationEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
    delegate.writeArtifactProperty(artifactId, key, value);
  }

  @Override
  public void deleteArtifactProperty(Id.Artifact artifactId, String key) throws Exception {
    authorizationEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
    delegate.deleteArtifactProperty(artifactId, key);
  }

  @Override
  public void deleteArtifactProperties(Id.Artifact artifactId) throws Exception {
    authorizationEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
    delegate.deleteArtifactProperties(artifactId);
  }

  @Override
  public void addSystemArtifacts() throws Exception {
    // to add system artifacts, users should have admin privileges on the system namespace
    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(NamespaceId.SYSTEM, principal, Action.ADMIN);
    delegate.addSystemArtifacts();
  }

  @Override
  public void deleteArtifact(Id.Artifact artifactId) throws Exception {
    // for deleting artifacts, users need admin privileges on the artifact being deleted.
    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(artifactId.toEntityId(), principal, Action.ADMIN);
    delegate.deleteArtifact(artifactId);
  }

  @Override
  public List<ArtifactInfo> getArtifactsInfo(final NamespaceId namespace) throws Exception {
    List<ArtifactInfo> artifactInfos = delegate.getArtifactsInfo(namespace);
    // todo - CDAP-11560 should filter in artifact store
    return AuthorizationUtil.isVisible(artifactInfos, authorizationEnforcer, authenticationContext.getPrincipal(),
                                       new Function<ArtifactInfo, EntityId>() {
                                         @Override
                                         public EntityId apply(ArtifactInfo input) {
                                           return namespace.artifact(input.getName(), input.getVersion());
                                         }
                                       },
                                       new Predicate<ArtifactInfo>() {
                                         @Override
                                         public boolean apply(ArtifactInfo input) {
                                           return ArtifactScope.SYSTEM.equals(input.getScope());
                                         }
                                       });
  }

  /**
   * Filter a list of {@link ArtifactSummary} that ensures the logged-in user has a {@link Action privilege} on
   *
   * @param artifacts the {@link List<ArtifactSummary>} to filter with
   * @param namespace namespace of the artifacts
   * @return filtered list of {@link ArtifactSummary}
   */
  private List<ArtifactSummary> filterAuthorizedArtifacts(List<ArtifactSummary> artifacts,
                                                          final NamespaceId namespace) throws Exception {
    return AuthorizationUtil.isVisible(artifacts, authorizationEnforcer, authenticationContext.getPrincipal(),
                                       new Function<ArtifactSummary, EntityId>() {
                                         @Override
                                         public EntityId apply(ArtifactSummary input) {
                                           return namespace.artifact(input.getName(), input.getVersion());
                                         }
                                       },
                                       new Predicate<ArtifactSummary>() {
                                         @Override
                                         public boolean apply(ArtifactSummary input) {
                                           return ArtifactScope.SYSTEM.equals(input.getScope());
                                         }
                                       });
  }
}
