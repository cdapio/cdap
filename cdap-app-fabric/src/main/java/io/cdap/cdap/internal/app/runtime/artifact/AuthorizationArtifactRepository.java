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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.inject.name.Named;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
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
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  public AuthorizationArtifactRepository(@Named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO)
                                           ArtifactRepository artifactRepository,
                                         AccessEnforcer accessEnforcer,
                                         AuthenticationContext authenticationContext) {
    this.delegate = artifactRepository;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @Override
  public CloseableClassLoader createArtifactClassLoader(ArtifactDescriptor artifactDescriptor,
                                                        EntityImpersonator entityImpersonator) throws IOException {
    return delegate.createArtifactClassLoader(artifactDescriptor, entityImpersonator);
  }

  @Override
  public void clear(NamespaceId namespace) throws Exception {
    List<ArtifactSummary> artifacts = delegate.getArtifactSummaries(namespace, false);
    for (ArtifactSummary artifactSummary : artifacts) {
      accessEnforcer.enforce(namespace.artifact(artifactSummary.getName(), artifactSummary.getVersion()),
                             authenticationContext.getPrincipal(), StandardPermission.DELETE);
    }
    delegate.clear(namespace);
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, boolean includeSystem) throws Exception {
    accessEnforcer.enforceOnParent(EntityType.ARTIFACT, namespace, authenticationContext.getPrincipal(),
                                   StandardPermission.LIST);
    return delegate.getArtifactSummaries(namespace, includeSystem);
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, String name, int limit,
                                                    ArtifactSortOrder order) throws Exception {
    accessEnforcer.enforceOnParent(EntityType.ARTIFACT, namespace, authenticationContext.getPrincipal(),
                                   StandardPermission.LIST);
    return delegate.getArtifactSummaries(namespace, name, limit, order);
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(ArtifactRange range, int limit,
                                                    ArtifactSortOrder order) throws Exception {
    accessEnforcer.enforceOnParent(EntityType.ARTIFACT, new NamespaceId(range.getNamespace()),
                                   authenticationContext.getPrincipal(),
                                   StandardPermission.LIST);

    return delegate.getArtifactSummaries(range, limit, order);
  }

  @Override
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    ArtifactId artifact = artifactId.toEntityId();
    // No authorization for system artifacts
    if (!NamespaceId.SYSTEM.equals(artifact.getParent())) {
      accessEnforcer.enforce(artifact, authenticationContext.getPrincipal(), StandardPermission.GET);
    }
    return delegate.getArtifact(artifactId);
  }

  @Override
  public InputStream newInputStream(Id.Artifact artifactId) throws IOException, NotFoundException {
    return delegate.newInputStream(artifactId);
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
      artifacts, accessEnforcer, authenticationContext.getPrincipal(),
      new Function<ArtifactDetail, EntityId>() {
        @Override
        public EntityId apply(ArtifactDetail input) {
          io.cdap.cdap.api.artifact.ArtifactId artifactId = input.getDescriptor().getArtifactId();
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
    // To add an artifact, a user must have ADMIN privilege on the artifact is being added
    Principal principal = authenticationContext.getPrincipal();
    accessEnforcer.enforce(artifactId.toEntityId(), principal, StandardPermission.CREATE);
    return delegate.addArtifact(artifactId, artifactFile, parentArtifacts, additionalPlugins, properties);
  }

  @Override
  public void writeArtifactProperties(Id.Artifact artifactId, Map<String, String> properties) throws Exception {
    accessEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    delegate.writeArtifactProperties(artifactId, properties);
  }

  @Override
  public void writeArtifactProperty(Id.Artifact artifactId, String key, String value) throws Exception {
    accessEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    delegate.writeArtifactProperty(artifactId, key, value);
  }

  @Override
  public void deleteArtifactProperty(Id.Artifact artifactId, String key) throws Exception {
    accessEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    delegate.deleteArtifactProperty(artifactId, key);
  }

  @Override
  public void deleteArtifactProperties(Id.Artifact artifactId) throws Exception {
    accessEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    delegate.deleteArtifactProperties(artifactId);
  }

  @Override
  public void addSystemArtifacts() throws Exception {
    // to add system artifacts, users should have admin privileges on the system namespace
    Principal principal = authenticationContext.getPrincipal();
    accessEnforcer.enforceOnParent(EntityType.ARTIFACT, NamespaceId.SYSTEM, principal, StandardPermission.CREATE);
    delegate.addSystemArtifacts();
  }

  @Override
  public void deleteArtifact(Id.Artifact artifactId) throws Exception {
    // for deleting artifacts, users need admin privileges on the artifact being deleted.
    Principal principal = authenticationContext.getPrincipal();
    accessEnforcer.enforce(artifactId.toEntityId(), principal, StandardPermission.DELETE);
    delegate.deleteArtifact(artifactId);
  }

  @Override
  public List<ArtifactInfo> getArtifactsInfo(final NamespaceId namespace) throws Exception {
    accessEnforcer.enforceOnParent(EntityType.ARTIFACT, namespace, authenticationContext.getPrincipal(),
                                   StandardPermission.LIST);
    return delegate.getArtifactsInfo(namespace);
  }
}
