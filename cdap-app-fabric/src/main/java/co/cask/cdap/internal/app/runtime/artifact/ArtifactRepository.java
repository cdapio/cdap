/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.common.ArtifactAlreadyExistsException;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.ArtifactRangeNotFoundException;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.common.conf.ArtifactConfig;
import co.cask.cdap.common.conf.ArtifactConfigReader;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.system.ArtifactSystemMetadataWriter;
import co.cask.cdap.internal.app.deploy.pipeline.NamespacedImpersonator;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ApplicationClass;
import co.cask.cdap.proto.artifact.ApplicationClassInfo;
import co.cask.cdap.proto.artifact.ApplicationClassSummary;
import co.cask.cdap.proto.artifact.ArtifactClasses;
import co.cask.cdap.proto.artifact.ArtifactInfo;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
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
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactRepository.class);
  private final ArtifactStore artifactStore;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final ArtifactInspector artifactInspector;
  private final List<File> systemArtifactDirs;
  private final ArtifactConfigReader configReader;
  private final MetadataStore metadataStore;
  private final PrivilegesManager privilegesManager;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final Impersonator impersonator;

  @VisibleForTesting
  @Inject
  public ArtifactRepository(CConfiguration cConf, ArtifactStore artifactStore, MetadataStore metadataStore,
                            PrivilegesManager privilegesManager, ProgramRunnerFactory programRunnerFactory,
                            Impersonator impersonator, AuthorizationEnforcer authorizationEnforcer,
                            AuthenticationContext authenticationContext) {
    this.artifactStore = artifactStore;
    this.artifactClassLoaderFactory = new ArtifactClassLoaderFactory(cConf, programRunnerFactory);
    this.artifactInspector = new ArtifactInspector(cConf, artifactClassLoaderFactory);
    this.systemArtifactDirs = new ArrayList<>();
    for (String dir : cConf.get(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR).split(";")) {
      File file = new File(dir);
      if (!file.isDirectory()) {
        LOG.warn("Ignoring {} because it is not a directory.", file);
        continue;
      }
      systemArtifactDirs.add(file);
    }
    this.configReader = new ArtifactConfigReader();
    this.metadataStore = metadataStore;
    this.privilegesManager = privilegesManager;
    this.impersonator = impersonator;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  /**
   * Create a classloader that uses the artifact at the specified location to load classes, with access to
   * packages that all program type has access to.
   * It delegates to {@link ArtifactClassLoaderFactory#createClassLoader(Location, NamespacedImpersonator)}.
   *
   * @see ArtifactClassLoaderFactory
   */
  public CloseableClassLoader createArtifactClassLoader(
    Location artifactLocation, NamespacedImpersonator namespacedImpersonator) throws IOException {
    return artifactClassLoaderFactory.createClassLoader(artifactLocation, namespacedImpersonator);
  }

  /**
   * Clear all artifacts in the given namespace. This method is only intended to be called by unit tests, and
   * when a namespace is being deleted.
   *
   * @param namespace the namespace to delete artifacts in.
   * @throws IOException if there was an error making changes in the meta store
   */
  public void clear(NamespaceId namespace) throws Exception {
    for (ArtifactDetail artifactDetail : artifactStore.getArtifacts(namespace)) {
      deleteArtifact(Id.Artifact.from(namespace.toId(), artifactDetail.getDescriptor().getArtifactId()));
    }
  }

  /**
   * Get all artifacts in the given namespace, optionally including system artifacts as well. Will never return
   * null. If no artifacts exist, an empty list is returned. Namespace existence is not checked.
   *
   * @param namespace the namespace to get artifacts from
   * @param includeSystem whether system artifacts should be included in the results
   * @return an unmodifiable list of artifacts that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  public List<ArtifactSummary> getArtifacts(final NamespaceId namespace, boolean includeSystem) throws Exception {
    List<ArtifactSummary> summaries = new ArrayList<>();
    if (includeSystem) {
      convertAndAdd(summaries, artifactStore.getArtifacts(NamespaceId.SYSTEM));
    }
    List<ArtifactSummary> artifacts = convertAndAdd(summaries, artifactStore.getArtifacts(namespace));
    return Collections.unmodifiableList(Lists.newArrayList(filterAuthorizedArtifacts(artifacts, namespace)));
  }

  /**
   * Get all artifacts in the given namespace of the given name. Will never return null.
   * If no artifacts exist, an exception is thrown. Namespace existence is not checked.
   *
   * @param namespace the namespace to get artifacts from
   * @param name the name of artifacts to get
   * @return an unmodifiable list of artifacts in the given namespace of the given name
   * @throws IOException if there as an exception reading from the meta store
   * @throws ArtifactNotFoundException if no artifacts of the given name in the given namespace exist
   */
  public List<ArtifactSummary> getArtifacts(NamespaceId namespace, String name)
    throws Exception {
    List<ArtifactSummary> summaries = new ArrayList<>();
    List<ArtifactSummary> artifacts = convertAndAdd(summaries, artifactStore.getArtifacts(namespace, name));
    return Collections.unmodifiableList(Lists.newArrayList(filterAuthorizedArtifacts(artifacts, namespace)));
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
   * Get all artifacts that match artifacts in the given ranges.
   *
   * @param range the range to match artifacts in
   * @return an unmodifiable list of all artifacts that match the given ranges. If none exist, an empty list is returned
   */
  public List<ArtifactDetail> getArtifacts(final ArtifactRange range) throws Exception {
    List<ArtifactDetail> artifacts = artifactStore.getArtifacts(range);

    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    return Lists.newArrayList(
      Iterables.filter(artifacts, new com.google.common.base.Predicate<ArtifactDetail>() {
        @Override
        public boolean apply(ArtifactDetail artifactDetail) {
          ArtifactId artifactId = artifactDetail.getDescriptor().getArtifactId();
          return filter.apply(range.getNamespace().toEntityId().artifact(artifactId.getName(),
                                                                         artifactId.getVersion().getVersion()));
        }
      })
    );
  }

  /**
   * Get all application classes in the given namespace, optionally including classes from system artifacts as well.
   * Will never return null. If no artifacts exist, an empty list is returned. Namespace existence is not checked.
   *
   * @param namespace the namespace to get application classes from
   * @param includeSystem whether classes from system artifacts should be included in the results
   * @return an unmodifiable list of application classes that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  public List<ApplicationClassSummary> getApplicationClasses(NamespaceId namespace,
                                                             boolean includeSystem) throws IOException {
    List<ApplicationClassSummary> summaries = Lists.newArrayList();
    if (includeSystem) {
      addAppSummaries(summaries, NamespaceId.SYSTEM);
    }
    addAppSummaries(summaries, namespace);

    return Collections.unmodifiableList(summaries);
  }

  /**
   * Get all application classes in the given namespace of the given class name.
   * Will never return null. If no artifacts exist, an empty list is returned. Namespace existence is not checked.
   *
   * @param namespace the namespace to get application classes from
   * @param className the application class to get
   * @return an unmodifiable list of application classes that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  public List<ApplicationClassInfo> getApplicationClasses(NamespaceId namespace,
                                                          String className) throws IOException {
    List<ApplicationClassInfo> infos = Lists.newArrayList();
    for (Map.Entry<ArtifactDescriptor, ApplicationClass> entry :
      artifactStore.getApplicationClasses(namespace, className).entrySet()) {
      ArtifactSummary artifactSummary = ArtifactSummary.from(entry.getKey().getArtifactId());
      ApplicationClass appClass = entry.getValue();
      infos.add(new ApplicationClassInfo(artifactSummary, appClass.getClassName(), appClass.getConfigSchema()));
    }
    return Collections.unmodifiableList(infos);
  }


  /**
   * Returns a {@link SortedMap} of plugin artifact to all plugins available for the given artifact. The keys
   * are sorted by the {@link ArtifactDescriptor} for the artifact that contains plugins available to the given
   * artifact.
   *
   * @param namespace the namespace to get plugins from
   * @param artifactId the id of the artifact to get plugins for
   * @return an unmodifiable sorted map from plugin artifact to plugins in that artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   */
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(NamespaceId namespace, Id.Artifact artifactId)
    throws IOException, ArtifactNotFoundException {
    return artifactStore.getPluginClasses(namespace, artifactId);
  }

  /**
   * Returns a {@link SortedMap} of plugin artifact to all plugins of the given type available for the given artifact.
   * The keys are sorted by the {@link ArtifactDescriptor} for the artifact that contains plugins available to the given
   * artifact.
   *
   * @param namespace the namespace to get plugins from
   * @param artifactId the id of the artifact to get plugins for
   * @param pluginType the type of plugins to get
   * @return an unmodifiable sorted map from plugin artifact to plugins in that artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   */
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(NamespaceId namespace, Id.Artifact artifactId,
                                                                    String pluginType)
    throws IOException, ArtifactNotFoundException {
    return artifactStore.getPluginClasses(namespace, artifactId, pluginType);
  }

  /**
   * Returns a {@link SortedMap} of plugin artifact to plugin available for the given artifact. The keys
   * are sorted by the {@link ArtifactDescriptor} for the artifact that contains plugins available to the given
   * artifact.
   *
   * @param namespace the namespace to get plugins from
   * @param artifactId the id of the artifact to get plugins for
   * @param pluginType the type of plugins to get
   * @param pluginName the name of plugins to get
   * @return an unmodifiable sorted map from plugin artifact to plugins in that artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   */
  public SortedMap<ArtifactDescriptor, PluginClass> getPlugins(NamespaceId namespace, Id.Artifact artifactId,
                                                               String pluginType, String pluginName)
    throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    return artifactStore.getPluginClasses(namespace, artifactId, pluginType, pluginName);
  }

  /**
   * Returns a {@link Map.Entry} representing the plugin information for the plugin being requested.
   *
   * @param namespace the namespace to get plugins from
   * @param artifactId the id of the artifact to get plugins for
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param selector for selecting which plugin to use
   * @return the entry found
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws PluginNotExistsException if no plugins of the given type and name are available to the given artifact
   */
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(NamespaceId namespace, Id.Artifact artifactId,
                                                               String pluginType, String pluginName,
                                                               PluginSelector selector)
    throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    SortedMap<ArtifactDescriptor, PluginClass> pluginClasses = artifactStore.getPluginClasses(
      namespace, artifactId, pluginType, pluginName);
    SortedMap<ArtifactId, PluginClass> artifactIds = Maps.newTreeMap();
    for (Map.Entry<ArtifactDescriptor, PluginClass> pluginClassEntry : pluginClasses.entrySet()) {
      artifactIds.put(pluginClassEntry.getKey().getArtifactId(), pluginClassEntry.getValue());
    }
    Map.Entry<ArtifactId, PluginClass> chosenArtifact = selector.select(artifactIds);
    if (chosenArtifact == null) {
      throw new PluginNotExistsException(artifactId, pluginType, pluginName);
    }

    for (Map.Entry<ArtifactDescriptor, PluginClass> pluginClassEntry : pluginClasses.entrySet()) {
      if (pluginClassEntry.getKey().getArtifactId().compareTo(chosenArtifact.getKey()) == 0) {
        return pluginClassEntry;
      }
    }
    throw new PluginNotExistsException(artifactId, pluginType, pluginName);
  }

  /**
   * Inspects and builds plugin and application information for the given artifact.
   *
   * @param artifactId the id of the artifact to inspect and store
   * @param artifactFile the artifact to inspect and store
   * @return detail about the newly added artifact
   * @throws IOException if there was an exception reading from the artifact store
   * @throws ArtifactAlreadyExistsException if the artifact already exists
   * @throws InvalidArtifactException if the artifact is invalid. For example, if it is not a zip file,
   *                                  or the application class given is not an Application.
   * @throws UnauthorizedException if the current user does not have the privilege to add an artifact in the specified
   *                               namespace. To add an artifact, a user needs {@link Action#WRITE} privilege on the
   *                               namespace in which the artifact is being added. If authorization is successful, and
   *                               the artifact is added successfully, then the user gets all {@link Action privileges}
   *                               on the added artifact.
   */
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile) throws Exception {
    return addArtifact(artifactId, artifactFile, null);
  }

  /**
   * Inspects and builds plugin and application information for the given artifact.
   *
   * @param artifactId the id of the artifact to inspect and store
   * @param artifactFile the artifact to inspect and store
   * @param parentArtifacts artifacts the given artifact extends.
   *                        If null, the given artifact does not extend another artifact
   * @throws IOException if there was an exception reading from the artifact store
   * @throws ArtifactRangeNotFoundException if none of the parent artifacts could be found
   * @throws ArtifactAlreadyExistsException if the artifact already exists and is not a snapshot version
   * @throws InvalidArtifactException if the artifact is invalid. Can happen if it is not a zip file,
   *                                  if the application class given is not an Application,
   *                                  or if it has parents that also have parents.
   * @throws UnauthorizedException if the user is not authorized to add an artifact in the specified namespace. To add
   *                               an artifact, a user must have {@link Action#WRITE} on the namespace in which
   *                               the artifact is being added. If authorization is successful, and
   *                               the artifact is added successfully, then the user gets all {@link Action privileges}
   *                               on the added artifact.
   */
  @VisibleForTesting
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts) throws Exception {
    return addArtifact(artifactId, artifactFile, parentArtifacts, null);
  }

  /**
   * Inspects and builds plugin and application information for the given artifact, adding an additional set of
   * plugin classes to the plugins found through inspection. This method is used when all plugin classes
   * cannot be derived by inspecting the artifact but need to be explicitly set. This is true for 3rd party plugins
   * like jdbc drivers.
   *
   * @param artifactId the id of the artifact to inspect and store
   * @param artifactFile the artifact to inspect and store
   * @param parentArtifacts artifacts the given artifact extends.
   *                        If null, the given artifact does not extend another artifact
   * @param additionalPlugins the set of additional plugin classes to add to the plugins found through inspection.
   *                          If null, no additional plugin classes will be added
   * @throws IOException if there was an exception reading from the artifact store
   * @throws ArtifactRangeNotFoundException if none of the parent artifacts could be found
   * @throws UnauthorizedException if the user is not authorized to add an artifact in the specified namespace. To add
   *                               an artifact, a user must have {@link Action#WRITE} on the namespace in which
   *                               the artifact is being added. If authorization is successful, and
   *                               the artifact is added successfully, then the user gets all {@link Action privileges}
   *                               on the added artifact.
   */
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts,
                                    @Nullable Set<PluginClass> additionalPlugins) throws Exception {
    // To add an artifact, a user must have write privileges on the namespace in which the artifact is being added
    // This method is used to add user app artifacts, so enforce authorization on the specified, non-system namespace
    Principal principal = authenticationContext.getPrincipal();
    NamespaceId namespace = artifactId.getNamespace().toEntityId();
    authorizationEnforcer.enforce(namespace, principal, Action.WRITE);

    ArtifactDetail artifactDetail = addArtifact(artifactId, artifactFile, parentArtifacts, additionalPlugins,
                                                Collections.<String, String>emptyMap());
    // artifact successfully added. now grant ALL permissions on the artifact to the current user
    privilegesManager.grant(artifactId.toEntityId(), principal, EnumSet.allOf(Action.class));
    return artifactDetail;
  }

  /**
   * Inspects and builds plugin and application information for the given artifact, adding an additional set of
   * plugin classes to the plugins found through inspection. This method is used when all plugin classes
   * cannot be derived by inspecting the artifact but need to be explicitly set. This is true for 3rd party plugins
   * like jdbc drivers.
   *
   * @param artifactId the id of the artifact to inspect and store
   * @param artifactFile the artifact to inspect and store
   * @param parentArtifacts artifacts the given artifact extends.
   *                        If null, the given artifact does not extend another artifact
   * @param additionalPlugins the set of additional plugin classes to add to the plugins found through inspection.
   *                          If null, no additional plugin classes will be added
   * @param properties properties for the artifact
   * @throws IOException if there was an exception reading from the artifact store
   * @throws ArtifactRangeNotFoundException if none of the parent artifacts could be found
   * @throws UnauthorizedException if the user is not authorized to add an artifact in the specified namespace. To add
   *                               an artifact, a user must have {@link Action#WRITE} on the namespace in which
   *                               the artifact is being added. If authorization is successful, and
   *                               the artifact is added successfully, then the user gets all {@link Action privileges}
   *                               on the added artifact.
   */
  @VisibleForTesting
  public ArtifactDetail addArtifact(final Id.Artifact artifactId, final File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts,
                                    @Nullable Set<PluginClass> additionalPlugins,
                                    Map<String, String> properties) throws Exception {
    if (additionalPlugins != null) {
      validatePluginSet(additionalPlugins);
    }

    parentArtifacts = parentArtifacts == null ? Collections.<ArtifactRange>emptySet() : parentArtifacts;
    CloseableClassLoader parentClassLoader = null;
    NamespacedImpersonator namespacedImpersonator = new NamespacedImpersonator(artifactId.getNamespace().toEntityId(),
                                                                               impersonator);
    if (!parentArtifacts.isEmpty()) {
      validateParentSet(artifactId, parentArtifacts);
      parentClassLoader = createParentClassLoader(artifactId, parentArtifacts, namespacedImpersonator);
    }
    try {
      ArtifactClasses artifactClasses = inspectArtifact(artifactId, artifactFile, additionalPlugins, parentClassLoader);
      ArtifactMeta meta = new ArtifactMeta(artifactClasses, parentArtifacts, properties);
      ArtifactDetail artifactDetail =
        artifactStore.write(artifactId, meta, Files.newInputStreamSupplier(artifactFile), namespacedImpersonator);
      ArtifactDescriptor descriptor = artifactDetail.getDescriptor();
      // info hides some fields that are available in detail, such as the location of the artifact
      ArtifactInfo artifactInfo = new ArtifactInfo(descriptor.getArtifactId(), artifactDetail.getMeta().getClasses(),
                                                   artifactDetail.getMeta().getProperties());
      // add system metadata for artifacts
      writeSystemMetadata(artifactId.toEntityId(), artifactInfo);
      return artifactDetail;
    } finally {
      Closeables.closeQuietly(parentClassLoader);
    }
  }

  /**
   * Writes properties for an artifact. Any existing properties will be overwritten
   *
   * @param artifactId the id of the artifact to add properties to
   * @param properties the artifact properties to add
   * @throws IOException if there was an exception writing to the artifact store
   * @throws ArtifactNotFoundException if the artifact does not exist
   * @throws UnauthorizedException if the current user is not permitted to write properties to the artifact. To be able
   *                               to write properties to an artifact, users must have admin privileges on the artifact
   */
  public void writeArtifactProperties(Id.Artifact artifactId, final Map<String, String> properties) throws Exception {
    authorizationEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
    artifactStore.updateArtifactProperties(artifactId, new Function<Map<String, String>, Map<String, String>>() {
      @Override
      public Map<String, String> apply(Map<String, String> oldProperties) {
        return properties;
      }
    });
  }

  /**
   * Writes a property for an artifact. If the property already exists, it will be overwritten. If it does not exist,
   * it will be added.
   *
   * @param artifactId the id of the artifact to write a property to
   * @param key the property key to write
   * @param value the property value to write
   * @throws IOException if there was an exception writing to the artifact store
   * @throws ArtifactNotFoundException if the artifact does not exist
   * @throws UnauthorizedException if the current user is not permitted to write properties to the artifact. To be able
   *                               to write properties to an artifact, users must have admin privileges on the artifact
   */
  public void writeArtifactProperty(Id.Artifact artifactId, final String key, final String value) throws Exception {
    authorizationEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
    artifactStore.updateArtifactProperties(artifactId, new Function<Map<String, String>, Map<String, String>>() {
      @Override
      public Map<String, String> apply(Map<String, String> oldProperties) {
        Map<String, String> updated = new HashMap<>();
        updated.putAll(oldProperties);
        updated.put(key, value);
        return updated;
      }
    });
  }

  /**
   * Deletes a property for an artifact. If the property does not exist, this will be a no-op.
   *
   * @param artifactId the id of the artifact to delete a property from
   * @param key the property to delete
   * @throws IOException if there was an exception writing to the artifact store
   * @throws ArtifactNotFoundException if the artifact does not exist
   * @throws UnauthorizedException if the current user is not permitted to remove a property from the artifact. To be
   *                               able to remove a property, users must have admin privileges on the artifact
   */
  public void deleteArtifactProperty(Id.Artifact artifactId, final String key) throws Exception {
    authorizationEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
    artifactStore.updateArtifactProperties(artifactId, new Function<Map<String, String>, Map<String, String>>() {
      @Override
      public Map<String, String> apply(Map<String, String> oldProperties) {
        if (!oldProperties.containsKey(key)) {
          return oldProperties;
        }
        Map<String, String> updated = new HashMap<>();
        updated.putAll(oldProperties);
        updated.remove(key);
        return updated;
      }
    });
  }

  /**
   * Deletes all properties for an artifact. If no properties exist, this will be a no-op.
   *
   * @param artifactId the id of the artifact to delete properties from
   * @throws IOException if there was an exception writing to the artifact store
   * @throws ArtifactNotFoundException if the artifact does not exist
   * @throws UnauthorizedException if the current user is not permitted to remove properties from the artifact. To be
   *                               able to remove properties, users must have admin privileges on the artifact
   */
  public void deleteArtifactProperties(Id.Artifact artifactId) throws Exception {
    authorizationEnforcer.enforce(artifactId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
    artifactStore.updateArtifactProperties(artifactId, new Function<Map<String, String>, Map<String, String>>() {
      @Override
      public Map<String, String> apply(Map<String, String> oldProperties) {
        return new HashMap<>();
      }
    });
  }

  private ArtifactClasses inspectArtifact(Id.Artifact artifactId, File artifactFile,
                                          @Nullable Set<PluginClass> additionalPlugins,
                                          @Nullable  ClassLoader parentClassLoader) throws IOException,
                                                                                           InvalidArtifactException {
    ArtifactClasses artifactClasses = artifactInspector.inspectArtifact(artifactId, artifactFile, parentClassLoader);
    validatePluginSet(artifactClasses.getPlugins());
    if (additionalPlugins == null || additionalPlugins.isEmpty()) {
      return artifactClasses;
    } else {
      return ArtifactClasses.builder()
        .addApps(artifactClasses.getApps())
        .addPlugins(artifactClasses.getPlugins())
        .addPlugins(additionalPlugins)
        .build();
    }
  }

  /**
   * Scan all files in the local system artifact directory, looking for jar files and adding them as system artifacts.
   * If the artifact already exists it will not be added again unless it is a snapshot version.
   *
   * @throws IOException if there was some IO error adding the system artifacts
   */
  public void addSystemArtifacts() throws Exception {
    // to add system artifacts, users should have write privileges on the system namespace
    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(NamespaceId.SYSTEM, principal, Action.WRITE);
    // scan the directory for artifact .jar files and config files for those artifacts
    List<SystemArtifactInfo> systemArtifacts = new ArrayList<>();
    for (File systemArtifactDir : systemArtifactDirs) {
      for (File jarFile : DirUtils.listFiles(systemArtifactDir, "jar")) {
        // parse id from filename
        Id.Artifact artifactId;
        try {
          artifactId = Id.Artifact.parse(Id.Namespace.SYSTEM, jarFile.getName());
        } catch (IllegalArgumentException e) {
          LOG.warn(String.format("Skipping system artifact '%s' because the name is invalid: ", e.getMessage()));
          continue;
        }

        // first revoke any orphane privileges
        co.cask.cdap.proto.id.ArtifactId artifact = artifactId.toEntityId();
        privilegesManager.revoke(artifact);
        // then grant all on the artifact
        privilegesManager.grant(artifact, principal, EnumSet.allOf(Action.class));

        // check for a corresponding .json config file
        String artifactFileName = jarFile.getName();
        String configFileName = artifactFileName.substring(0, artifactFileName.length() - ".jar".length()) + ".json";
        File configFile = new File(systemArtifactDir, configFileName);

        try {
          // read and parse the config file if it exists. Otherwise use an empty config with the artifact filename
          ArtifactConfig artifactConfig = configFile.isFile() ?
            configReader.read(artifactId.getNamespace(), configFile) : new ArtifactConfig();

          validateParentSet(artifactId, artifactConfig.getParents());
          validatePluginSet(artifactConfig.getPlugins());
          systemArtifacts.add(new SystemArtifactInfo(artifactId, jarFile, artifactConfig));
        } catch (InvalidArtifactException e) {
          LOG.warn(String.format("Could not add system artifact '%s' because it is invalid.", artifactFileName), e);
          // since adding artifact failed, revoke privileges, since they may be orphane now
          privilegesManager.revoke(artifact);
        }
      }
    }

    // taking advantage of the fact that we only have 1 level of dependencies
    // so we can add all the parents first, then we know its safe to add everything else
    // add all parents
    Set<Id.Artifact> parents = new HashSet<>();
    for (SystemArtifactInfo child : systemArtifacts) {
      Id.Artifact childId = child.getArtifactId();

      for (SystemArtifactInfo potentialParent : systemArtifacts) {
        Id.Artifact potentialParentId = potentialParent.getArtifactId();
        // skip if we're looking at ourselves
        if (childId.equals(potentialParentId)) {
          continue;
        }

        if (child.getConfig().hasParent(potentialParentId)) {
          parents.add(potentialParentId);
        }
      }
    }

    // add all parents first
    for (SystemArtifactInfo systemArtifact : systemArtifacts) {
      if (parents.contains(systemArtifact.getArtifactId())) {
        addSystemArtifact(systemArtifact);
      }
    }

    // add children next
    for (SystemArtifactInfo systemArtifact : systemArtifacts) {
      if (!parents.contains(systemArtifact.getArtifactId())) {
        addSystemArtifact(systemArtifact);
      }
    }
  }

  private void addSystemArtifact(SystemArtifactInfo systemArtifactInfo) throws Exception {
    String fileName = systemArtifactInfo.getArtifactFile().getName();
    try {
      Id.Artifact artifactId = systemArtifactInfo.getArtifactId();

      // if it's not a snapshot and it already exists, don't bother trying to add it since artifacts are immutable
      if (!artifactId.getVersion().isSnapshot()) {
        try {
          artifactStore.getArtifact(artifactId);
          LOG.info("Artifact {} already exists, will not try loading it again.", artifactId);
          return;
        } catch (ArtifactNotFoundException e) {
          // this is fine, means it doesn't exist yet and we should add it
        }
      }

      addArtifact(artifactId,
                  systemArtifactInfo.getArtifactFile(),
                  systemArtifactInfo.getConfig().getParents(),
                  systemArtifactInfo.getConfig().getPlugins(),
                  systemArtifactInfo.getConfig().getProperties());
      LOG.info("Added system artifact {}.", artifactId);
    } catch (ArtifactAlreadyExistsException e) {
      // shouldn't happen... but if it does for some reason it's fine, it means it was added some other way already.
    } catch (ArtifactRangeNotFoundException e) {
      LOG.warn("Could not add system artifact '{}' because it extends artifacts that do not exist.", fileName, e);
    } catch (InvalidArtifactException e) {
      LOG.warn("Could not add system artifact '{}' because it is invalid.", fileName, e);
    } catch (UnauthorizedException e) {
      LOG.warn("Could not add system artifact '{}' because of an authorization error.", fileName, e);
    }
  }

  /**
   * Delete the specified artifact. Programs that use the artifact will not be able to start.
   *
   * @param artifactId the artifact to delete
   * @throws IOException if there was some IO error deleting the artifact
   * @throws UnauthorizedException if the current user is not authorized to delete the artifact. To delete an artifact,
   *                               a user needs {@link Action#ADMIN} permission on the artifact.
   */
  public void deleteArtifact(Id.Artifact artifactId) throws Exception {
    // for deleting artifacts, users need admin privileges on the artifact being deleted.
    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(artifactId.toEntityId(), principal, Action.ADMIN);
    // delete the artifact first and then privileges. Not the other way to avoid orphan artifact
    // which does not have any privilege if the artifact delete from store fails. see CDAP-6648
    artifactStore.delete(artifactId);
    metadataStore.removeMetadata(artifactId.toEntityId());
    // revoke all privileges on the artifact
    privilegesManager.revoke(artifactId.toEntityId());
  }

  // convert details to summaries (to hide location and other unnecessary information)
  private List<ArtifactSummary> convertAndAdd(List<ArtifactSummary> summaries, Iterable<ArtifactDetail> details) {
    for (ArtifactDetail detail : details) {
      summaries.add(ArtifactSummary.from(detail.getDescriptor().getArtifactId()));
    }
    return summaries;
  }

  /**
   * Filter a list of {@link ArtifactSummary} that ensures the logged-in user has a {@link Action privilege} on
   *
   * @param artifacts the {@link List<ArtifactSummary>} to filter with
   * @param namespace namespace of the artifacts
   * @return filtered list of {@link ArtifactSummary}
   */
  private List<ArtifactSummary> filterAuthorizedArtifacts(List<ArtifactSummary> artifacts, final NamespaceId namespace)
    throws Exception {
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(authenticationContext.getPrincipal());
    return Lists.newArrayList(
      Iterables.filter(artifacts, new com.google.common.base.Predicate<ArtifactSummary>() {
        @Override
        public boolean apply(ArtifactSummary artifactSummary) {
          // no authorization on system artifacts
          return ArtifactScope.SYSTEM.equals(artifactSummary.getScope()) ||
            filter.apply(namespace.artifact(artifactSummary.getName(), artifactSummary.getVersion()));
        }
      })
    );
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
   * Create a parent classloader using an artifact from one of the artifacts in the specified parents.
   *
   * @param artifactId the id of the artifact to create the parent classloader for
   * @param parentArtifacts the ranges of parents to create the classloader from
   * @return a classloader based off a parent artifact
   * @throws ArtifactRangeNotFoundException if none of the parents could be found
   * @throws InvalidArtifactException if one of the parents also has parents
   * @throws IOException if there was some error reading from the store
   */
  private CloseableClassLoader createParentClassLoader(Id.Artifact artifactId, Set<ArtifactRange> parentArtifacts,
                                                       NamespacedImpersonator namespacedImpersonator)
    throws ArtifactRangeNotFoundException, IOException, InvalidArtifactException {

    List<ArtifactDetail> parents = new ArrayList<>();
    for (ArtifactRange parentRange : parentArtifacts) {
      parents.addAll(artifactStore.getArtifacts(parentRange));
    }

    if (parents.isEmpty()) {
      throw new ArtifactRangeNotFoundException(String.format("Artifact %s extends artifacts '%s' that do not exist",
                                                             artifactId, Joiner.on('/').join(parentArtifacts)));
    }

    // check if any of the parents also have parents, which is not allowed. This is to simplify things
    // so that we don't have to chain a bunch of classloaders, and also to keep it simple for users to avoid
    // complicated dependency trees that are hard to manage.
    boolean isInvalid = false;
    StringBuilder errMsg = new StringBuilder("Invalid artifact '")
      .append(artifactId)
      .append("'.")
      .append(" Artifact parents cannot have parents.");
    for (ArtifactDetail parent : parents) {
      Set<ArtifactRange> grandparents = parent.getMeta().getUsableBy();
      if (!grandparents.isEmpty()) {
        isInvalid = true;
        errMsg
          .append(" Parent '")
          .append(parent.getDescriptor().getArtifactId().getName())
          .append("-")
          .append(parent.getDescriptor().getArtifactId().getVersion().getVersion())
          .append("' has parents.");
      }
    }
    if (isInvalid) {
      throw new InvalidArtifactException(errMsg.toString());
    }

    // assumes any of the parents will do
    Location parentLocation = parents.get(0).getDescriptor().getLocation();

    return createArtifactClassLoader(parentLocation, namespacedImpersonator);
  }

  private void addAppSummaries(List<ApplicationClassSummary> summaries, NamespaceId namespace) {
    for (Map.Entry<ArtifactDescriptor, List<ApplicationClass>> classInfo :
      artifactStore.getApplicationClasses(namespace).entrySet()) {
      ArtifactSummary artifactSummary = ArtifactSummary.from(classInfo.getKey().getArtifactId());

      for (ApplicationClass appClass : classInfo.getValue()) {
        summaries.add(new ApplicationClassSummary(artifactSummary, appClass.getClassName()));
      }
    }
  }

  /**
   * Validates the parents of an artifact. Checks that each artifact only appears with a single version range.
   *
   * @param parents the set of parent ranges to validate
   * @throws InvalidArtifactException if there is more than one version range for an artifact
   */
  @VisibleForTesting
  static void validateParentSet(Id.Artifact artifactId, Set<ArtifactRange> parents) throws InvalidArtifactException {
    boolean isInvalid = false;
    StringBuilder errMsg = new StringBuilder("Invalid parents field.");

    // check for multiple version ranges for the same artifact.
    // ex: "parents": [ "etlbatch[1.0.0,2.0.0)", "etlbatch[3.0.0,4.0.0)" ]
    Set<String> parentNames = new HashSet<>();
    // keep track of dupes so that we don't have repeat error messages if there are more than 2 ranges for a name
    Set<String> dupes = new HashSet<>();
    for (ArtifactRange parent : parents) {
      String parentName = parent.getName();
      if (!parentNames.add(parentName) && !dupes.contains(parentName)) {
        errMsg.append(" Only one version range for parent '");
        errMsg.append(parentName);
        errMsg.append("' can be present.");
        dupes.add(parentName);
        isInvalid = true;
      }
      if (artifactId.getName().equals(parentName) && artifactId.getNamespace().equals(parent.getNamespace())) {
        throw new InvalidArtifactException(String.format(
          "Invalid parent '%s' for artifact '%s'. An artifact cannot extend itself.", parent, artifactId));
      }
    }

    // final err message should look something like:
    // "Invalid parents. Only one version range for parent 'etlbatch' can be present."
    if (isInvalid) {
      throw new InvalidArtifactException(errMsg.toString());
    }
  }

  /**
   * Validates the set of plugins for an artifact. Checks that the pair of plugin type and name are unique among
   * all plugins in an artifact.
   *
   * @param plugins the set of plugins to validate
   * @throws InvalidArtifactException if there is more than one class with the same type and name
   */
  @VisibleForTesting
  static void validatePluginSet(Set<PluginClass> plugins) throws InvalidArtifactException {
    boolean isInvalid = false;
    StringBuilder errMsg = new StringBuilder("Invalid plugins field.");
    Set<ImmutablePair<String, String>> existingPlugins = new HashSet<>();
    Set<ImmutablePair<String, String>> dupes = new HashSet<>();
    for (PluginClass plugin : plugins) {
      ImmutablePair<String, String> typeAndName = ImmutablePair.of(plugin.getType(), plugin.getName());
      if (!existingPlugins.add(typeAndName) && !dupes.contains(typeAndName)) {
        errMsg.append(" Only one plugin with type '");
        errMsg.append(typeAndName.getFirst());
        errMsg.append("' and name '");
        errMsg.append(typeAndName.getSecond());
        errMsg.append("' can be present.");
        dupes.add(typeAndName);
        isInvalid = true;
      }
    }

    // final err message should look something like:
    // "Invalid plugins. Only one plugin with type 'source' and name 'table' can be present."
    if (isInvalid) {
      throw new InvalidArtifactException(errMsg.toString());
    }
  }

  private void writeSystemMetadata(co.cask.cdap.proto.id.ArtifactId artifactId, ArtifactInfo artifactInfo) {
    // add system metadata for artifacts
    ArtifactSystemMetadataWriter writer = new ArtifactSystemMetadataWriter(metadataStore, artifactId, artifactInfo);
    writer.write();
  }
}
