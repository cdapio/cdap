/*
 * Copyright © 2015-2018 Cask Data, Inc.
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
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.common.ArtifactAlreadyExistsException;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.ArtifactRangeNotFoundException;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.proto.artifact.ApplicationClassInfo;
import co.cask.cdap.proto.artifact.ApplicationClassSummary;
import co.cask.cdap.proto.artifact.ArtifactSortOrder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.security.impersonation.EntityImpersonator;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.annotations.VisibleForTesting;
import org.apache.twill.filesystem.Location;

import java.io.File;
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
public interface ArtifactRepository {

  /**
   * Create a classloader that uses the artifact at the specified location to load classes, with access to
   * packages that all program type has access to.
   * It delegates to {@link ArtifactClassLoaderFactory#createClassLoader(Location, EntityImpersonator)}.
   *
   * @see ArtifactClassLoaderFactory
   */
  CloseableClassLoader createArtifactClassLoader(Location artifactLocation,
                                                 EntityImpersonator entityImpersonator) throws IOException;

  /**
   * Clear all artifacts in the given namespace. This method is only intended to be called by unit tests, and
   * when a namespace is being deleted.
   *
   * @param namespace the namespace to delete artifacts in.
   * @throws IOException if there was an error making changes in the meta store
   */
  void clear(NamespaceId namespace) throws Exception;

  /**
   * Get all artifacts in the given namespace, optionally including system artifacts as well. Will never return
   * null. If no artifacts exist, an empty list is returned. Namespace existence is not checked.
   *
   * @param namespace the namespace to get artifacts from
   * @param includeSystem whether system artifacts should be included in the results
   * @return an unmodifiable list of artifacts that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, boolean includeSystem) throws Exception;

  /**
   * Get all artifacts in the given namespace of the given name. Will never return null.
   * If no artifacts exist, an exception is thrown. Namespace existence is not checked.
   *
   * @param namespace the namespace to get artifacts from
   * @param name the name of artifacts to get
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable list of artifacts in the given namespace of the given name
   * @throws IOException if there as an exception reading from the meta store
   * @throws ArtifactNotFoundException if no artifacts of the given name in the given namespace exist
   */
  List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, String name, int limit,
                                             ArtifactSortOrder order) throws Exception;

  /**
   * Get all artifacts in the given artifact range. Will never return null.
   *
   * @param range the range of the artifact
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable list of artifacts in the given namespace of the given name
   * @throws IOException if there as an exception reading from the meta store
   */
  List<ArtifactSummary> getArtifactSummaries(ArtifactRange range, int limit,
                                             ArtifactSortOrder order) throws Exception;

  /**
   * Get details about the given artifact. Will never return null.
   * If no such artifact exist, an exception is thrown. Namespace existence is not checked.
   *
   * @param artifactId the id of the artifact to get
   * @return details about the given artifact
   * @throws IOException if there as an exception reading from the meta store
   * @throws ArtifactNotFoundException if the given artifact does not exist
   */
  ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception;

  /**
   * Get all artifact details that match artifacts in the given ranges.
   *
   * @param range the range to match artifacts in
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable list of all artifacts that match the given ranges. If none exist, an empty list is returned
   */
  List<ArtifactDetail> getArtifactDetails(ArtifactRange range, int limit, ArtifactSortOrder order) throws Exception;

  /**
   * Get all application classes in the given namespace, optionally including classes from system artifacts as well.
   * Will never return null. If no artifacts exist, an empty list is returned. Namespace existence is not checked.
   *
   * @param namespace the namespace to get application classes from
   * @param includeSystem whether classes from system artifacts should be included in the results
   * @return an unmodifiable list of application classes that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  List<ApplicationClassSummary> getApplicationClasses(NamespaceId namespace,
                                                      boolean includeSystem) throws IOException;

  /**
   * Get all application classes in the given namespace of the given class name.
   * Will never return null. If no artifacts exist, an empty list is returned. Namespace existence is not checked.
   *
   * @param namespace the namespace to get application classes from
   * @param className the application class to get
   * @return an unmodifiable list of application classes that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  List<ApplicationClassInfo> getApplicationClasses(NamespaceId namespace, String className) throws IOException;


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
  SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(
    NamespaceId namespace, Id.Artifact artifactId) throws IOException, ArtifactNotFoundException;

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
  SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(
    NamespaceId namespace, Id.Artifact artifactId, String pluginType) throws IOException, ArtifactNotFoundException;

  /**
   * Returns a {@link SortedMap} of plugin artifact to plugin available for the given artifact. The keys
   * are sorted by the {@link ArtifactDescriptor} for the artifact that contains plugins available to the given
   * artifact.
   *
   * @param namespace the namespace to get plugins from
   * @param artifactId the id of the artifact to get plugins for
   * @param pluginType the type of plugins to get
   * @param pluginName the name of plugins to get
   * @param pluginPredicate the predicate for the plugin
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable sorted map from plugin artifact to plugins in that artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   */
  SortedMap<ArtifactDescriptor, PluginClass> getPlugins(
    NamespaceId namespace, Id.Artifact artifactId, String pluginType, String pluginName,
    com.google.common.base.Predicate<co.cask.cdap.proto.id.ArtifactId> pluginPredicate,
    int limit, ArtifactSortOrder order) throws IOException, PluginNotExistsException, ArtifactNotFoundException;

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
  Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(
    NamespaceId namespace, ArtifactRange artifactRange, String pluginType, String pluginName,
    PluginSelector selector) throws IOException, PluginNotExistsException, ArtifactNotFoundException;

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
  ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile) throws Exception;

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
  ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                             @Nullable Set<ArtifactRange> parentArtifacts,
                             @Nullable Set<PluginClass> additionalPlugins) throws Exception;

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
  ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                             @Nullable Set<ArtifactRange> parentArtifacts,
                             @Nullable Set<PluginClass> additionalPlugins,
                             Map<String, String> properties) throws Exception;

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
  void writeArtifactProperties(Id.Artifact artifactId, Map<String, String> properties) throws Exception;

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
  void writeArtifactProperty(Id.Artifact artifactId, String key, String value) throws Exception;

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
  void deleteArtifactProperty(Id.Artifact artifactId, String key) throws Exception;

  /**
   * Deletes all properties for an artifact. If no properties exist, this will be a no-op.
   *
   * @param artifactId the id of the artifact to delete properties from
   * @throws IOException if there was an exception writing to the artifact store
   * @throws ArtifactNotFoundException if the artifact does not exist
   * @throws UnauthorizedException if the current user is not permitted to remove properties from the artifact. To be
   *                               able to remove properties, users must have admin privileges on the artifact
   */
  void deleteArtifactProperties(Id.Artifact artifactId) throws Exception;

  /**
   * Scan all files in the local system artifact directory, looking for jar files and adding them as system artifacts.
   * If the artifact already exists it will not be added again unless it is a snapshot version.
   *
   * @throws IOException if there was some IO error adding the system artifacts
   */
  void addSystemArtifacts() throws Exception;

  /**
   * Delete the specified artifact. Programs that use the artifact will not be able to start.
   *
   * @param artifactId the artifact to delete
   * @throws IOException if there was some IO error deleting the artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws UnauthorizedException if the current user is not authorized to delete the artifact. To delete an artifact,
   *                               a user needs {@link Action#ADMIN} permission on the artifact.
   */
  void deleteArtifact(Id.Artifact artifactId) throws Exception;

  /**
   * return list of {@link ArtifactInfo} in the namespace
   * @param namespace
   * @return list of {@link ArtifactInfo}
   * @throws Exception
   */
  List<ArtifactInfo> getArtifactsInfo(NamespaceId namespace) throws Exception;
}
