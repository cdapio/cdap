package io.cdap.cdap.common.app;

import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ArtifactRepositoryReader;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDescriptor;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public interface ReadonlyArtifactRepositoryAccessor extends ArtifactRepositoryReader {
  /**
   * Create a classloader that uses the artifact specified by {@link ArtifactDescriptor} to load
   * classes, with access to packages that all program type has access to.
   *
   */
  CloseableClassLoader createArtifactClassLoader(ArtifactDescriptor artifactDescriptor,
      io.cdap.cdap.proto.id.ArtifactId artifactId) throws IOException;

  /**
   * Get all application classes in the given namespace, optionally including classes from system
   * artifacts as well. Will never return null. If no artifacts exist, an empty list is returned.
   * Namespace existence is not checked.
   *
   * @param namespace the namespace to get application classes from
   * @param includeSystem whether classes from system artifacts should be included in the
   *     results
   * @return an unmodifiable list of application classes that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  List<ApplicationClassSummary> getApplicationClasses(NamespaceId namespace,
      boolean includeSystem) throws IOException;

  /**
   * Get all application classes in the given namespace of the given class name. Will never return
   * null. If no artifacts exist, an empty list is returned. Namespace existence is not checked.
   *
   * @param namespace the namespace to get application classes from
   * @param className the application class to get
   * @return an unmodifiable list of application classes that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  List<ApplicationClassInfo> getApplicationClasses(NamespaceId namespace, String className)
      throws IOException;


  /**
   * Returns a {@link SortedMap} of plugin artifact to all plugins available for the given artifact.
   * The keys are sorted by the {@link ArtifactDescriptor} for the artifact that contains plugins
   * available to the given artifact.
   *
   * @param namespace the namespace to get plugins from
   * @param artifactId the id of the artifact to get plugins for
   * @return an unmodifiable sorted map from plugin artifact to plugins in that artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception reading plugin metadata from the artifact
   *     store
   */
  SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(
      NamespaceId namespace, Id.Artifact artifactId) throws IOException, ArtifactNotFoundException;

  /**
   * Returns a {@link SortedMap} of plugin artifact to all plugins of the given type available for
   * the given artifact. The keys are sorted by the {@link ArtifactDescriptor} for the artifact that
   * contains plugins available to the given artifact.
   *
   * @param namespace the namespace to get plugins from
   * @param artifactId the id of the artifact to get plugins for
   * @param pluginType the type of plugins to get
   * @return an unmodifiable sorted map from plugin artifact to plugins in that artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception reading plugin metadata from the artifact
   *     store
   */
  SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(
      NamespaceId namespace, Id.Artifact artifactId, String pluginType)
      throws IOException, ArtifactNotFoundException;

  /**
   * Returns a {@link SortedMap} of plugin artifact to plugin available for the given artifact. The
   * keys are sorted by the {@link ArtifactDescriptor} for the artifact that contains plugins
   * available to the given artifact.
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
   * @throws IOException if there was an exception reading plugin metadata from the artifact
   *     store
   */
  SortedMap<ArtifactDescriptor, PluginClass> getPlugins(
      NamespaceId namespace, Id.Artifact artifactId, String pluginType, String pluginName,
      com.google.common.base.Predicate<io.cdap.cdap.proto.id.ArtifactId> pluginPredicate,
      int limit, ArtifactSortOrder order)
      throws Exception;

  /**
   * Returns a {@link Map.Entry} representing the plugin information for the plugin being
   * requested.
   *
   * @param namespace the namespace to get plugins from
   * @param artifactRange the artifact range to get plugins for
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param selector for selecting which plugin to use
   * @return the entry found
   * @throws IOException if there was an exception reading plugin metadata from the artifact
   *     store
   * @throws ArtifactNotFoundException if the given artifact does not exist
   *
   */
  Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(
      NamespaceId namespace, ArtifactRange artifactRange, String pluginType, String pluginName,
      PluginSelector selector)
      throws Exception;

  /**
   * return list of {@link ArtifactInfo} in the namespace
   *
   * @return list of {@link ArtifactInfo}
   */
  List<ArtifactInfo> getArtifactsInfo(NamespaceId namespace) throws Exception;
}
