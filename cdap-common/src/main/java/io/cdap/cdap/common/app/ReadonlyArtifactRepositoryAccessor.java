package io.cdap.cdap.common.app;

import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
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
      java.util.function.Predicate<io.cdap.cdap.proto.id.ArtifactId> pluginPredicate,
      int limit, ArtifactSortOrder order)
      throws Exception;


  /**
   * Get all artifacts in the given namespace of the given name. Will never return null. If no
   * artifacts exist, an exception is thrown. Namespace existence is not checked.
   *
   * @param namespace the namespace to get artifacts from
   * @param name the name of artifacts to get
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable list of artifacts in the given namespace of the given name
   * @throws IOException if there is an exception reading from the meta store
   * @throws ArtifactNotFoundException if no artifacts of the given name in the given namespace
   *     exist
   */
  List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, String name, int limit,
      ArtifactSortOrder order) throws Exception;
}
