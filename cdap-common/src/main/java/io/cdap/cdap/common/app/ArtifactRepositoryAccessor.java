package io.cdap.cdap.common.app;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.id.Id.Namespace;
import java.util.function.Predicate;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.ReadonlyArtifactRepositoryAccessor;
import io.cdap.cdap.common.id.Id.Artifact;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDescriptor;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDetail;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtifactRepositoryAccessor implements ReadonlyArtifactRepositoryAccessor {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactRepositoryAccessor.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private final List<ArtifactDetail> allArtifacts;
  private final SortedMap<ArtifactDescriptor, PluginClass> allSystemPlugins;
  private final SortedMap<ArtifactDescriptor, PluginClass> allUserPlugins;


  public ArtifactRepositoryAccessor(
      List<ArtifactDetail> allSystemArtifacts,
      List<ArtifactDetail> allUserArtifacts,
      SortedMap<ArtifactDescriptor, PluginClass> allSystemPlugins,
      SortedMap<ArtifactDescriptor, PluginClass> allUserPlugins) {
    this.allArtifacts = Stream.concat(allSystemArtifacts.stream(), allUserArtifacts.stream()).collect(
        Collectors.toList());
    this.allSystemPlugins = allSystemPlugins;
    this.allUserPlugins = allUserPlugins;
    Collections.sort(allArtifacts, new Comparator<ArtifactDetail>() {
      @Override
      public int compare(ArtifactDetail o1, ArtifactDetail o2) {
        return o1.getDescriptor().compareTo(o2.getDescriptor());
      }
    });

    //LOG.info(GSON.toJson(allArtifacts));
    //LOG.info(GSON.toJson(allSystemPlugins));
    //LOG.info(GSON.toJson(allUserPlugins));
  }

  @Override
  public SortedMap<ArtifactDescriptor, PluginClass> getPlugins(
      NamespaceId namespace, Id.Artifact artifactId, String pluginType, String pluginName,
      Predicate<io.cdap.cdap.proto.id.ArtifactId> pluginPredicate,
      int limit, ArtifactSortOrder order) throws Exception {

    //LOG.info("[getPlugins] Namespace: "+ namespace.getNamespace());
    //LOG.info("[getPlugins] Namespace Entity Name: "+namespace.getEntityName());
    //LOG.info("HERE --------------");
    //LOG.info("[getPlugins] allUserPlugins: "+ GSON.toJson(allUserPlugins.keySet()));
    //LOG.info("[getPlugins] allSystemPlugins: "+ GSON.toJson(allSystemPlugins.keySet()));


    SortedMap<ArtifactDescriptor, PluginClass> result = order == ArtifactSortOrder.DESC
        ? new TreeMap<>(Collections.reverseOrder()) :
        new TreeMap<>();

    ArtifactDetail parentArtifactDetail = getArtifact(artifactId);

    for(Map.Entry<ArtifactDescriptor, PluginClass> entry: allUserPlugins.entrySet()) {
      ArtifactDescriptor artifactDescriptor = entry.getKey();
      PluginClass pluginClass = entry.getValue();

      if (namespace.getNamespace().equals(artifactDescriptor.getNamespace())
          && parentArtifactDetail.getDescriptor().equals(artifactDescriptor)
          && pluginType.equals(pluginClass.getType())
          && pluginName.equals(pluginClass.getName())
          && pluginPredicate.test(new ArtifactId(
          artifactDescriptor.getNamespace(),
          artifactDescriptor.getArtifactId().getName(),
          artifactDescriptor.getArtifactId().getVersion().getVersion()
      ))) {
        result.put(artifactDescriptor, pluginClass);
      }

      if (limit < result.size()) {
        result.remove(result.lastKey());
      }
    }

    // add matching system artifacts
    for(Map.Entry<ArtifactDescriptor, PluginClass> entry: allSystemPlugins.entrySet()) {
      ArtifactDescriptor artifactDescriptor = entry.getKey();
      PluginClass pluginClass = entry.getValue();

      if (parentArtifactDetail.getDescriptor().equals(artifactDescriptor)
          && pluginType.equals(pluginClass.getType())
          && pluginName.equals(pluginClass.getName())
          && pluginPredicate.test(new ArtifactId(
              artifactDescriptor.getNamespace(),
              artifactDescriptor.getArtifactId().getName(),
              artifactDescriptor.getArtifactId().getVersion().getVersion()
          ))) {
        result.put(artifactDescriptor, pluginClass);
      }

      if (limit < result.size()) {
        result.remove(result.lastKey());
      }
    }

    return result;
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, String name,
      int limit, ArtifactSortOrder order) throws Exception {

    List<ArtifactSummary> artifacts = new ArrayList<>();
    if (order == ArtifactSortOrder.DESC) {
      ListIterator<ArtifactDetail> iterator = allArtifacts.listIterator(allArtifacts.size());
      while(iterator.hasPrevious()) {
        ArtifactDetail artifact = iterator.previous();
        if (artifact.getDescriptor().getNamespace().equals(namespace.getNamespace())
            && artifact.getDescriptor().getArtifactId().getName().equals(name)
            && artifacts.size() < limit
        ) {
          artifacts.add(new ArtifactSummary(
              artifact.getDescriptor().getArtifactId().getName(),
              artifact.getDescriptor().getArtifactId().getVersion().getVersion(),
              artifact.getDescriptor().getArtifactId().getScope()
          ));
        }
      }
      return artifacts;
    }

    for (ArtifactDetail artifact: allArtifacts) {
      if (artifact.getDescriptor().getNamespace().equals(namespace.getNamespace())
          && artifact.getDescriptor().getArtifactId().getName().equals(name)
          && artifacts.size() < limit
      ){
        artifacts.add(new ArtifactSummary(
            artifact.getDescriptor().getArtifactId().getName(),
            artifact.getDescriptor().getArtifactId().getVersion().getVersion(),
            artifact.getDescriptor().getArtifactId().getScope()
        ));
      }
    }
    return artifacts;
  }

  @Override
  public ArtifactDetail getArtifact(Artifact artifactId) throws ArtifactNotFoundException {
    String namespace = artifactId.getNamespace().getId();

    LOG.info("[getArtifact] Namespace: "+ namespace);

    String name = artifactId.getName();
    ArtifactVersion version = artifactId.getVersion();

    for (ArtifactDetail artifact: allArtifacts) {
      if (artifact.getDescriptor().getNamespace().equals(namespace)
        && artifact.getDescriptor().getArtifactId().getName().equals(name)
        && artifact.getDescriptor().getArtifactId().getVersion().equals(version))
        return artifact;
    }

    throw new ArtifactNotFoundException(namespace, name);
  }

  @Override
  public InputStream newInputStream(Artifact artifactId) throws IOException, NotFoundException {
    try {
      return getArtifact(artifactId).getDescriptor().getLocation().getInputStream();
    } catch (ArtifactNotFoundException e) {
      throw new NotFoundException(e);
    }
  }

  @Override
  public List<ArtifactDetail> getArtifactDetails(ArtifactRange range, int limit,
      ArtifactSortOrder order) throws Exception {

    LOG.info("[getArtifactDetails] Namespace: "+ range.getNamespace());

    List<ArtifactDetail> artifacts = new ArrayList<>();
    if (order == ArtifactSortOrder.DESC) {
      ListIterator<ArtifactDetail> iterator = allArtifacts.listIterator(allArtifacts.size());
      while(iterator.hasPrevious()) {
        ArtifactDetail artifact = iterator.previous();
        if (artifact.getDescriptor().getNamespace().equals(range.getNamespace())
            && artifact.getDescriptor().getArtifactId().getName().equals(range.getName())
            && range.versionIsInRange(artifact.getDescriptor().getArtifactId().getVersion())
            && artifacts.size() < limit
        ) {
          artifacts.add(artifact);
        }
      }
      return artifacts;
    }

    for (ArtifactDetail artifact: allArtifacts) {
      if (artifact.getDescriptor().getNamespace().equals(range.getNamespace())
        && artifact.getDescriptor().getArtifactId().getName().equals(range.getName())
        && range.versionIsInRange(artifact.getDescriptor().getArtifactId().getVersion())
          && artifacts.size() < limit
      ){
        artifacts.add(artifact);
      }
    }
    return artifacts;
  }
}
