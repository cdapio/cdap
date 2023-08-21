package io.cdap.cdap.internal.app.services;

import com.google.common.base.Predicate;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.ReadonlyArtifactRepositoryAccessor;
import io.cdap.cdap.common.id.Id.Artifact;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDescriptor;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDetail;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

public class ArtifactRepositoryAccessor implements ReadonlyArtifactRepositoryAccessor {
  private ArtifactRepository artifactRepository;
  private Impersonator impersonator;

  public ArtifactRepositoryAccessor(ArtifactRepository artifactRepository, Impersonator impersonator) {
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;
  }

  @Override
  public CloseableClassLoader createArtifactClassLoader(ArtifactDescriptor artifactDescriptor,
      io.cdap.cdap.proto.id.ArtifactId artifactId)
      throws IOException {
    EntityImpersonator classLoaderImpersonator = new EntityImpersonator(artifactId,
        impersonator);
    return artifactRepository.createArtifactClassLoader(artifactDescriptor, classLoaderImpersonator);
  }

  @Override
  public List<ApplicationClassSummary> getApplicationClasses(NamespaceId namespace,
      boolean includeSystem) throws IOException {
    return artifactRepository.getApplicationClasses(namespace, includeSystem);
  }

  @Override
  public List<ApplicationClassInfo> getApplicationClasses(NamespaceId namespace,
      String className) throws IOException {
    return artifactRepository.getApplicationClasses(namespace, className);
  }

  @Override
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(NamespaceId namespace,
      Artifact artifactId) throws IOException, ArtifactNotFoundException {
    return artifactRepository.getPlugins(namespace, artifactId);
  }

  @Override
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(NamespaceId namespace,
      Artifact artifactId, String pluginType) throws IOException, ArtifactNotFoundException {
    return artifactRepository.getPlugins(namespace, artifactId, pluginType);
  }

  @Override
  public SortedMap<ArtifactDescriptor, PluginClass> getPlugins(NamespaceId namespace,
      Artifact artifactId, String pluginType, String pluginName,
      Predicate<ArtifactId> pluginPredicate, int limit, ArtifactSortOrder order)
      throws Exception {
    return artifactRepository.getPlugins(namespace, artifactId, pluginType, pluginName, pluginPredicate, limit, order);
  }

  @Override
  public Entry<ArtifactDescriptor, PluginClass> findPlugin(NamespaceId namespace,
      ArtifactRange artifactRange, String pluginType, String pluginName,
      PluginSelector selector) throws Exception {
    return artifactRepository.findPlugin(namespace, artifactRange, pluginType, pluginName, selector);
  }

  @Override
  public List<ArtifactInfo> getArtifactsInfo(NamespaceId namespace) throws Exception {
    return artifactRepository.getArtifactsInfo(namespace);
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace,
      boolean includeSystem) throws Exception {
    return artifactRepository.getArtifactSummaries(namespace, includeSystem);
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, String name,
      int limit, ArtifactSortOrder order) throws Exception {
    return artifactRepository.getArtifactSummaries(namespace, name, limit, order);
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(ArtifactRange range, int limit,
      ArtifactSortOrder order) throws Exception {
    return artifactRepository.getArtifactSummaries(range, limit, order);
  }

  @Override
  public ArtifactDetail getArtifact(Artifact artifactId) throws Exception {
    return artifactRepository.getArtifact(artifactId);
  }

  @Override
  public InputStream newInputStream(Artifact artifactId) throws IOException, NotFoundException {
    return artifactRepository.newInputStream(artifactId);
  }

  @Override
  public List<ArtifactDetail> getArtifactDetails(ArtifactRange range, int limit,
      ArtifactSortOrder order) throws Exception {
    return artifactRepository.getArtifactDetails(range, limit, order);
  }
}
