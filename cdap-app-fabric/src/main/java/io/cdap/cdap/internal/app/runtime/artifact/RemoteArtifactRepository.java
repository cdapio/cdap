/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.ArtifactConfigReader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.app.spark.SparkCompatReader;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import javax.annotation.Nullable;

public class RemoteArtifactRepository implements ArtifactRepository {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteArtifactRepository.class);
  private final ArtifactRepositoryReader artifactRepositoryReader;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final ArtifactInspector artifactInspector;
  private final Set<File> systemArtifactDirs;
  private final ArtifactConfigReader configReader;
  private final MetadataServiceClient metadataServiceClient;
  private final Impersonator impersonator;

  @VisibleForTesting
  @Inject
  public RemoteArtifactRepository(CConfiguration cConf, ArtifactRepositoryReader artifactRepositoryReader,
                                   MetadataServiceClient metadataServiceClient,
                                   ProgramRunnerFactory programRunnerFactory,
                                   Impersonator impersonator) {
    this.artifactRepositoryReader = artifactRepositoryReader;
    this.artifactClassLoaderFactory = new ArtifactClassLoaderFactory(cConf, programRunnerFactory);
    this.artifactInspector = new ArtifactInspector(cConf, artifactClassLoaderFactory);
    this.systemArtifactDirs = new HashSet<>();
    String systemArtifactsDir = cConf.get(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR);
    if (!Strings.isNullOrEmpty(systemArtifactsDir)) {
      String sparkDirStr = SparkCompatReader.get(cConf).getCompat();

      for (String dir : systemArtifactsDir.split(";")) {
        File file = new File(dir);
        if (!file.isDirectory()) {
          LOG.warn("Ignoring {} because it is not a directory.", file);
          continue;
        }
        systemArtifactDirs.add(file);
        // Also look in the relevant spark compat directory for spark version specific artifacts.
        File sparkDir = new File(file, sparkDirStr);
        if (file.isDirectory()) {
          systemArtifactDirs.add(sparkDir);
        }
      }
    }
    this.configReader = new ArtifactConfigReader();
    this.metadataServiceClient = metadataServiceClient;
    this.impersonator = impersonator;
  }

  @Override
  public CloseableClassLoader createArtifactClassLoader(Location artifactLocation,
                                                        EntityImpersonator entityImpersonator) throws IOException {
      return artifactClassLoaderFactory.createClassLoader(ImmutableList.of(artifactLocation).iterator(),
                                                          entityImpersonator);
  }

  @Override
  public void clear(NamespaceId namespace) throws Exception {

  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, boolean includeSystem) throws Exception {
    return null;
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, String name, int limit,
                                                    ArtifactSortOrder order) throws Exception {
    return null;
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(ArtifactRange range, int limit,
                                                    ArtifactSortOrder order) throws Exception {
    return null;
  }

  @Override
  public List<ApplicationClassSummary> getApplicationClasses(NamespaceId namespace,
                                                             boolean includeSystem) throws IOException {
    return null;
  }

  @Override
  public List<ApplicationClassInfo> getApplicationClasses(NamespaceId namespace, String className) throws IOException {
    return null;
  }

  @Override
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(NamespaceId namespace,
                                                                    Id.Artifact artifactId) throws IOException, ArtifactNotFoundException {
    return null;
  }

  @Override
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(NamespaceId namespace, Id.Artifact artifactId,
                                                                    String pluginType) throws IOException, ArtifactNotFoundException {
    return null;
  }

  @Override
  public SortedMap<ArtifactDescriptor, PluginClass> getPlugins(NamespaceId namespace, Id.Artifact artifactId,
                                                               String pluginType, String pluginName,
                                                               Predicate<ArtifactId> pluginPredicate, int limit,
                                                               ArtifactSortOrder order) throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    return null;
  }

  @Override
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(NamespaceId namespace, ArtifactRange artifactRange,
                                                               String pluginType, String pluginName,
                                                               PluginSelector selector) throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    return null;
  }

  @Override
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile) throws Exception {
    return null;
  }

  @Override
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts,
                                    @Nullable Set<PluginClass> additionalPlugins) throws Exception {
    return null;
  }

  @Override
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts,
                                    @Nullable Set<PluginClass> additionalPlugins,
                                    Map<String, String> properties) throws Exception {
    return null;
  }

  @Override
  public void writeArtifactProperties(Id.Artifact artifactId, Map<String, String> properties) throws Exception {

  }

  @Override
  public void writeArtifactProperty(Id.Artifact artifactId, String key, String value) throws Exception {

  }

  @Override
  public void deleteArtifactProperty(Id.Artifact artifactId, String key) throws Exception {

  }

  @Override
  public void deleteArtifactProperties(Id.Artifact artifactId) throws Exception {

  }

  @Override
  public void addSystemArtifacts() throws Exception {

  }

  @Override
  public void deleteArtifact(Id.Artifact artifactId) throws Exception {

  }

  @Override
  public List<ArtifactInfo> getArtifactsInfo(NamespaceId namespace) throws Exception {
    return null;
  }

  @Override
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    return null;
  }

  @Override
  public List<ArtifactDetail> getArtifactDetails(ArtifactRange range, int limit,
                                                 ArtifactSortOrder order) throws Exception {
    return null;
  }
}
