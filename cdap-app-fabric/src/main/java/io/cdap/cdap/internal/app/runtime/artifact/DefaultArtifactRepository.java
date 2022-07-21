/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ArtifactRangeNotFoundException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.ArtifactConfig;
import io.cdap.cdap.common.conf.ArtifactConfigReader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data2.metadata.system.ArtifactSystemMetadataWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.app.spark.SparkCompatReader;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.PluginId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import org.apache.commons.io.IOUtils;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;

/**
 * Default implementation of the {@link ArtifactRepository}, all the operation does not have authorization enforce
 * involved
 */
public class DefaultArtifactRepository implements ArtifactRepository {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultArtifactRepository.class);
  private final ArtifactStore artifactStore;
  private final ArtifactRepositoryReader artifactRepositoryReader;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final ArtifactInspector artifactInspector;
  private final Set<File> systemArtifactDirs;
  private final ArtifactConfigReader configReader;
  private final MetadataServiceClient metadataServiceClient;
  private final Impersonator impersonator;
  private final int maxArtifactLoadParallelism;

  @VisibleForTesting
  @Inject
  public DefaultArtifactRepository(CConfiguration cConf, ArtifactStore artifactStore,
                                   ArtifactRepositoryReader artifactRepositoryReader,
                                   MetadataServiceClient metadataServiceClient,
                                   ProgramRunnerFactory programRunnerFactory,
                                   Impersonator impersonator) {
    this.artifactStore = artifactStore;
    this.artifactRepositoryReader = artifactRepositoryReader;
    this.artifactClassLoaderFactory = new ArtifactClassLoaderFactory(cConf, programRunnerFactory);
    this.artifactInspector = new DefaultArtifactInspector(cConf, artifactClassLoaderFactory, impersonator);
    this.systemArtifactDirs = new HashSet<>();
    this.maxArtifactLoadParallelism = cConf.getInt(Constants.AppFabric.SYSTEM_ARTIFACTS_MAX_PARALLELISM);
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
  public CloseableClassLoader createArtifactClassLoader(
    ArtifactDescriptor artifactDescriptor, EntityImpersonator entityImpersonator) throws IOException {
    return artifactClassLoaderFactory.createClassLoader(ImmutableList.of(artifactDescriptor.getLocation()).iterator(),
                                                        entityImpersonator);
  }

  @Override
  public void clear(NamespaceId namespace) throws Exception {
    for (ArtifactDetail artifactDetail : artifactStore.getArtifacts(namespace)) {
      deleteArtifact(Id.Artifact.from(Id.Namespace.fromEntityId(namespace),
                                      artifactDetail.getDescriptor().getArtifactId()));
    }
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(final NamespaceId namespace,
                                                    boolean includeSystem) throws Exception {
    List<ArtifactSummary> summaries = new ArrayList<>();
    if (includeSystem) {
      convertAndAdd(summaries, artifactStore.getArtifacts(NamespaceId.SYSTEM));
    }
    return convertAndAdd(summaries, artifactStore.getArtifacts(namespace));
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, String name, int limit,
                                                    ArtifactSortOrder order)
    throws Exception {
    List<ArtifactSummary> summaries = new ArrayList<>();
    return convertAndAdd(summaries, artifactStore.getArtifacts(namespace, name, limit, order));
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(final ArtifactRange range, int limit,
                                                    ArtifactSortOrder order) throws Exception {
    List<ArtifactSummary> summaries = new ArrayList<>();
    return convertAndAdd(summaries, artifactStore.getArtifacts(range, limit, order));
  }

  @Override
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    return artifactRepositoryReader.getArtifact(artifactId);
  }

  @Override
  public InputStream newInputStream(Id.Artifact artifactId) throws IOException, NotFoundException {
    return artifactRepositoryReader.newInputStream(artifactId);
  }

  @Override
  public List<ArtifactDetail> getArtifactDetails(final ArtifactRange range, int limit,
                                                 ArtifactSortOrder order) throws Exception {
    return artifactRepositoryReader.getArtifactDetails(range, limit, order);
  }

  @Override
  public List<ApplicationClassSummary> getApplicationClasses(NamespaceId namespace,
                                                             boolean includeSystem) throws IOException {
    List<ApplicationClassSummary> summaries = Lists.newArrayList();
    if (includeSystem) {
      addAppSummaries(summaries, NamespaceId.SYSTEM);
    }
    addAppSummaries(summaries, namespace);

    return Collections.unmodifiableList(summaries);
  }

  @Override
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

  @Override
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(
    NamespaceId namespace, Id.Artifact artifactId) throws IOException, ArtifactNotFoundException {
    return artifactStore.getPluginClasses(namespace, artifactId);
  }

  @Override
  public SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins(
    NamespaceId namespace, Id.Artifact artifactId, String pluginType) throws IOException, ArtifactNotFoundException {
    return artifactStore.getPluginClasses(namespace, artifactId, pluginType);
  }

  @Override
  public SortedMap<ArtifactDescriptor, PluginClass> getPlugins(
    NamespaceId namespace, Id.Artifact artifactId, String pluginType, String pluginName,
    Predicate<io.cdap.cdap.proto.id.ArtifactId> pluginPredicate,
    int limit, ArtifactSortOrder order) throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    return artifactStore.getPluginClasses(namespace, artifactId, pluginType, pluginName,
                                          pluginPredicate::apply, limit, order);
  }

  @Override
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(NamespaceId namespace, ArtifactRange artifactRange,
                                                               String pluginType, String pluginName,
                                                               PluginSelector selector)
    throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    SortedMap<ArtifactDescriptor, PluginClass> pluginClasses = artifactStore.getPluginClasses(
      namespace, artifactRange, pluginType, pluginName, null, Integer.MAX_VALUE, ArtifactSortOrder.UNORDERED);
    return getPluginEntries(pluginClasses, selector,
                            Id.Namespace.fromEntityId(new NamespaceId(artifactRange.getNamespace())),
                            pluginType, pluginName);
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
                       Collections.emptyMap());
  }

  @Override
  public ArtifactDetail addArtifact(final Id.Artifact artifactId, final File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts,
                                    @Nullable Set<PluginClass> additionalPlugins,
                                    Map<String, String> properties) throws Exception {
    if (additionalPlugins != null) {
      validatePluginSet(additionalPlugins);
    }

    parentArtifacts = parentArtifacts == null ? Collections.emptySet() : parentArtifacts;
    CloseableClassLoader parentClassLoader = null;
    EntityImpersonator entityImpersonator = new EntityImpersonator(artifactId.toEntityId(),
                                                                   impersonator);
    List<ArtifactDescriptor> parentDescriptors = new ArrayList<>();
    if (!parentArtifacts.isEmpty()) {
      validateParentSet(artifactId, parentArtifacts);
      parentDescriptors = getParentArtifactDescriptors(artifactId, parentArtifacts);
    }

    additionalPlugins = additionalPlugins == null ? Collections.emptySet() : additionalPlugins;
    ArtifactClassesWithMetadata artifactClassesWithMetadata = inspectArtifact(artifactId, artifactFile,
                                                                              parentDescriptors,
                                                                              additionalPlugins);
    ArtifactMeta meta = new ArtifactMeta(artifactClassesWithMetadata.getArtifactClasses(), parentArtifacts,
                                         properties);
    ArtifactDetail artifactDetail = artifactStore.write(artifactId, meta, artifactFile, entityImpersonator);
    ArtifactDescriptor descriptor = artifactDetail.getDescriptor();
    // info hides some fields that are available in detail, such as the location of the artifact
    ArtifactInfo artifactInfo = new ArtifactInfo(descriptor.getArtifactId(), artifactDetail.getMeta().getClasses(),
                                                 artifactDetail.getMeta().getProperties());
    // add system metadata for artifacts
    writeSystemMetadata(artifactId.toEntityId(), artifactInfo);

    // add plugin metadata, these metadata can be in any scope depending on the artifact scope
    metadataServiceClient.batch(artifactClassesWithMetadata.getMutations());
    return artifactDetail;
  }

  @Override
  public void writeArtifactProperties(Id.Artifact artifactId, final Map<String, String> properties) throws Exception {
    artifactStore.updateArtifactProperties(artifactId, oldProperties -> properties);
  }

  @Override
  public void writeArtifactProperty(Id.Artifact artifactId, final String key, final String value) throws Exception {
    artifactStore.updateArtifactProperties(artifactId, oldProperties -> {
      Map<String, String> updated = new HashMap<>();
      updated.putAll(oldProperties);
      updated.put(key, value);
      return updated;
    });
  }

  @Override
  public void deleteArtifactProperty(Id.Artifact artifactId, final String key) throws Exception {
    artifactStore.updateArtifactProperties(artifactId, oldProperties -> {
      if (!oldProperties.containsKey(key)) {
        return oldProperties;
      }
      Map<String, String> updated = new HashMap<>();
      updated.putAll(oldProperties);
      updated.remove(key);
      return updated;
    });
  }

  @Override
  public void deleteArtifactProperties(Id.Artifact artifactId) throws Exception {
    artifactStore.updateArtifactProperties(artifactId, oldProperties -> new HashMap<>());
  }

  @Override
  public void addSystemArtifacts() throws Exception {
    // scan the directory for artifact .jar files and config files for those artifacts
    Map<Id.Artifact, SystemArtifactInfo> systemArtifacts = new HashMap<>();
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
          systemArtifacts.put(artifactId, new SystemArtifactInfo(artifactId, jarFile, artifactConfig));
        } catch (InvalidArtifactException e) {
          LOG.warn(String.format("Could not add system artifact '%s' because it is invalid.", artifactFileName), e);
        }
      }
    }

    // child -> parents
    Multimap<Id.Artifact, Id.Artifact> childToParents = HashMultimap.create();
    // parent -> children
    Multimap<Id.Artifact, Id.Artifact> parentToChildren = HashMultimap.create();
    Set<Id.Artifact> remainingArtifacts = new HashSet<>();
    // build mapping from child to parents and from parents to children
    for (SystemArtifactInfo child : systemArtifacts.values()) {
      Id.Artifact childId = child.getArtifactId();
      remainingArtifacts.add(childId);

      for (SystemArtifactInfo potentialParent : systemArtifacts.values()) {
        Id.Artifact potentialParentId = potentialParent.getArtifactId();
        // skip if we're looking at ourselves
        if (childId.equals(potentialParentId)) {
          continue;
        }

        if (child.getConfig().hasParent(potentialParentId)) {
          childToParents.put(childId, potentialParentId);
          parentToChildren.put(potentialParentId, childId);
        }
      }
    }

    if (!remainingArtifacts.isEmpty()) {
      ExecutorService executorService =
        Executors.newFixedThreadPool(Math.min(maxArtifactLoadParallelism, remainingArtifacts.size()),
                                     Threads.createDaemonThreadFactory("system-artifact-loader-%d"));
      try {
        // loop until there is no change
        boolean artifactsAdded = true;
        while (!remainingArtifacts.isEmpty() && artifactsAdded) {
          artifactsAdded = loadSystemArtifacts(executorService, systemArtifacts, remainingArtifacts, parentToChildren,
                                               childToParents);
        }
      } finally {
        executorService.shutdownNow();
      }

      if (!remainingArtifacts.isEmpty()) {
        LOG.warn("Unable to add system artifacts {} due to cyclic dependencies",
                 Joiner.on(",").join(remainingArtifacts));
      }
    }
  }

  /**
   * Add as many system artifacts as possible in parallel. Returns true if at least one artifact was added and there
   * were no errors.
   */
  private boolean loadSystemArtifacts(ExecutorService executorService,
                                      Map<Id.Artifact, SystemArtifactInfo> systemArtifacts,
                                      Set<Id.Artifact> remainingArtifacts,
                                      Multimap<Id.Artifact, Id.Artifact> parentToChildren,
                                      Multimap<Id.Artifact, Id.Artifact> childToParents) throws Exception {

    // add all artifacts that don't have any more parents
    Set<Id.Artifact> addedArtifacts = new HashSet<>();
    List<Future<Id.Artifact>> futures = new ArrayList<>();
    for (Id.Artifact remainingArtifact : remainingArtifacts) {
      if (!childToParents.containsKey(remainingArtifact)) {
        futures.add(executorService.submit(() -> {
          addSystemArtifact(systemArtifacts.get(remainingArtifact));
          return remainingArtifact;
        }));
      }
    }

    Exception failure = null;
    for (Future<Id.Artifact> f : futures) {
      try {
        Id.Artifact addedArtifact = f.get();
        addedArtifacts.add(addedArtifact);
        for (Id.Artifact child : parentToChildren.get(addedArtifact)) {
          childToParents.remove(child, addedArtifact);
        }
        remainingArtifacts.removeAll(addedArtifacts);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (failure != null) {
          failure.addSuppressed(cause);
        } else if (cause instanceof Exception) {
          failure = (Exception) cause;
        } else {
          throw e;
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
    return !futures.isEmpty();
  }

  @Override
  public void deleteArtifact(Id.Artifact artifactId) throws Exception {
    ArtifactDetail artifactDetail = artifactStore.getArtifact(artifactId);
    io.cdap.cdap.proto.id.ArtifactId artifact = artifactId.toEntityId();

    // delete the artifact first and then privileges. Not the other way to avoid orphan artifact
    // which does not have any privilege if the artifact delete from store fails. see CDAP-6648
    artifactStore.delete(artifactId);

    List<MetadataMutation> mutations = new ArrayList<>();
    // drop artifact metadata
    mutations.add(new MetadataMutation.Drop(artifact.toMetadataEntity()));
    Set<PluginClass> plugins = artifactDetail.getMeta().getClasses().getPlugins();

    // drop plugin metadata
    plugins.forEach(pluginClass -> {
      PluginId pluginId = new PluginId(artifact.getNamespace(), artifact.getArtifact(), artifact.getVersion(),
                                       pluginClass.getName(), pluginClass.getType());
      mutations.add(new MetadataMutation.Drop(pluginId.toMetadataEntity()));
    });
    metadataServiceClient.batch(mutations);
  }

  @Override
  public List<ArtifactInfo> getArtifactsInfo(NamespaceId namespace) throws Exception {
    final List<ArtifactDetail> artifactDetails = artifactStore.getArtifacts(namespace);

    return Lists.transform(artifactDetails, new Function<ArtifactDetail, ArtifactInfo>() {
      @Nullable
      @Override
      public ArtifactInfo apply(@Nullable ArtifactDetail input) {
        // transform artifactDetail to artifactInfo
        ArtifactId artifactId = input.getDescriptor().getArtifactId();
        return new ArtifactInfo(artifactId.getName(), artifactId.getVersion().getVersion(), artifactId.getScope(),
                                input.getMeta().getClasses(), input.getMeta().getProperties(),
                                input.getMeta().getUsableBy());
      }
    });
  }

  private void addSystemArtifact(SystemArtifactInfo systemArtifactInfo) throws Exception {
    String fileName = systemArtifactInfo.getArtifactFile().getName();
    try {
      Id.Artifact artifactId = systemArtifactInfo.getArtifactId();

      // Check if it already exists
      try {
        ArtifactDetail currentArtifactDetail = artifactStore.getArtifact(artifactId);
        if (!shouldUpdateSytemArtifact(currentArtifactDetail, systemArtifactInfo)) {
          LOG.info("Artifact {} already exists and it did not change, will not try loading it again.", artifactId);
          return;
        }
      } catch (ArtifactNotFoundException e) {
        // this is fine, means it doesn't exist yet and we should add it
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

  private boolean shouldUpdateSytemArtifact(ArtifactDetail currentArtifactDetail,
                                            SystemArtifactInfo systemArtifactInfo) {
    if (!currentArtifactDetail.getDescriptor().getArtifactId().getVersion().isSnapshot()) {
      // if it's not a snapshot, don't bother trying to update it since artifacts are immutable
      return false;
    }
    // For snapshots check if it's different. Artifact update is disruptive, so we spend some cycles
    // to check if it's really needed
    Set<ArtifactRange> parents = systemArtifactInfo.getConfig().getParents();
    if (!Objects.equals(parents, currentArtifactDetail.getMeta().getUsableBy())) {
      return true;
    }
    if (!Objects.equals(systemArtifactInfo.getConfig().getProperties(),
                       currentArtifactDetail.getMeta().getProperties())) {
      return true;
    }
    Set<PluginClass> additionalPlugins = systemArtifactInfo.getConfig().getPlugins();
    if (additionalPlugins != null && !currentArtifactDetail.getMeta().getClasses().getPlugins().containsAll(
      additionalPlugins)) {
      return true;
    }
    try (
      InputStream stream1 = currentArtifactDetail.getDescriptor().getLocation().getInputStream();
      InputStream stream2 = new FileInputStream(systemArtifactInfo.getArtifactFile())
      ) {
      return !IOUtils.contentEquals(stream1, stream2);
    } catch (IOException e) {
      // In case of any IO problems, jsut update it
      return true;
    }
  }

  private ArtifactClassesWithMetadata inspectArtifact(Id.Artifact artifactId, File artifactFile,
                                                      List<ArtifactDescriptor> parentDescriptors,
                                                      Set<PluginClass> additionalPlugins)
    throws IOException, InvalidArtifactException {
    ArtifactClassesWithMetadata artifact = artifactInspector.inspectArtifact(artifactId, artifactFile,
                                                                             parentDescriptors,
                                                                             additionalPlugins);
    validatePluginSet(artifact.getArtifactClasses().getPlugins());
    if (additionalPlugins == null || additionalPlugins.isEmpty()) {
      return artifact;
    } else {
      ArtifactClasses newArtifactClasses = ArtifactClasses.builder()
        .addApps(artifact.getArtifactClasses().getApps())
        .addPlugins(artifact.getArtifactClasses().getPlugins())
        .addPlugins(additionalPlugins)
        .build();
      return new ArtifactClassesWithMetadata(newArtifactClasses, artifact.getMutations());
    }
  }

  private Map.Entry<ArtifactDescriptor, PluginClass> getPluginEntries(
    Map<ArtifactDescriptor, PluginClass> pluginClasses, PluginSelector selector, Id.Namespace namespace,
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

  // convert details to summaries (to hide location and other unnecessary information)
  private List<ArtifactSummary> convertAndAdd(List<ArtifactSummary> summaries, Iterable<ArtifactDetail> details) {
    for (ArtifactDetail detail : details) {
      summaries.add(ArtifactSummary.from(detail.getDescriptor().getArtifactId()));
    }
    return summaries;
  }

  /**
   * Get {@link ArtifactDescriptor} of parent and grandparent (if any) artifacts for the given artifact.
   *
   * @param artifactId the id of the artifact for which to find its parent and grandparent {@link ArtifactDescriptor}
   * @param parentArtifacts the ranges of parents to find
   * @return {@link ArtifactDescriptor} of parent and grandparent (if any) artifacts, in that specific order
   * @throws ArtifactRangeNotFoundException if none of the parents could be found
   * @throws InvalidArtifactException       if one of the parents also has parents
   */
  private List<ArtifactDescriptor> getParentArtifactDescriptors(Id.Artifact artifactId,
                                                                Set<ArtifactRange> parentArtifacts)
    throws ArtifactRangeNotFoundException, InvalidArtifactException {
    List<ArtifactDetail> parents = new ArrayList<>();
    for (ArtifactRange parentRange : parentArtifacts) {
      parents.addAll(artifactStore.getArtifacts(parentRange, Integer.MAX_VALUE, ArtifactSortOrder.UNORDERED));
    }

    if (parents.isEmpty()) {
      throw new ArtifactRangeNotFoundException(String.format("Artifact %s extends artifacts '%s' that do not exist",
                                                             artifactId, Joiner.on('/').join(parentArtifacts)));
    }

    ArtifactDescriptor parentArtifact = null;
    ArtifactDescriptor grandparentArtifact = null;

    // check if any of the parents also have grandparents, which is not allowed. This is to simplify things
    // so that we don't have to chain a bunch of classloaders, and also to keep it simple for users to avoid
    // complicated dependency trees that are hard to manage.
    for (ArtifactDetail parent : parents) {
      Set<ArtifactRange> grandparentRanges = parent.getMeta().getUsableBy();
      for (ArtifactRange grandparentRange : grandparentRanges) {
        // if the parent as the child as a parent (cyclic dependency)
        if (grandparentRange.getNamespace().equals(artifactId.getNamespace().getId()) &&
          grandparentRange.getName().equals(artifactId.getName()) &&
          grandparentRange.versionIsInRange(artifactId.getVersion())) {
          throw new InvalidArtifactException(String.format(
            "Invalid artifact '%s': cyclic dependency. Parent '%s' has artifact '%s' as a parent.",
            artifactId, parent.getDescriptor().getArtifactId(), artifactId));
        }

        List<ArtifactDetail> grandparents =
          artifactStore.getArtifacts(grandparentRange, Integer.MAX_VALUE, ArtifactSortOrder.UNORDERED);

        // check that no grandparent has parents
        for (ArtifactDetail grandparent : grandparents) {
          Set<ArtifactRange> greatGrandparents = grandparent.getMeta().getUsableBy();
          if (!greatGrandparents.isEmpty()) {
            throw new InvalidArtifactException(String.format(
              "Invalid artifact '%s'. Grandparents of artifacts cannot have parents. Grandparent '%s' has parents.",
              artifactId, grandparent.getDescriptor().getArtifactId()));
          }

          // assumes any grandparent will do
          if (parentArtifact == null && grandparentArtifact == null) {
            grandparentArtifact = grandparent.getDescriptor();
          }
        }
      }

      // assumes any parent will do
      if (parentArtifact == null) {
        parentArtifact = parent.getDescriptor();
      }
    }

    List<ArtifactDescriptor> parentArtifactList = new ArrayList<>();
    parentArtifactList.add(parentArtifact);
    if (grandparentArtifact != null) {
      parentArtifactList.add(grandparentArtifact);
    }
    return parentArtifactList;
  }

  /**
   * Create a parent classloader (potentially multi-level classloader) based on the list of parent artifacts provided.
   * The multi-level classloader will be constructed based the order of artifacts in the list (e.g. lower level
   * classloader from artifacts in the front of the list and high leveler classloader from those towards the end)
   *
   * @param parentArtifacts list of parent artifacts to create the classloader from
   * @throws IOException if there was some error reading from the store
   */
  private CloseableClassLoader createParentClassLoader(List<ArtifactDescriptor> parentArtifacts,
                                                       EntityImpersonator entityImpersonator)
    throws IOException {
    List<Location> parentLocations = new ArrayList<>();
    for (ArtifactDescriptor descriptor : parentArtifacts) {
      parentLocations.add(descriptor.getLocation());
    }
    return artifactClassLoaderFactory.createClassLoader(parentLocations.iterator(), entityImpersonator);
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
      if (artifactId.getName().equals(parentName) &&
        artifactId.getNamespace().toEntityId().getNamespace().equals(parent.getNamespace())) {
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

  private void writeSystemMetadata(io.cdap.cdap.proto.id.ArtifactId artifactId, ArtifactInfo artifactInfo) {
    // add system metadata for artifacts
    ArtifactSystemMetadataWriter writer =
      new ArtifactSystemMetadataWriter(metadataServiceClient, artifactId, artifactInfo);
    writer.write();
  }
}
