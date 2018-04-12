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

import co.cask.cdap.api.artifact.ApplicationClass;
import co.cask.cdap.api.artifact.ArtifactClasses;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.CloseableClassLoader;
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
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.system.ArtifactSystemMetadataWriter;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.internal.app.spark.SparkCompat;
import co.cask.cdap.proto.artifact.ApplicationClassInfo;
import co.cask.cdap.proto.artifact.ApplicationClassSummary;
import co.cask.cdap.proto.artifact.ArtifactSortOrder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.EntityImpersonator;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * Default implementation of the {@link ArtifactRepository}, all the operation does not have authorization enforce
 * involved
 */
public class DefaultArtifactRepository implements ArtifactRepository {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultArtifactRepository.class);
  private final ArtifactStore artifactStore;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final ArtifactInspector artifactInspector;
  private final Set<File> systemArtifactDirs;
  private final ArtifactConfigReader configReader;
  private final MetadataStore metadataStore;
  private final Impersonator impersonator;

  @VisibleForTesting
  @Inject
  public DefaultArtifactRepository(CConfiguration cConf, ArtifactStore artifactStore, MetadataStore metadataStore,
                                   ProgramRunnerFactory programRunnerFactory,
                                   Impersonator impersonator) {
    this.artifactStore = artifactStore;
    this.artifactClassLoaderFactory = new ArtifactClassLoaderFactory(cConf, programRunnerFactory);
    this.artifactInspector = new ArtifactInspector(cConf, artifactClassLoaderFactory);
    this.systemArtifactDirs = new HashSet<>();
    String systemArtifactsDir = cConf.get(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR);
    if (!Strings.isNullOrEmpty(systemArtifactsDir)) {
      String sparkDirStr = SparkCompat.get(cConf).getCompat();

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
    this.metadataStore = metadataStore;
    this.impersonator = impersonator;
  }

  @Override
  public CloseableClassLoader createArtifactClassLoader(
    Location artifactLocation, EntityImpersonator entityImpersonator) throws IOException {
    return artifactClassLoaderFactory.createClassLoader(ImmutableList.of(artifactLocation).iterator(),
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
    return artifactStore.getArtifact(artifactId);
  }

  @Override
  public List<ArtifactDetail> getArtifactDetails(final ArtifactRange range, int limit,
                                                 ArtifactSortOrder order) throws Exception {
    return artifactStore.getArtifacts(range, limit, order);
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
    Predicate<co.cask.cdap.proto.id.ArtifactId> pluginPredicate,
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
                       Collections.<String, String>emptyMap());
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
    if (!parentArtifacts.isEmpty()) {
      validateParentSet(artifactId, parentArtifacts);
      parentClassLoader = createParentClassLoader(artifactId, parentArtifacts, entityImpersonator);
    }
    try {
      ArtifactClasses artifactClasses = inspectArtifact(artifactId, artifactFile, additionalPlugins, parentClassLoader);
      ArtifactMeta meta = new ArtifactMeta(artifactClasses, parentArtifacts, properties);
      ArtifactDetail artifactDetail =
        artifactStore.write(artifactId, meta, Files.newInputStreamSupplier(artifactFile), entityImpersonator);
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

  @Override
  public void writeArtifactProperties(Id.Artifact artifactId, final Map<String, String> properties) throws Exception {
    artifactStore.updateArtifactProperties(artifactId, new Function<Map<String, String>, Map<String, String>>() {
      @Override
      public Map<String, String> apply(Map<String, String> oldProperties) {
        return properties;
      }
    });
  }

  @Override
  public void writeArtifactProperty(Id.Artifact artifactId, final String key, final String value) throws Exception {
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

  @Override
  public void deleteArtifactProperty(Id.Artifact artifactId, final String key) throws Exception {
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

  @Override
  public void deleteArtifactProperties(Id.Artifact artifactId) throws Exception {
    artifactStore.updateArtifactProperties(artifactId, new Function<Map<String, String>, Map<String, String>>() {
      @Override
      public Map<String, String> apply(Map<String, String> oldProperties) {
        return new HashMap<>();
      }
    });
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

    // loop until there is no change
    boolean nochange = false;
    while (!remainingArtifacts.isEmpty() && !nochange) {
      // add all artifacts that don't have any more parents
      Set<Id.Artifact> addedArtifacts = new HashSet<>();
      for (Id.Artifact remainingArtifact : remainingArtifacts) {
        if (!childToParents.containsKey(remainingArtifact)) {
          addSystemArtifact(systemArtifacts.get(remainingArtifact));
          addedArtifacts.add(remainingArtifact);
          for (Id.Artifact child : parentToChildren.get(remainingArtifact)) {
            childToParents.remove(child, remainingArtifact);
          }
        }
      }
      remainingArtifacts.removeAll(addedArtifacts);
      nochange = addedArtifacts.isEmpty();
    }

    if (!remainingArtifacts.isEmpty()) {
      LOG.warn("Unable to add system artifacts {} due to cyclic dependencies", Joiner.on(",").join(remainingArtifacts));
    }
  }

  @Override
  public void deleteArtifact(Id.Artifact artifactId) throws Exception {
    // delete the artifact first and then privileges. Not the other way to avoid orphan artifact
    // which does not have any privilege if the artifact delete from store fails. see CDAP-6648
    artifactStore.delete(artifactId);
    metadataStore.removeMetadata(artifactId.toEntityId().toMetadataEntity());
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

  private ArtifactClasses inspectArtifact(Id.Artifact artifactId, File artifactFile,
                                          @Nullable Set<PluginClass> additionalPlugins,
                                          @Nullable ClassLoader parentClassLoader) throws IOException,
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
                                                       EntityImpersonator entityImpersonator)
    throws ArtifactRangeNotFoundException, IOException, InvalidArtifactException {

    List<ArtifactDetail> parents = new ArrayList<>();
    for (ArtifactRange parentRange : parentArtifacts) {
      parents.addAll(artifactStore.getArtifacts(parentRange, Integer.MAX_VALUE, ArtifactSortOrder.UNORDERED));
    }

    if (parents.isEmpty()) {
      throw new ArtifactRangeNotFoundException(String.format("Artifact %s extends artifacts '%s' that do not exist",
                                                             artifactId, Joiner.on('/').join(parentArtifacts)));
    }

    Location parentLocation = null;
    Location grandparentLocation = null;

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
          if (parentLocation == null && grandparentLocation == null) {
            grandparentLocation = grandparent.getDescriptor().getLocation();
          }
        }
      }

      // assumes any parent will do
      if (parentLocation == null) {
        parentLocation = parent.getDescriptor().getLocation();
      }
    }

    List<Location> parentLocations = new ArrayList<>();
    parentLocations.add(parentLocation);
    if (grandparentLocation != null) {
      parentLocations.add(grandparentLocation);
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

  private void writeSystemMetadata(co.cask.cdap.proto.id.ArtifactId artifactId, ArtifactInfo artifactInfo) {
    // add system metadata for artifacts
    ArtifactSystemMetadataWriter writer = new ArtifactSystemMetadataWriter(metadataStore, artifactId, artifactInfo);
    writer.write();
  }
}
