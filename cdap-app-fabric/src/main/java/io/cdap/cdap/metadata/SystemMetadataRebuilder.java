/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;
import io.cdap.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import io.cdap.cdap.data2.metadata.system.ArtifactSystemMetadataWriter;
import io.cdap.cdap.data2.metadata.system.DatasetSystemMetadataProvider;
import io.cdap.cdap.data2.metadata.system.DelegateSystemMetadataWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.internal.app.deploy.pipeline.MetadataWriterStage;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.metadata.profile.ProfileMetadataMessageProcessor;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.store.DefaultNamespaceStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the metadata that CDAP derives from its own stores to the metadata storage again: the
 * system metadata of all artifacts, applications, programs and datasets, the plugin metadata of
 * the applications, and the profile metadata of programs and schedules.
 *
 * <p>Use this after the metadata storage lost metadata. All writes add to the metadata that the
 * storage has. They remove nothing and keep the creation time of an entity that the storage
 * knows. Thus it is safe to run this when the storage is complete, more than one time, and while
 * CDAP is in use.
 *
 * <p>The metadata storage is the only store of these items. If the storage lost them, they stay
 * lost:
 * <ul>
 *   <li>tags and properties that users added;</li>
 *   <li>system metadata that an application emitted when it was deployed;</li>
 *   <li>system metadata that needs an instance of a dataset, for example its schema.</li>
 * </ul>
 */
@Singleton
public class SystemMetadataRebuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SystemMetadataRebuilder.class);
  private static final int BATCH_SIZE = 100;
  private static final ScopedName CREATION_TIME =
      new ScopedName(MetadataScope.SYSTEM, MetadataConstants.CREATION_TIME_KEY);
  // No need to wait until a search sees each write.
  private static final MutationOptions WRITE_OPTIONS =
      MutationOptions.builder().setAsynchronous(true).build();
  // The system metadata writers need a client for a method that this class does not call.
  private static final MetadataServiceClient UNUSED_CLIENT = new NoOpMetadataServiceClient();

  private final MetadataStorage metadataStorage;
  private final TransactionRunner transactionRunner;
  private final ArtifactStore artifactStore;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  SystemMetadataRebuilder(MetadataStorage metadataStorage, TransactionRunner transactionRunner,
      ArtifactStore artifactStore, MetricsCollectionService metricsCollectionService) {
    this.metadataStorage = metadataStorage;
    this.transactionRunner = transactionRunner;
    this.artifactStore = artifactStore;
    this.metricsCollectionService = metricsCollectionService;
  }

  /**
   * Deletes all metadata, also the metadata that users added, and then writes the metadata of
   * all entities to the metadata storage. Use this if the index of the metadata storage is not
   * usable.
   *
   * @throws IOException if the index cannot be created again, or if {@link #rebuild()} fails
   */
  public synchronized void dropAndRebuild() throws IOException {
    LOG.warn("Deleting all metadata from the metadata storage.");
    metadataStorage.dropIndex();
    metadataStorage.createIndex();
    rebuild(false);
  }

  /**
   * Writes the metadata of all entities to the metadata storage, and records in the storage that
   * this is complete.
   *
   * @throws IOException if a store cannot be read or the metadata storage cannot be written. The
   *     metadata that was written before the failure stays.
   */
  public synchronized void rebuild() throws IOException {
    rebuild(true);
  }

  private void rebuild(boolean preserveExistingCreationTime) throws IOException {
    LOG.info("Writing the system metadata of all entities to the metadata storage again.");
    List<NamespaceId> namespaces = new ArrayList<>();
    // The system namespace has artifacts and applications, but is not in the namespace store.
    namespaces.add(NamespaceId.SYSTEM);
    for (NamespaceMeta meta : new DefaultNamespaceStore(transactionRunner).list()) {
      namespaces.add(meta.getNamespaceId());
    }

    // Artifacts go first: the metadata of an application needs the classes of its artifact.
    Map<io.cdap.cdap.proto.id.ArtifactId, ArtifactDetail> artifacts = new HashMap<>();
    for (NamespaceId namespace : namespaces) {
      for (ArtifactDetail artifact : artifactStore.getArtifacts(namespace)) {
        ArtifactId artifactId = artifact.getDescriptor().getArtifactId();
        artifacts.put(namespace.artifact(artifactId.getName(),
            artifactId.getVersion().getVersion()), artifact);
      }
    }
    List<MetadataMutation.Create> mutations = new ArrayList<>();
    artifacts.forEach((artifactId, artifact) -> mutations.add(
        new ArtifactSystemMetadataWriter(UNUSED_CLIENT, artifactId, new ArtifactInfo(
            artifact.getDescriptor().getArtifactId(), artifact.getMeta().getClasses(),
            artifact.getMeta().getProperties())).getMetadataMutation()));
    write("artifacts", toUpdates(mutations, preserveExistingCreationTime));

    for (NamespaceId namespace : namespaces) {
      write("applications and programs in namespace " + namespace.getNamespace(),
          toUpdates(collectApplicationMetadata(namespace, artifacts),
              preserveExistingCreationTime));
      write("datasets in namespace " + namespace.getNamespace(),
          toUpdates(collectDatasetMetadata(namespace), preserveExistingCreationTime));
    }

    for (NamespaceId namespace : namespaces) {
      // Read in a transaction, and write after it: the writes can take long.
      List<MetadataMutation> updates = TransactionRunners.run(transactionRunner, context -> {
        return new ProfileMetadataMessageProcessor(metadataStorage, context,
            metricsCollectionService).collectMetadata(namespace);
      }, IOException.class);
      write("programs, schedules and plugins in namespace " + namespace.getNamespace(),
          mergeUpdates(updates));
    }
    metadataStorage.markRebuilt();
    LOG.info("Finished writing the system metadata of all entities to the metadata storage.");
  }

  private List<MetadataMutation.Create> collectApplicationMetadata(NamespaceId namespace,
      Map<io.cdap.cdap.proto.id.ArtifactId, ArtifactDetail> artifacts) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      List<MetadataMutation.Create> mutations = new ArrayList<>();
      AppMetadataStore.create(context).scanApplications(
          // All versions of an application share its metadata. The latest version counts.
          ScanApplicationsRequest.builder().setNamespaceId(namespace).setLatestOnly(true).build(),
          entry -> {
            ApplicationId appId = entry.getKey();
            ApplicationMeta meta = entry.getValue();
            // An application that has no record of its creation gets the current time, in the
            // same way as a new application.
            long creationTime = meta.getChange() == null ? System.currentTimeMillis()
                : meta.getChange().getCreationTimeMillis();
            mutations.addAll(MetadataWriterStage.createSystemMetadataMutations(
                UNUSED_CLIENT, appId, meta.getSpec(),
                getApplicationClass(appId, meta.getSpec(), artifacts),
                String.valueOf(creationTime), null));
            return true;
          });
      return mutations;
    }, IOException.class);
  }

  /**
   * Returns the application class of an application. The metadata uses it for the capabilities
   * that the application requires.
   *
   * <p>The application store does not record the name of the class. Therefore this method can
   * only tell the class if the artifact has exactly one application class, which is the usual
   * case. Otherwise it returns a class without requirements.
   */
  private static ApplicationClass getApplicationClass(ApplicationId appId,
      ApplicationSpecification appSpec,
      Map<io.cdap.cdap.proto.id.ArtifactId, ArtifactDetail> artifacts) {
    ArtifactId artifactId = appSpec.getArtifactId();
    NamespaceId artifactNamespace = artifactId.getScope() == ArtifactScope.SYSTEM
        ? NamespaceId.SYSTEM : appId.getNamespaceId();
    ArtifactDetail artifact = artifacts.get(artifactNamespace.artifact(
        artifactId.getName(), artifactId.getVersion().getVersion()));
    if (artifact != null) {
      Set<ApplicationClass> classes = artifact.getMeta().getClasses().getApps();
      if (classes.size() == 1) {
        return classes.iterator().next();
      }
    }
    return new ApplicationClass(appSpec.getName(), "", null);
  }

  private List<MetadataMutation.Create> collectDatasetMetadata(NamespaceId namespace) {
    List<MetadataMutation.Create> mutations = new ArrayList<>();
    for (DatasetSpecification spec : new DatasetInstanceManager(transactionRunner)
        .getAll(namespace)) {
      DatasetId datasetId = namespace.dataset(spec.getName());
      // Only user datasets have system metadata.
      if (!DatasetsUtil.isUserDataset(datasetId)) {
        continue;
      }
      Map<String, String> properties = spec.getOriginalProperties() == null
          ? spec.getProperties() : spec.getOriginalProperties();
      mutations.add(new DelegateSystemMetadataWriter(UNUSED_CLIENT, datasetId,
          new DatasetSystemMetadataProvider(datasetId, DatasetProperties.of(properties), null,
              spec.getType(), spec.getDescription())).getMetadataMutation());
    }
    return mutations;
  }

  /**
   * Turns the mutations of the writers of system metadata into updates. Such a mutation creates
   * the system metadata of an entity: it removes the system metadata that it does not contain,
   * and it keeps the creation time that the storage has. An update removes nothing, but it
   * replaces all properties that it contains. Therefore the update gets a creation time only if
   * the storage has none for the entity.
   */
  private List<MetadataMutation> toUpdates(List<MetadataMutation.Create> creates,
      boolean preserveExistingCreationTime) throws IOException {
    List<MetadataMutation> updates = new ArrayList<>(creates.size());
    for (MetadataMutation.Create create : creates) {
      Metadata metadata = create.getMetadata();
      if (preserveExistingCreationTime && metadata.getProperties().containsKey(CREATION_TIME)) {
        Metadata existing = metadataStorage.read(
            new Read(create.getEntity(), MetadataScope.SYSTEM, MetadataKind.PROPERTY));
        if (existing.getProperties().containsKey(CREATION_TIME)) {
          Map<ScopedName, String> properties = new HashMap<>(metadata.getProperties());
          properties.remove(CREATION_TIME);
          metadata = new Metadata(metadata.getTags(), properties);
        }
      }
      updates.add(new MetadataMutation.Update(create.getEntity(), metadata));
    }
    return updates;
  }

  /**
   * Merges the updates for the same entity into one update, so that every entity occurs one time
   * in a batch of writes.
   */
  private static List<MetadataMutation> mergeUpdates(List<MetadataMutation> updates) {
    Map<MetadataEntity, Metadata> merged = new LinkedHashMap<>();
    List<MetadataMutation> nonUpdates = new ArrayList<>();
    for (MetadataMutation mutation : updates) {
      if (!(mutation instanceof MetadataMutation.Update)) {
        nonUpdates.add(mutation);
        continue;
      }
      Metadata metadata = ((MetadataMutation.Update) mutation).getUpdates();
      merged.merge(mutation.getEntity(), metadata, (first, second) -> {
        Set<ScopedName> tags = new HashSet<>(first.getTags());
        tags.addAll(second.getTags());
        Map<ScopedName, String> properties = new HashMap<>(first.getProperties());
        properties.putAll(second.getProperties());
        return new Metadata(tags, properties);
      });
    }
    List<MetadataMutation> result = new ArrayList<>(merged.size() + nonUpdates.size());
    merged.forEach((entity, metadata) -> result.add(new MetadataMutation.Update(entity, metadata)));
    result.addAll(nonUpdates);
    return result;
  }

  private void write(String description, List<MetadataMutation> updates) throws IOException {
    for (List<MetadataMutation> batch : Lists.partition(updates, BATCH_SIZE)) {
      metadataStorage.batch(batch, WRITE_OPTIONS);
    }
    LOG.info("Wrote the system metadata of {} entities: {}.", updates.size(), description);
  }
}
