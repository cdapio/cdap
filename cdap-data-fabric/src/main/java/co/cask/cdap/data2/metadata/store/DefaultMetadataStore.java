/*
 * Copyright 2015-2019 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.store;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.audit.payload.builder.MetadataPayloadBuilder;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.spi.metadata.dataset.SearchHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link MetadataStore}.
 */
public class DefaultMetadataStore extends SearchHelper implements MetadataStore {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataStore.class);
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();
  private static final Set<String> EMPTY_TAGS = ImmutableSet.of();

  private AuditPublisher auditPublisher;

  @Inject
  DefaultMetadataStore(TransactionSystemClient txClient,
                       @Named(Constants.Dataset.TABLE_TYPE) DatasetDefinition tableDefinition) {
    super(txClient, tableDefinition);
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  @Override
  public void createIndex() throws IOException {
    createDatasets();
  }

  @Override
  public void dropIndex() throws IOException {
    dropDatasets();
  }

  @Override
  public void replaceMetadata(MetadataScope scope, MetadataDataset.Record newMetadata,
                              Set<String> propertiesToKeep, Set<String> propertiesToPreserve) {

    MetadataEntity entity = newMetadata.getMetadataEntity();

    Set<String> newTags = newMetadata.getTags();
    Map<String, String> newProperties = newMetadata.getProperties();
    AtomicReference<Set<String>> addedTags = new AtomicReference<>();
    AtomicReference<Set<String>> deletedTags = new AtomicReference<>();
    AtomicReference<MetadataDataset.Record> previous = new AtomicReference<>();

    MetadataDataset.Record latest = execute((mds) -> {
      MetadataDataset.Record existing = mds.getMetadata(entity);
      Set<String> oldTags = existing.getTags();
      Map<String, String> oldProperties = existing.getProperties();
      // compute differences
      Set<String> tagsToDelete = Sets.difference(oldTags, newTags);
      Set<String> tagsToAdd = Sets.difference(newTags, oldTags);
      Set<String> propertiesToDelete = Sets.difference(Sets.difference(oldProperties.keySet(), newProperties.keySet()),
                                                       Sets.union(propertiesToKeep, propertiesToPreserve));
      Map<String, String> propertiesToSet = newProperties;
      Set<String> oldPropertiesToPreserve = Sets.intersection(oldProperties.keySet(), propertiesToPreserve);
      if (!oldPropertiesToPreserve.isEmpty()) {
        propertiesToSet = Maps.filterKeys(propertiesToSet, key -> !oldPropertiesToPreserve.contains(key));
      }
      // perform updates
      MetadataDataset.Change changed = null;
      if (!tagsToDelete.isEmpty()) {
        changed = mds.removeTags(entity, tagsToDelete);
      }
      if (!tagsToAdd.isEmpty()) {
        changed = mds.addTags(entity, tagsToAdd);
      }
      if (!propertiesToDelete.isEmpty()) {
        changed = mds.removeProperties(entity, propertiesToDelete);
      }
      if (!propertiesToSet.isEmpty()) {
        changed = mds.addProperties(entity, propertiesToSet);
      }
      previous.set(existing);
      addedTags.set(tagsToAdd);
      deletedTags.set(tagsToDelete);
      return changed == null ? null : changed.getLatest();
    }, MetadataScope.SYSTEM);

    Set<String> previousTags = previous.get().getTags();
    Map<String, String> previousProperties = previous.get().getProperties();
    Map<String, String> addedProperties, deletedProperties;
    if (latest == null) {
      addedProperties = Collections.emptyMap();
      deletedProperties = Collections.emptyMap();
    } else {
      // compute additions and deletions again as we publish audit outside of tx
      Map<String, String> currentProperties = latest.getProperties();
      //noinspection ConstantConditions
      addedProperties = Maps.filterEntries(currentProperties,
                                           entry -> !entry.getValue().equals(previousProperties.get(entry.getKey())));
      //noinspection ConstantConditions
      deletedProperties = Maps.filterEntries(previousProperties,
                                             entry -> !entry.getValue().equals(currentProperties.get(entry.getKey())));
    }
    publishAudit(new MetadataRecord(entity, MetadataScope.SYSTEM, previousProperties, previousTags),
                 new MetadataRecord(entity, MetadataScope.SYSTEM, addedProperties, addedTags.get()),
                 new MetadataRecord(entity, MetadataScope.SYSTEM, deletedProperties, deletedTags.get()));
  }

  @Override
  public void addProperties(final MetadataScope scope, final MetadataEntity metadataEntity,
                            final Map<String, String> properties) {
    MetadataDataset.Change metadataChange = execute(mds -> mds.addProperties(metadataEntity, properties), scope);
    publishAudit(scope, metadataEntity, properties, metadataChange);
  }

  private void publishAudit(MetadataScope scope, MetadataEntity metadataEntity,
                            Map<String, String> properties, MetadataDataset.Change metadataChange) {
    MetadataRecord previous = new MetadataRecord(metadataEntity, scope,
                                                 metadataChange.getExisting().getProperties(),
                                                 metadataChange.getExisting().getTags());
    final ImmutableMap.Builder<String, String> propAdditions = ImmutableMap.builder();
    final ImmutableMap.Builder<String, String> propDeletions = ImmutableMap.builder();
    // Iterating over properties all over again, because we want to move the diff calculation outside the transaction.
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String existingValue = previous.getProperties().get(entry.getKey());
      if (existingValue != null && existingValue.equals(entry.getValue())) {
        // Value already exists and is the same as the value being passed. No update necessary.
        continue;
      }
      // At this point, its either an update of an existing property (1 addition + 1 deletion) or a new property.
      // If it is an update, then mark a single deletion.
      if (existingValue != null) {
        propDeletions.put(entry.getKey(), existingValue);
      }
      // In both update or new cases, mark a single addition.
      propAdditions.put(entry.getKey(), entry.getValue());
    }
    LOG.trace("Publishing Audit Message for {}", metadataEntity);
    publishAudit(previous, new MetadataRecord(metadataEntity, scope, propAdditions.build(), EMPTY_TAGS),
                 new MetadataRecord(metadataEntity, scope, propDeletions.build(), EMPTY_TAGS));
  }

  @Override
  public void addProperties(MetadataScope scope, Map<MetadataEntity, Map<String, String>> toUpdate) {
    Map<MetadataEntity, ImmutablePair<Map<String, String>, MetadataDataset.Change>> changeLog = execute(mds -> {
      Map<MetadataEntity, ImmutablePair<Map<String, String>, MetadataDataset.Change>> changes = new HashMap<>();
      for (Map.Entry<MetadataEntity, Map<String, String>> entry : toUpdate.entrySet()) {
        MetadataEntity metadataEntity = entry.getKey();
        Map<String, String> properties = entry.getValue();
        MetadataDataset.Change metadataChange = mds.addProperties(metadataEntity, properties);
        changes.put(metadataEntity, ImmutablePair.of(properties, metadataChange));
      }
      return changes;
    }, scope);
    for (Map.Entry<MetadataEntity, ImmutablePair<Map<String, String>, MetadataDataset.Change>>
      entry : changeLog.entrySet()) {
      publishAudit(scope, entry.getKey(), entry.getValue().getFirst(), entry.getValue().getSecond());
    }
  }

  @Override
  public void addProperty(final MetadataScope scope, final MetadataEntity metadataEntity, final String key,
                          final String value) {
    MetadataDataset.Change change = execute(mds -> mds.addProperty(metadataEntity, key, value), scope);
    publishAudit(new MetadataRecord(metadataEntity, scope, change.getExisting().getProperties(),
                                    change.getExisting().getTags()),
                 new MetadataRecord(metadataEntity, scope, ImmutableMap.of(key, value), EMPTY_TAGS),
                 new MetadataRecord(metadataEntity, scope));
  }

  /**
   * Adds tags for the specified {@link MetadataEntity}.
   */
  @Override
  public void addTags(final MetadataScope scope, final MetadataEntity metadataEntity,
                      final Set<String> tagsToAdd) {
    MetadataDataset.Change metadataChange = execute(mds -> mds.addTags(metadataEntity, tagsToAdd), scope);
    publishAudit(new MetadataRecord(metadataEntity, scope, metadataChange.getExisting().getProperties(),
                                    metadataChange.getExisting().getTags()),
                 new MetadataRecord(metadataEntity, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToAdd)),
                 new MetadataRecord(metadataEntity, scope));
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataEntity metadataEntity) {
    return ImmutableSet.of(getMetadata(MetadataScope.USER, metadataEntity), getMetadata(MetadataScope.SYSTEM,
                                                                                            metadataEntity));
  }

  @Override
  public MetadataRecord getMetadata(final MetadataScope scope, final MetadataEntity metadataEntity) {
    return execute(mds -> {
      MetadataDataset.Record metadata = mds.getMetadata(metadataEntity);
      return new MetadataRecord(metadataEntity, scope, metadata.getProperties(), metadata.getTags());
    }, scope);
  }

  /**
   * @return a set of {@link MetadataRecord}s representing all the metadata (including properties and tags)
   * for the specified set of {@link MetadataEntity}s.
   */
  @Override
  public Set<MetadataRecord> getMetadata(final MetadataScope scope, final Set<MetadataEntity> metadataEntities) {
    return execute(mds -> {
      Set<MetadataRecord> metadataRecords = new HashSet<>(metadataEntities.size());
      // do a batch get
      Set<MetadataDataset.Record> metadatas = mds.getMetadata(metadataEntities);
      for (MetadataDataset.Record metadata : metadatas) {
        metadataRecords.add(new MetadataRecord(metadata.getMetadataEntity(), scope, metadata.getProperties(),
                                               metadata.getTags()));
      }
      return metadataRecords;
    }, scope);
  }

  @Override
  public Map<String, String> getProperties(MetadataEntity metadataEntity) {
    return ImmutableMap.<String, String>builder()
      .putAll(getProperties(MetadataScope.USER, metadataEntity))
      .putAll(getProperties(MetadataScope.SYSTEM, metadataEntity))
      .build();
  }

  /**
   * @return the metadata for the specified {@link MetadataEntity}
   */
  @Override
  public Map<String, String> getProperties(MetadataScope scope, final MetadataEntity metadataEntity) {
    return execute(mds -> mds.getProperties(metadataEntity), scope);
  }

  @Override
  public Set<String> getTags(MetadataEntity metadataEntity) {
    return ImmutableSet.<String>builder()
      .addAll(getTags(MetadataScope.USER, metadataEntity))
      .addAll(getTags(MetadataScope.SYSTEM, metadataEntity))
      .build();
  }

  /**
   * @return the tags for the specified {@link MetadataEntity}
   */
  @Override
  public Set<String> getTags(MetadataScope scope, final MetadataEntity metadataEntity) {
    return execute(mds -> mds.getTags(metadataEntity), scope);
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) {
    removeMetadata(MetadataScope.USER, metadataEntity);
    removeMetadata(MetadataScope.SYSTEM, metadataEntity);
  }

  /**
   * Removes all metadata (including properties and tags) for the specified {@link MetadataEntity}.
   */
  @Override
  public void removeMetadata(final MetadataScope scope, final MetadataEntity metadataEntity) {
    MetadataDataset.Change metadataChange = execute(mds -> mds.removeMetadata(metadataEntity), scope);
    MetadataRecord previous = new MetadataRecord(metadataEntity, scope,
                                                 metadataChange.getExisting().getProperties(),
                                                 metadataChange.getExisting().getTags());
    publishAudit(previous, new MetadataRecord(metadataEntity, scope), new MetadataRecord(previous));
  }

  /**
   * Removes all properties for the specified {@link MetadataEntity}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final MetadataEntity metadataEntity) {
    MetadataDataset.Change metadataChange = execute(mds -> mds.removeProperties(metadataEntity), scope);
    MetadataRecord previous = new MetadataRecord(metadataEntity, scope,
                                                 metadataChange.getExisting().getProperties(),
                                                 metadataChange.getExisting().getTags());
    publishAudit(previous, new MetadataRecord(metadataEntity, scope),
                 new MetadataRecord(metadataEntity, scope, previous.getProperties(), EMPTY_TAGS));
  }

  /**
   * Removes the specified properties of the {@link MetadataEntity}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final MetadataEntity metadataEntity,
                               final Set<String> keys) {
    MetadataDataset.Change metadataChange = execute(mds -> mds.removeProperties(metadataEntity, keys), scope);
    publishAudit(new MetadataRecord(metadataEntity, scope, metadataChange.getExisting().getProperties(),
                                    metadataChange.getExisting().getTags()),
                 new MetadataRecord(metadataEntity, scope),
                 new MetadataRecord(metadataEntity, scope, metadataChange.getDeletedProperties(), EMPTY_TAGS));
  }

  @Override
  public void removeProperties(MetadataScope scope, Map<MetadataEntity, Set<String>> toRemove) {
    Map<MetadataEntity, MetadataDataset.Change> changes = execute(mds -> {
      Map<MetadataEntity, MetadataDataset.Change> map = new HashMap<>();
      for (Map.Entry<MetadataEntity, Set<String>> entry : toRemove.entrySet()) {
        MetadataDataset.Change change = mds.removeProperties(entry.getKey(), entry.getValue());
        map.put(entry.getKey(), change);
      }
      return map;
    }, scope);
    for (Map.Entry<MetadataEntity, MetadataDataset.Change> entry : changes.entrySet()) {
      MetadataEntity metadataEntity = entry.getKey();
      MetadataDataset.Change metadataChange = entry.getValue();
      publishAudit(new MetadataRecord(metadataEntity, scope, metadataChange.getExisting().getProperties(),
                                      metadataChange.getExisting().getTags()),
                   new MetadataRecord(metadataEntity, scope),
                   new MetadataRecord(metadataEntity, scope, metadataChange.getDeletedProperties(), EMPTY_TAGS));
    }
  }

  /**
   * Removes all the tags from the {@link MetadataEntity}
   */
  @Override
  public void removeTags(final MetadataScope scope, final MetadataEntity metadataEntity) {
    MetadataDataset.Change metadataChange = execute(mds -> mds.removeTags(metadataEntity), scope);
    MetadataRecord previous = new MetadataRecord(metadataEntity, scope,
                                                 metadataChange.getExisting().getProperties(),
                                                 metadataChange.getExisting().getTags());
    publishAudit(previous, new MetadataRecord(metadataEntity, scope),
                 new MetadataRecord(metadataEntity, scope, EMPTY_PROPERTIES, previous.getTags()));
  }

  /**
   * Removes the specified tags from the {@link MetadataEntity}
   */
  @Override
  public void removeTags(final MetadataScope scope, final MetadataEntity metadataEntity,
                         final Set<String> tagsToRemove) {
    MetadataDataset.Change metadataChange = execute(mds -> mds.removeTags(metadataEntity, tagsToRemove), scope);
    publishAudit(new MetadataRecord(metadataEntity, scope, metadataChange.getExisting().getProperties(),
                                    metadataChange.getExisting().getTags()),
                 new MetadataRecord(metadataEntity, scope),
                 new MetadataRecord(metadataEntity, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToRemove)));
  }

  @Override
  public MetadataSearchResponse search(SearchRequest request) {
    return super.search(request, null);
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope,
                                                   final Set<MetadataEntity> metadataEntities,
                                                   final long timeMillis) {
    Set<MetadataDataset.Record> metadataHistoryEntries =
      execute(mds -> mds.getSnapshotBeforeTime(metadataEntities, timeMillis), scope);

    ImmutableSet.Builder<MetadataRecord> builder = ImmutableSet.builder();
    for (MetadataDataset.Record metadata : metadataHistoryEntries) {
      builder.add(new MetadataRecord(metadata.getEntityId(), scope,
                                     metadata.getProperties(), metadata.getTags()));
    }
    return builder.build();
  }

  private void publishAudit(MetadataRecord previous, MetadataRecord additions, MetadataRecord deletions) {
    MetadataPayloadBuilder builder = new MetadataPayloadBuilder();
    builder.addPrevious(previous);
    builder.addAdditions(additions);
    builder.addDeletions(deletions);
    AuditPublishers.publishAudit(auditPublisher, previous.getMetadataEntity(), AuditType.METADATA_CHANGE,
                                 builder.build());
  }

  private <T> T execute(TransactionExecutor.Function<MetadataDataset, T> func, MetadataScope scope) {
    return execute(context -> func.apply(context.getDataset(scope)));
  }

  /**
   * Adds V2 datasets and types to the given {@link DatasetFramework}. Used by the upgrade tool.
   *
   * @param framework Dataset framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(MetadataDataset.class.getName(), BUSINESS_METADATA_INSTANCE_ID, DatasetProperties.EMPTY);
    framework.addInstance(MetadataDataset.class.getName(), SYSTEM_METADATA_INSTANCE_ID, DatasetProperties.EMPTY);
  }
}
