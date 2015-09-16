/*
 * Copyright 2015 Cask Data, Inc.
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
package co.cask.cdap.data2.metadata.service;

import co.cask.cdap.data2.metadata.dataset.BusinessMetadataDataset;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link BusinessMetadataStore} used in memory mode.
 */
public class InMemoryBusinessMetadataStore implements BusinessMetadataStore {
  private static final String TAGS_KEY = "tags";
  private static final String TAGS_SEPARATOR = ",";

  private final ConcurrentHashMap<Id.NamespacedId, Map<String, String>> metadata;
  private final ConcurrentHashMap<String, Set<Id.NamespacedId>> invertedIndex;

  @Inject
  public InMemoryBusinessMetadataStore() {
    this.metadata = new ConcurrentHashMap<>();
    this.invertedIndex = new ConcurrentHashMap<>();
  }

  @Override
  public void setProperties(Id.NamespacedId entityId, Map<String, String> properties) {
    metadata.putIfAbsent(entityId, new HashMap<String, String>());
    Map<String, String> metadataForEntity = metadata.get(entityId);
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      // Get existing ones
      if(metadataForEntity.containsKey(entry.getKey())) {
        continue;
      }

      metadataForEntity.put(entry.getKey(), entry.getValue());

      invertedIndex.putIfAbsent(entry.getKey(), new HashSet<Id.NamespacedId> ());
      Set<Id.NamespacedId> entities = invertedIndex.get(entry.getKey());
      entities.add(entityId);

      String kv = createKeyValue(entry.getKey(), entry.getValue());
      invertedIndex.putIfAbsent(kv, new HashSet<Id.NamespacedId>());
      Set<Id.NamespacedId> entitiesKV = invertedIndex.get(kv);
      entitiesKV.add(entityId);
    }
  }

  private String createKeyValue(String key, String value) {
    return key + BusinessMetadataDataset.KEYVALUE_SEPARATOR + value;
  }


  @Override
  public void addTags(Id.NamespacedId entityId, String... tagsToAdd) {
    metadata.putIfAbsent(entityId, new HashMap<String, String>());
    Map<String, String> metadataForEntity = metadata.get(entityId);
    if (!metadataForEntity.containsKey(TAGS_KEY)) {
      String tags = Joiner.on(TAGS_SEPARATOR).join(tagsToAdd);
      metadataForEntity.put(TAGS_KEY, tags);
    } else {
      String[] existingTags = (metadataForEntity.get(TAGS_KEY)).split(TAGS_SEPARATOR);
      Iterable<String> newTags = Iterables.concat(Arrays.asList(existingTags), Arrays.asList(tagsToAdd));
      metadataForEntity.put(TAGS_KEY, Joiner.on(TAGS_SEPARATOR).join(newTags));
    }

    invertedIndex.putIfAbsent(TAGS_KEY, new HashSet<Id.NamespacedId>());
    invertedIndex.get(TAGS_KEY).add(entityId);
  }

  @Override
  public MetadataRecord getMetadata(Id.NamespacedId entityId) {
    Map<String, String> all = metadata.get(entityId);
    Map<String, String> properties = new HashMap<>();
    Set<String> tagsAsSet = new HashSet<>();

    if (all != null) {
      for (Map.Entry<String, String> entry : all.entrySet()) {
        if (entry.getKey().equals(TAGS_KEY)) {
          String[] tags = entry.getValue().split(",");
          tagsAsSet.addAll(Arrays.asList(tags));
        } else {
          properties.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return new MetadataRecord(entityId, properties, tagsAsSet);
  }

  @Override
  public Set<MetadataRecord> getMetadata(Set<Id.NamespacedId> entityIds) {
    Set<MetadataRecord> allRecords = new HashSet<>();
    for (Id.NamespacedId entityId : entityIds) {
      MetadataRecord record = getMetadata(entityId);
      allRecords.add(record);
    }
    return allRecords;
  }

  @Override
  public Map<String, String> getProperties(Id.NamespacedId entityId) {
    Map<String, String> all = metadata.get(entityId);
    Map<String, String> properties = new HashMap<>();
    if (all != null) {
      for (Map.Entry<String, String> entry : all.entrySet()) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }
    return properties;
  }

  @Override
  public Set<String> getTags(Id.NamespacedId entityId) {
    Map<String, String> all = metadata.get(entityId);
    Set<String> tagsAsSet = new HashSet<>();

    if (all != null) {
      for (Map.Entry<String, String> entry : all.entrySet()) {
        if (entry.getKey().equals(TAGS_KEY)) {
          String[] tags = entry.getValue().split(",");
          tagsAsSet.addAll(Arrays.asList(tags));
          break;
        }
      }
    }
    return tagsAsSet;
  }

  @Override
  public void removeMetadata(Id.NamespacedId entityId) {
    metadata.remove(entityId);
  }

  @Override
  public void removeProperties(Id.NamespacedId entityId) {
    Map<String, String> all = metadata.get(entityId);
    String tags = all.get(TAGS_KEY);
    all.clear();
    if (tags != null) {
      all.put(TAGS_KEY, tags);
    }
  }

  @Override
  public void removeProperties(Id.NamespacedId entityId, String... keys) {
    Map<String, String> all = metadata.get(entityId);
    for (String key : keys) {
      all.remove(key);
    }
  }

  @Override
  public void removeTags(Id.NamespacedId entityId) {
    Map<String, String> all = metadata.get(entityId);
    all.remove(TAGS_KEY);
  }

  @Override
  public void removeTags(Id.NamespacedId entityId, String... tagsToRemove) {
    Map<String, String> all = metadata.get(entityId);
    String tags = all.get(TAGS_KEY);
    if (tags != null) {
      Set<String> tagsAsSet= new HashSet<>((Arrays.asList(tags.split(","))));
      for (String tagToRemove : tagsToRemove) {
        tagsAsSet.remove(tagToRemove);
      }
      all.put(TAGS_KEY, Joiner.on(TAGS_SEPARATOR).join(tagsAsSet));
    }
  }

  @Override
  public Iterable<BusinessMetadataRecord> searchMetadata(String searchQuery) {
    List<BusinessMetadataRecord> records = new LinkedList<>();

    Set<Id.NamespacedId> entityIds = invertedIndex.get(searchQuery);
    if (entityIds == null) {
      return records;
    }

    for (Id.NamespacedId entityId : entityIds) {
      Map<String, String> all = metadata.get(entityId);
      for (Map.Entry<String, String> entry : all.entrySet()) {
        records.add(new BusinessMetadataRecord(entityId, entry.getKey(), entry.getValue()));
      }
    }
    return records;
  }

  @Override
  public Iterable<BusinessMetadataRecord> searchMetadataOnType(String searchQuery, MetadataSearchTargetType type) {
    Iterable<BusinessMetadataRecord> records = searchMetadata(searchQuery);
    // TODO Filter based on target type
    return records;
  }
}
