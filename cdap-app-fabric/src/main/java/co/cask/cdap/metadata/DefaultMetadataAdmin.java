/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.common.InvalidMetadataException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of {@link MetadataAdmin} that interacts directly with {@link MetadataStore}.
 */
public class DefaultMetadataAdmin implements MetadataAdmin {
  private static final CharMatcher keywordMatcher = CharMatcher.inRange('A', 'Z')
    .or(CharMatcher.inRange('a', 'z'))
    .or(CharMatcher.inRange('0', '9'))
    .or(CharMatcher.is('_')
          .or(CharMatcher.is('-')));

  private final MetadataStore metadataStore;
  private final CConfiguration cConf;
  private final EntityValidator entityValidator;

  @Inject
  DefaultMetadataAdmin(MetadataStore metadataStore, CConfiguration cConf, EntityValidator entityValidator) {
    this.metadataStore = metadataStore;
    this.cConf = cConf;
    this.entityValidator = entityValidator;
  }

  @Override
  public void addProperties(Id.NamespacedId entityId, Map<String, String> properties)
    throws NotFoundException, InvalidMetadataException {
    entityValidator.ensureEntityExists(entityId);
    validateProperties(entityId, properties);
    metadataStore.setProperties(MetadataScope.USER, entityId, properties);
  }

  @Override
  public void addTags(Id.NamespacedId entityId, String... tags) throws NotFoundException, InvalidMetadataException {
    entityValidator.ensureEntityExists(entityId);
    validateTags(entityId, tags);
    metadataStore.addTags(MetadataScope.USER, entityId, tags);
  }

  @Override
  public Set<MetadataRecord> getMetadata(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    return metadataStore.getMetadata(entityId);
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataScope scope, Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    return ImmutableSet.of(metadataStore.getMetadata(scope, entityId));
  }

  @Override
  public Map<String, String> getProperties(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    return metadataStore.getProperties(entityId);
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    return metadataStore.getProperties(scope, entityId);
  }

  @Override
  public Set<String> getTags(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    return metadataStore.getTags(entityId);
  }

  @Override
  public Set<String> getTags(MetadataScope scope, Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    return metadataStore.getTags(scope, entityId);
  }

  @Override
  public void removeMetadata(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    metadataStore.removeMetadata(MetadataScope.USER, entityId);
  }

  @Override
  public void removeProperties(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    metadataStore.removeProperties(MetadataScope.USER, entityId);
  }

  @Override
  public void removeProperties(Id.NamespacedId entityId, String... keys) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    metadataStore.removeProperties(MetadataScope.USER, entityId, keys);
  }

  @Override
  public void removeTags(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    metadataStore.removeTags(MetadataScope.USER, entityId);
  }

  @Override
  public void removeTags(Id.NamespacedId entityId, String... tags) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    metadataStore.removeTags(MetadataScope.USER, entityId, tags);
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadata(String namespaceId, String searchQuery,
                                                        @Nullable MetadataSearchTargetType type) {
    if (type == null) {
      return metadataStore.searchMetadata(namespaceId, searchQuery);
    }
    return metadataStore.searchMetadataOnType(namespaceId, searchQuery, type);
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadata(MetadataScope scope, String namespaceId, String searchQuery,
                                                        @Nullable final MetadataSearchTargetType type) {
    if (type == null) {
      return metadataStore.searchMetadata(scope, namespaceId, searchQuery);
    }
    return metadataStore.searchMetadataOnType(scope, namespaceId, searchQuery, type);
  }

  // Helper methods to validate the metadata entries.

  private void validateProperties(Id.NamespacedId entityId,
                                  Map<String, String> properties) throws InvalidMetadataException {
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      // validate key
      validateAllowedFormat(entityId, entry.getKey());
      validateTagReservedKey(entityId, entry.getKey());
      validateLength(entityId, entry.getKey());

      // validate value
      validateAllowedFormat(entityId, entry.getValue());
      validateLength(entityId, entry.getValue());
    }
  }

  public void validateTags(Id.NamespacedId entityId, String ... tags) throws InvalidMetadataException {
    for (String tag : tags) {
      validateAllowedFormat(entityId, tag);
      validateLength(entityId, tag);
    }
  }

  /**
   * Validate that the key is not reserved {@link MetadataDataset#TAGS_KEY}.
   */
  private void validateTagReservedKey(Id.NamespacedId entityId, String key) throws InvalidMetadataException {
    if (MetadataDataset.TAGS_KEY.equals(key.toLowerCase())) {
      throw new InvalidMetadataException(entityId,
                                  "Could not set metadata with reserved key " + MetadataDataset.TAGS_KEY);
    }
  }

  /**
   * Validate the key matches the {@link #keywordMatcher} character test.
   */
  private void validateAllowedFormat(Id.NamespacedId entityId, String keyword) throws InvalidMetadataException {
    if (!keywordMatcher.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(entityId, "Illegal format for the value : " + keyword);
    }
  }

  /**
   * Validate that the key length does not exceed the configured limit.
   */
  private void validateLength(Id.NamespacedId entityId, String keyword) throws InvalidMetadataException {
    // check for max char per value
    if (keyword.length() > cConf.getInt(Constants.Metadata.MAX_CHARS_ALLOWED)) {
      throw new InvalidMetadataException(entityId, "Metadata " + keyword + " should not exceed maximum of " +
        cConf.get(Constants.Metadata.MAX_CHARS_ALLOWED) + " characters.");
    }
  }
}
