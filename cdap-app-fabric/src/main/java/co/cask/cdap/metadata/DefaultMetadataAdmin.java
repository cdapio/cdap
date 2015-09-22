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

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.InvalidMetadataException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataDataset;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataRecord;
import co.cask.cdap.data2.metadata.service.BusinessMetadataStore;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of {@link MetadataAdmin} that interacts directly with {@link BusinessMetadataStore}
 */
public class DefaultMetadataAdmin implements MetadataAdmin {
  private static final CharMatcher keywordMatcher = CharMatcher.inRange('A', 'Z')
    .or(CharMatcher.inRange('a', 'z'))
    .or(CharMatcher.inRange('0', '9'))
    .or(CharMatcher.is('_')
          .or(CharMatcher.is('-')));

  private final BusinessMetadataStore businessMds;
  private final CConfiguration cConf;
  private final EntityValidator entityValidator;

  @Inject
  DefaultMetadataAdmin(AbstractNamespaceClient namespaceClient, BusinessMetadataStore businessMds,
                       CConfiguration cConf, Store store, DatasetFramework datasetFramework,
                       StreamAdmin streamAdmin) {
    this.businessMds = businessMds;
    this.cConf = cConf;
    this.entityValidator = new EntityValidator(namespaceClient, store, datasetFramework, streamAdmin);
  }

  @Override
  public void addProperties(Id.NamespacedId entityId, Map<String, String> properties)
    throws NotFoundException, InvalidMetadataException {
    entityValidator.ensureEntityExists(entityId);
    validateProperties(entityId, properties);
    businessMds.setProperties(entityId, properties);
  }

  @Override
  public void addTags(Id.NamespacedId entityId, String... tags) throws NotFoundException, InvalidMetadataException {
    entityValidator.ensureEntityExists(entityId);
    validateTags(entityId, tags);
    businessMds.addTags(entityId, tags);
  }

  @Override
  public Set<MetadataRecord> getMetadata(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    // For now, we only return business metadata
    return ImmutableSet.of(businessMds.getMetadata(entityId));
  }

  @Override
  public Map<String, String> getProperties(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    return businessMds.getProperties(entityId);
  }

  @Override
  public Set<String> getTags(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    return businessMds.getTags(entityId);
  }

  @Override
  public void removeMetadata(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    businessMds.removeMetadata(entityId);
  }

  @Override
  public void removeProperties(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    businessMds.removeProperties(entityId);
  }

  @Override
  public void removeProperties(Id.NamespacedId entityId, String... keys) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    businessMds.removeProperties(entityId, keys);
  }

  @Override
  public void removeTags(Id.NamespacedId entityId) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    businessMds.removeTags(entityId);
  }

  @Override
  public void removeTags(Id.NamespacedId entityId, String... tags) throws NotFoundException {
    entityValidator.ensureEntityExists(entityId);
    businessMds.removeTags(entityId, tags);
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadata(String namespaceId, String searchQuery,
                                                        @Nullable final MetadataSearchTargetType type)
    throws NotFoundException {
    Iterable<BusinessMetadataRecord> results;
    if (type == null) {
      results = businessMds.searchMetadata(namespaceId, searchQuery);
    } else {
      results = businessMds.searchMetadataOnType(namespaceId, searchQuery, type);
    }

    Set<MetadataSearchResultRecord> searchResultRecords = new LinkedHashSet<>();
    for (BusinessMetadataRecord bmr : results) {
      MetadataSearchResultRecord msr = new MetadataSearchResultRecord(bmr.getTargetId());
      searchResultRecords.add(msr);
    }
    return searchResultRecords;
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

  // Validate the key should not be be reserved {@link BusinessMetadataDataset.TAGS_KEY}
  private void validateTagReservedKey(Id.NamespacedId entityId, String key) throws InvalidMetadataException {
    if (BusinessMetadataDataset.TAGS_KEY.equals(key.toLowerCase())) {
      throw new InvalidMetadataException(entityId,
                                  "Could not set metadata with reserved key " + BusinessMetadataDataset.TAGS_KEY);
    }
  }

  private void validateAllowedFormat(Id.NamespacedId entityId, String keyword) throws InvalidMetadataException {
    if (!keywordMatcher.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(entityId, "Illegal format for the value : " + keyword);
    }
  }

  private void validateLength(Id.NamespacedId entityId, String keyword) throws InvalidMetadataException {
    // check for max char per value
    if (keyword.length() > cConf.getInt(Constants.Metadata.MAX_CHARS_ALLOWED)) {
      throw new InvalidMetadataException(entityId, "Metadata " + keyword + " should not exceed maximum of " +
        cConf.get(Constants.Metadata.MAX_CHARS_ALLOWED) + " characters.");
    }
  }
}
