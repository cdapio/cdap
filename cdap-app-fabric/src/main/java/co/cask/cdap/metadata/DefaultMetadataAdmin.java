/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.common.InvalidMetadataException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link MetadataAdmin} that interacts directly with {@link MetadataStore}.
 */
public class DefaultMetadataAdmin implements MetadataAdmin {
  private static final CharMatcher KEY_AND_TAG_MATCHER = CharMatcher.inRange('A', 'Z')
    .or(CharMatcher.inRange('a', 'z'))
    .or(CharMatcher.inRange('0', '9'))
    .or(CharMatcher.is('_'))
    .or(CharMatcher.is('-'));

  private static final CharMatcher VALUE_MATCHER = CharMatcher.inRange('A', 'Z')
    .or(CharMatcher.inRange('a', 'z'))
    .or(CharMatcher.inRange('0', '9'))
    .or(CharMatcher.is('_'))
    .or(CharMatcher.is('-'))
    .or(CharMatcher.WHITESPACE);

  private final MetadataStore metadataStore;
  private final CConfiguration cConf;
  private final EntityExistenceVerifier entityExistenceVerifier;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  DefaultMetadataAdmin(MetadataStore metadataStore, CConfiguration cConf,
                       EntityExistenceVerifier entityExistenceVerifier,
                       AuthorizationEnforcer authorizationEnforcer,
                       AuthenticationContext authenticationContext) {
    this.metadataStore = metadataStore;
    this.cConf = cConf;
    this.entityExistenceVerifier = entityExistenceVerifier;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @Override
  public void addProperties(Id.NamespacedId entityId, Map<String, String> properties)
    throws NotFoundException, InvalidMetadataException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    validateProperties(entityId, properties);
    metadataStore.setProperties(MetadataScope.USER, entityId, properties);
  }

  @Override
  public void addTags(Id.NamespacedId entityId, String... tags) throws NotFoundException, InvalidMetadataException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    validateTags(entityId, tags);
    metadataStore.addTags(MetadataScope.USER, entityId, tags);
  }

  @Override
  public Set<MetadataRecord> getMetadata(Id.NamespacedId entityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    return metadataStore.getMetadata(entityId);
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataScope scope, Id.NamespacedId entityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    return ImmutableSet.of(metadataStore.getMetadata(scope, entityId));
  }

  @Override
  public Map<String, String> getProperties(Id.NamespacedId entityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    return metadataStore.getProperties(entityId);
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, Id.NamespacedId entityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    return metadataStore.getProperties(scope, entityId);
  }

  @Override
  public Set<String> getTags(Id.NamespacedId entityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    return metadataStore.getTags(entityId);
  }

  @Override
  public Set<String> getTags(MetadataScope scope, Id.NamespacedId entityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    return metadataStore.getTags(scope, entityId);
  }

  @Override
  public void removeMetadata(Id.NamespacedId entityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    metadataStore.removeMetadata(MetadataScope.USER, entityId);
  }

  @Override
  public void removeProperties(Id.NamespacedId entityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    metadataStore.removeProperties(MetadataScope.USER, entityId);
  }

  @Override
  public void removeProperties(Id.NamespacedId entityId, String... keys) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    metadataStore.removeProperties(MetadataScope.USER, entityId, keys);
  }

  @Override
  public void removeTags(Id.NamespacedId entityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    metadataStore.removeTags(MetadataScope.USER, entityId);
  }

  @Override
  public void removeTags(Id.NamespacedId entityId, String... tags) throws NotFoundException {
    entityExistenceVerifier.ensureExists(entityId.toEntityId());
    metadataStore.removeTags(MetadataScope.USER, entityId, tags);
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadata(String namespaceId, String searchQuery,
                                                        Set<MetadataSearchTargetType> types) throws Exception {

    return filterAuthorizedSearchResult(metadataStore.searchMetadataOnType(namespaceId, searchQuery, types));
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadata(MetadataScope scope, String namespaceId, String searchQuery,
                                                        Set<MetadataSearchTargetType> types) throws Exception {
    return filterAuthorizedSearchResult(metadataStore.searchMetadataOnType(scope, namespaceId, searchQuery, types));
  }

  /**
   * Filter a list of {@link MetadataSearchResultRecord} that ensures the logged-in user has a privilege on
   *
   * @param results the {@link Set<MetadataSearchResultRecord>} to filter with
   * @return filtered list of {@link MetadataSearchResultRecord}
   */
  private Set<MetadataSearchResultRecord> filterAuthorizedSearchResult(Set<MetadataSearchResultRecord> results)
    throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    return ImmutableSet.copyOf(
      Iterables.filter(results, new com.google.common.base.Predicate<MetadataSearchResultRecord>() {
        @Override
        public boolean apply(MetadataSearchResultRecord metadataSearchResultRecord) {
          return filter.apply(metadataSearchResultRecord.getEntityId().toEntityId());
        }
      })
    );
  }

  // Helper methods to validate the metadata entries.

  private void validateProperties(Id.NamespacedId entityId,
                                  Map<String, String> properties) throws InvalidMetadataException {
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      // validate key
      validateKeyAndTagsFormat(entityId, entry.getKey());
      validateTagReservedKey(entityId, entry.getKey());
      validateLength(entityId, entry.getKey());

      // validate value
      validateValueFormat(entityId, entry.getValue());
      validateLength(entityId, entry.getValue());
    }
  }

  private void validateTags(Id.NamespacedId entityId, String... tags) throws InvalidMetadataException {
    for (String tag : tags) {
      validateKeyAndTagsFormat(entityId, tag);
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
   * Validate the key matches the {@link #KEY_AND_TAG_MATCHER} character test.
   */
  private void validateKeyAndTagsFormat(Id.NamespacedId entityId, String keyword) throws InvalidMetadataException {
    if (!KEY_AND_TAG_MATCHER.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(entityId, "Illegal format for the value : " + keyword);
    }
  }

  /**
   * Validate the value of a property matches the {@link #VALUE_MATCHER} character test.
   */
  private void validateValueFormat(Id.NamespacedId entityId, String keyword) throws InvalidMetadataException {
    if (!VALUE_MATCHER.matchesAllOf(keyword)) {
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
