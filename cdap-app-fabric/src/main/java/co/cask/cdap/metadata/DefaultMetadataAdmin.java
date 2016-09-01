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
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespacedId;
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
  public void addProperties(NamespacedId namespacedId, Map<String, String> properties)
    throws NotFoundException, InvalidMetadataException {
    entityExistenceVerifier.ensureExists(namespacedId);
    validateProperties(namespacedId, properties);
    metadataStore.setProperties(MetadataScope.USER, namespacedId, properties);
  }

  @Override
  public void addTags(NamespacedId namespacedId, String... tags) throws NotFoundException, InvalidMetadataException {
    entityExistenceVerifier.ensureExists(namespacedId);
    validateTags(namespacedId, tags);
    metadataStore.addTags(MetadataScope.USER, namespacedId, tags);
  }

  @Override
  public Set<MetadataRecord> getMetadata(NamespacedId namespacedId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    return metadataStore.getMetadata(namespacedId);
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataScope scope, NamespacedId namespacedId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    return ImmutableSet.of(metadataStore.getMetadata(scope, namespacedId));
  }

  @Override
  public Map<String, String> getProperties(NamespacedId namespacedId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    return metadataStore.getProperties(namespacedId);
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, NamespacedId namespacedId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    return metadataStore.getProperties(scope, namespacedId);
  }

  @Override
  public Set<String> getTags(NamespacedId namespacedId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    return metadataStore.getTags(namespacedId);
  }

  @Override
  public Set<String> getTags(MetadataScope scope, NamespacedId namespacedId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    return metadataStore.getTags(scope, namespacedId);
  }

  @Override
  public void removeMetadata(NamespacedId namespacedId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    metadataStore.removeMetadata(MetadataScope.USER, namespacedId);
  }

  @Override
  public void removeProperties(NamespacedId namespacedId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    metadataStore.removeProperties(MetadataScope.USER, namespacedId);
  }

  @Override
  public void removeProperties(NamespacedId namespacedId, String... keys) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    metadataStore.removeProperties(MetadataScope.USER, namespacedId, keys);
  }

  @Override
  public void removeTags(NamespacedId namespacedId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    metadataStore.removeTags(MetadataScope.USER, namespacedId);
  }

  @Override
  public void removeTags(NamespacedId namespacedId, String... tags) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedId);
    metadataStore.removeTags(MetadataScope.USER, namespacedId, tags);
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
          return filter.apply(metadataSearchResultRecord.getEntityId());
        }
      })
    );
  }

  // Helper methods to validate the metadata entries.

  private void validateProperties(NamespacedId namespacedId,
                                  Map<String, String> properties) throws InvalidMetadataException {
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      // validate key
      validateKeyAndTagsFormat(namespacedId, entry.getKey());
      validateTagReservedKey(namespacedId, entry.getKey());
      validateLength(namespacedId, entry.getKey());

      // validate value
      validateValueFormat(namespacedId, entry.getValue());
      validateLength(namespacedId, entry.getValue());
    }
  }

  private void validateTags(NamespacedId namespacedId, String... tags) throws InvalidMetadataException {
    for (String tag : tags) {
      validateKeyAndTagsFormat(namespacedId, tag);
      validateLength(namespacedId, tag);
    }
  }

  /**
   * Validate that the key is not reserved {@link MetadataDataset#TAGS_KEY}.
   */
  private void validateTagReservedKey(NamespacedId namespacedId, String key) throws InvalidMetadataException {
    if (MetadataDataset.TAGS_KEY.equals(key.toLowerCase())) {
      throw new InvalidMetadataException(namespacedId,
                                  "Could not set metadata with reserved key " + MetadataDataset.TAGS_KEY);
    }
  }

  /**
   * Validate the key matches the {@link #KEY_AND_TAG_MATCHER} character test.
   */
  private void validateKeyAndTagsFormat(NamespacedId namespacedId, String keyword) throws InvalidMetadataException {
    if (!KEY_AND_TAG_MATCHER.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(namespacedId, "Illegal format for the value : " + keyword);
    }
  }

  /**
   * Validate the value of a property matches the {@link #VALUE_MATCHER} character test.
   */
  private void validateValueFormat(NamespacedId namespacedId, String keyword) throws InvalidMetadataException {
    if (!VALUE_MATCHER.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(namespacedId, "Illegal format for the value : " + keyword);
    }
  }

  /**
   * Validate that the key length does not exceed the configured limit.
   */
  private void validateLength(NamespacedId namespacedId, String keyword) throws InvalidMetadataException {
    // check for max char per value
    if (keyword.length() > cConf.getInt(Constants.Metadata.MAX_CHARS_ALLOWED)) {
      throw new InvalidMetadataException(namespacedId, "Metadata " + keyword + " should not exceed maximum of " +
        cConf.get(Constants.Metadata.MAX_CHARS_ALLOWED) + " characters.");
    }
  }
}
