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
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
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
  public void addProperties(NamespacedEntityId namespacedEntityId, Map<String, String> properties)
    throws NotFoundException, InvalidMetadataException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    validateProperties(namespacedEntityId, properties);
    metadataStore.setProperties(MetadataScope.USER, namespacedEntityId, properties);
  }

  @Override
  public void addTags(NamespacedEntityId namespacedEntityId, String... tags)
    throws NotFoundException, InvalidMetadataException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    validateTags(namespacedEntityId, tags);
    metadataStore.addTags(MetadataScope.USER, namespacedEntityId, tags);
  }

  @Override
  public Set<MetadataRecord> getMetadata(NamespacedEntityId namespacedEntityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    return metadataStore.getMetadata(namespacedEntityId);
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataScope scope, NamespacedEntityId namespacedEntityId)
    throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    return ImmutableSet.of(metadataStore.getMetadata(scope, namespacedEntityId));
  }

  @Override
  public Map<String, String> getProperties(NamespacedEntityId namespacedEntityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    return metadataStore.getProperties(namespacedEntityId);
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId)
    throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    return metadataStore.getProperties(scope, namespacedEntityId);
  }

  @Override
  public Set<String> getTags(NamespacedEntityId namespacedEntityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    return metadataStore.getTags(namespacedEntityId);
  }

  @Override
  public Set<String> getTags(MetadataScope scope, NamespacedEntityId namespacedEntityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    return metadataStore.getTags(scope, namespacedEntityId);
  }

  @Override
  public void removeMetadata(NamespacedEntityId namespacedEntityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    metadataStore.removeMetadata(MetadataScope.USER, namespacedEntityId);
  }

  @Override
  public void removeProperties(NamespacedEntityId namespacedEntityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    metadataStore.removeProperties(MetadataScope.USER, namespacedEntityId);
  }

  @Override
  public void removeProperties(NamespacedEntityId namespacedEntityId, String... keys) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    metadataStore.removeProperties(MetadataScope.USER, namespacedEntityId, keys);
  }

  @Override
  public void removeTags(NamespacedEntityId namespacedEntityId) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    metadataStore.removeTags(MetadataScope.USER, namespacedEntityId);
  }

  @Override
  public void removeTags(NamespacedEntityId namespacedEntityId, String... tags) throws NotFoundException {
    entityExistenceVerifier.ensureExists(namespacedEntityId);
    metadataStore.removeTags(MetadataScope.USER, namespacedEntityId, tags);
  }

  @Override
  public MetadataSearchResponse search(String namespaceId, String searchQuery,
                                       Set<MetadataSearchTargetType> types,
                                       SortInfo sortInfo, int offset, int limit,
                                       int numCursors, String cursor) throws Exception {
    return filterAuthorizedSearchResult(
      metadataStore.search(namespaceId, searchQuery, types, sortInfo, offset, limit, numCursors, cursor)
    );
  }

  /**
   * Filter a list of {@link MetadataSearchResultRecord} that ensures the logged-in user has a privilege on
   *
   * @param results the {@link MetadataSearchResponse} to filter
   * @return filtered {@link MetadataSearchResponse}
   */
  private MetadataSearchResponse filterAuthorizedSearchResult(MetadataSearchResponse results)
    throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    return new MetadataSearchResponse(
      results.getSort(), results.getOffset(), results.getLimit(), results.getNumCursors(), results.getTotal(),
      ImmutableSet.copyOf(
        Iterables.filter(results.getResults(), new com.google.common.base.Predicate<MetadataSearchResultRecord>() {
          @Override
          public boolean apply(MetadataSearchResultRecord metadataSearchResultRecord) {
            return filter.apply(metadataSearchResultRecord.getEntityId());
          }
        })
      ),
      results.getCursors());
  }

  // Helper methods to validate the metadata entries.

  private void validateProperties(NamespacedEntityId namespacedEntityId,
                                  Map<String, String> properties) throws InvalidMetadataException {
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      // validate key
      validateKeyAndTagsFormat(namespacedEntityId, entry.getKey());
      validateTagReservedKey(namespacedEntityId, entry.getKey());
      validateLength(namespacedEntityId, entry.getKey());

      // validate value
      validateValueFormat(namespacedEntityId, entry.getValue());
      validateLength(namespacedEntityId, entry.getValue());
    }
  }

  private void validateTags(NamespacedEntityId namespacedEntityId, String... tags) throws InvalidMetadataException {
    for (String tag : tags) {
      validateKeyAndTagsFormat(namespacedEntityId, tag);
      validateLength(namespacedEntityId, tag);
    }
  }

  /**
   * Validate that the key is not reserved {@link MetadataDataset#TAGS_KEY}.
   */
  private void validateTagReservedKey(NamespacedEntityId namespacedEntityId, String key)
    throws InvalidMetadataException {
    if (MetadataDataset.TAGS_KEY.equals(key.toLowerCase())) {
      throw new InvalidMetadataException(namespacedEntityId,
                                         "Could not set metadata with reserved key " + MetadataDataset.TAGS_KEY);
    }
  }

  /**
   * Validate the key matches the {@link #KEY_AND_TAG_MATCHER} character test.
   */
  private void validateKeyAndTagsFormat(NamespacedEntityId namespacedEntityId, String keyword)
    throws InvalidMetadataException {
    if (!KEY_AND_TAG_MATCHER.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(namespacedEntityId, "Illegal format for the value : " + keyword);
    }
  }

  /**
   * Validate the value of a property matches the {@link #VALUE_MATCHER} character test.
   */
  private void validateValueFormat(NamespacedEntityId namespacedEntityId, String keyword)
    throws InvalidMetadataException {
    if (!VALUE_MATCHER.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(namespacedEntityId, "Illegal format for the value : " + keyword);
    }
  }

  /**
   * Validate that the key length does not exceed the configured limit.
   */
  private void validateLength(NamespacedEntityId namespacedEntityId, String keyword) throws InvalidMetadataException {
    // check for max char per value
    if (keyword.length() > cConf.getInt(Constants.Metadata.MAX_CHARS_ALLOWED)) {
      throw new InvalidMetadataException(namespacedEntityId, "Metadata " + keyword + " should not exceed maximum of " +
        cConf.get(Constants.Metadata.MAX_CHARS_ALLOWED) + " characters.");
    }
  }
}
