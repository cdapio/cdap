/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.InvalidMetadataException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link MetadataAdmin} that interacts directly with {@link MetadataStore}.
 */
public class DefaultMetadataAdmin implements MetadataAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataAdmin.class);

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
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  DefaultMetadataAdmin(MetadataStore metadataStore, CConfiguration cConf,
                       AuthorizationEnforcer authorizationEnforcer,
                       AuthenticationContext authenticationContext) {
    this.metadataStore = metadataStore;
    this.cConf = cConf;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @Override
  public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties)
    throws InvalidMetadataException {
    validateProperties(metadataEntity, properties);
    metadataStore.setProperties(MetadataScope.USER, metadataEntity, properties);
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, String... tags) throws InvalidMetadataException {
    validateTags(metadataEntity, tags);
    metadataStore.addTags(MetadataScope.USER, metadataEntity, tags);
  }

  @Override
  public Set<MetadataRecordV2> getMetadata(MetadataEntity metadataEntity) {
    return metadataStore.getMetadata(metadataEntity);
  }

  @Override
  public Set<MetadataRecordV2> getMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
    return ImmutableSet.of(metadataStore.getMetadata(scope, metadataEntity));
  }

  @Override
  public Map<String, String> getProperties(MetadataEntity metadataEntity) {
    return metadataStore.getProperties(metadataEntity);
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, MetadataEntity metadataEntity) {
    return metadataStore.getProperties(scope, metadataEntity);
  }

  @Override
  public Set<String> getTags(MetadataEntity metadataEntity) {
    return metadataStore.getTags(metadataEntity);
  }

  @Override
  public Set<String> getTags(MetadataScope scope, MetadataEntity metadataEntity) {
    return metadataStore.getTags(scope, metadataEntity);
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) {
    metadataStore.removeMetadata(MetadataScope.USER, metadataEntity);
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity) {
    metadataStore.removeProperties(MetadataScope.USER, metadataEntity);
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity, String... keys) {
    metadataStore.removeProperties(MetadataScope.USER, metadataEntity, keys);
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity) {
    metadataStore.removeTags(MetadataScope.USER, metadataEntity);
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity, String... tags) {
    metadataStore.removeTags(MetadataScope.USER, metadataEntity, tags);
  }

  @Override
  public MetadataSearchResponse search(String namespaceId, String searchQuery,
                                       Set<EntityTypeSimpleName> types,
                                       SortInfo sortInfo, int offset, int limit,
                                       int numCursors, String cursor, boolean showHidden,
                                       Set<EntityScope> entityScope) throws Exception {
    return filterAuthorizedSearchResult(
      metadataStore.search(namespaceId, searchQuery, types, sortInfo, offset, limit, numCursors, cursor, showHidden,
                           entityScope)
    );
  }

  /**
   * Filter a list of {@link MetadataSearchResultRecord} that ensures the logged-in user has a privilege on
   *
   * @param results the {@link MetadataSearchResponse} to filter
   * @return filtered {@link MetadataSearchResponse}
   */
  private MetadataSearchResponse filterAuthorizedSearchResult(final MetadataSearchResponse results) throws Exception {
    return new MetadataSearchResponse(
      results.getSort(), results.getOffset(), results.getLimit(), results.getNumCursors(), results.getTotal(),
      ImmutableSet.copyOf(
        AuthorizationUtil.isVisible(results.getResults(), authorizationEnforcer, authenticationContext.getPrincipal(),
                                    MetadataSearchResultRecord::getEntityId, null)),
      results.getCursors(), results.isShowHidden(), results.getEntityScope());
  }

  // Helper methods to validate the metadata entries.

  private void validateProperties(MetadataEntity metadataEntity,
                                  Map<String, String> properties) throws InvalidMetadataException {
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      // validate key
      validateKeyAndTagsFormat(metadataEntity, entry.getKey());
      validateTagReservedKey(metadataEntity, entry.getKey());
      validateLength(metadataEntity, entry.getKey());

      // validate value
      validateValueFormat(metadataEntity, entry.getValue());
      validateLength(metadataEntity, entry.getValue());
    }
  }

  private void validateTags(MetadataEntity metadataEntity, String... tags) throws InvalidMetadataException {
    for (String tag : tags) {
      validateKeyAndTagsFormat(metadataEntity, tag);
      validateLength(metadataEntity, tag);
    }
  }

  /**
   * Validate that the key is not reserved {@link MetadataDataset#TAGS_KEY}.
   */
  private void validateTagReservedKey(MetadataEntity metadataEntity, String key)
    throws InvalidMetadataException {
    if (MetadataDataset.TAGS_KEY.equals(key.toLowerCase())) {
      throw new InvalidMetadataException(metadataEntity,
                                         "Could not set metadata with reserved key " + MetadataDataset.TAGS_KEY);
    }
  }

  /**
   * Validate the key matches the {@link #KEY_AND_TAG_MATCHER} character test.
   */
  private void validateKeyAndTagsFormat(MetadataEntity metadataEntity, String keyword)
    throws InvalidMetadataException {
    if (!KEY_AND_TAG_MATCHER.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(metadataEntity, "Illegal format for the value : " + keyword);
    }
  }

  /**
   * Validate the value of a property matches the {@link #VALUE_MATCHER} character test.
   */
  private void validateValueFormat(MetadataEntity metadataEntity, String keyword)
    throws InvalidMetadataException {
    if (!VALUE_MATCHER.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(metadataEntity, "Illegal format for the value : " + keyword);
    }
  }

  /**
   * Validate that the key length does not exceed the configured limit.
   */
  private void validateLength(MetadataEntity metadataEntity, String keyword) throws InvalidMetadataException {
    // check for max char per value
    if (keyword.length() > cConf.getInt(Constants.Metadata.MAX_CHARS_ALLOWED)) {
      throw new InvalidMetadataException(metadataEntity, "Metadata " + keyword + " should not exceed maximum of " +
        cConf.get(Constants.Metadata.MAX_CHARS_ALLOWED) + " characters.");
    }
  }
}
