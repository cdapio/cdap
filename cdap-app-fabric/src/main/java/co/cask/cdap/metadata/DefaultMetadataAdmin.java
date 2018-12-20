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
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.metadata.MetadataSearchResponseV2;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecordV2;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link MetadataAdmin} that interacts directly with {@link MetadataStore}.
 */
public class DefaultMetadataAdmin extends MetadataValidator implements MetadataAdmin {

  private final MetadataStore metadataStore;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  DefaultMetadataAdmin(MetadataStore metadataStore, CConfiguration cConf,
                       AuthorizationEnforcer authorizationEnforcer,
                       AuthenticationContext authenticationContext) {
    super(cConf);
    this.metadataStore = metadataStore;
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
  public void addTags(MetadataEntity metadataEntity, Set<String> tags) throws InvalidMetadataException {
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
  public void removeProperties(MetadataEntity metadataEntity, Set<String> keys) {
    metadataStore.removeProperties(MetadataScope.USER, metadataEntity, keys);
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity) {
    metadataStore.removeTags(MetadataScope.USER, metadataEntity);
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity, Set<String> tags) {
    metadataStore.removeTags(MetadataScope.USER, metadataEntity, tags);
  }

  @Override
  public MetadataSearchResponseV2 search(SearchRequest searchRequest) throws Exception {
    return filterAuthorizedSearchResult(metadataStore.search(searchRequest));
  }

  /**
   * Filter a list of {@link MetadataSearchResultRecordV2} that ensures the logged-in user has a privilege on
   *
   * @param results the {@link MetadataSearchResponseV2} to filter
   * @return filtered {@link MetadataSearchResponseV2}
   */
  private MetadataSearchResponseV2 filterAuthorizedSearchResult(final MetadataSearchResponseV2 results)
    throws Exception {
    //noinspection ConstantConditions
    return new MetadataSearchResponseV2(
      results.getSort(), results.getOffset(), results.getLimit(), results.getNumCursors(), results.getTotal(),
      // For authorization either use the known entity and if it is custom entity do enforcement on the parent.
      // TODO CDAP-13574 Support authorization for custom entities/resources
      ImmutableSet.copyOf(
        AuthorizationUtil.isVisible(results.getResults(), authorizationEnforcer, authenticationContext.getPrincipal(),
                                    input -> EntityId.getSelfOrParentEntityId(input.getMetadataEntity()), null)),
      results.getCursors(), results.isShowHidden(), results.getEntityScope());
  }
}
