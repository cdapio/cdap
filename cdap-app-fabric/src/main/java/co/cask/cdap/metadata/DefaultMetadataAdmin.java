/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
import co.cask.cdap.data2.metadata.AuditMetadataStorage;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataKind;
import co.cask.cdap.spi.metadata.MetadataMutation;
import co.cask.cdap.spi.metadata.MetadataRecord;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.Read;
import co.cask.cdap.spi.metadata.ScopedName;
import co.cask.cdap.spi.metadata.ScopedNameOfKind;
import co.cask.cdap.spi.metadata.SearchRequest;
import co.cask.cdap.spi.metadata.SearchResponse;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of {@link MetadataAdmin} that interacts directly with {@link MetadataStorage}.
 */
public class DefaultMetadataAdmin extends MetadataValidator implements MetadataAdmin {

  private final MetadataStorage storage;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  DefaultMetadataAdmin(AuditMetadataStorage storage, CConfiguration cConf,
                       AuthorizationEnforcer authorizationEnforcer,
                       AuthenticationContext authenticationContext) {
    super(cConf);
    this.storage = storage;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @Override
  public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties)
    throws InvalidMetadataException, IOException {
    validateProperties(metadataEntity, properties);
    storage.apply(new MetadataMutation.Update(metadataEntity, new Metadata(MetadataScope.USER, properties)));
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, Set<String> tags) throws InvalidMetadataException, IOException {
    validateTags(metadataEntity, tags);
    storage.apply(new MetadataMutation.Update(metadataEntity, new Metadata(MetadataScope.USER, tags)));
  }

  @Override
  public Metadata getMetadata(MetadataEntity metadataEntity) throws IOException {
    return storage.read(new Read(metadataEntity));
  }

  @Override
  public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws IOException {
    return storage.read(new Read(metadataEntity, scope));
  }

  @Override
  public Map<String, String> getProperties(MetadataEntity metadataEntity) throws IOException {
    Metadata metadata = storage.read(new Read(metadataEntity, MetadataKind.PROPERTY));
    return metadata.getProperties().entrySet().stream()
      .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue));
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, MetadataEntity metadataEntity) throws IOException {
    Metadata metadata = storage.read(new Read(metadataEntity, scope, MetadataKind.PROPERTY));
    return metadata.getProperties(scope);
  }

  @Override
  public Set<String> getTags(MetadataEntity metadataEntity) throws IOException {
    Metadata metadata = storage.read(new Read(metadataEntity, MetadataKind.TAG));
    return metadata.getTags().stream().map(ScopedName::getName).collect(Collectors.toSet());
  }

  @Override
  public Set<String> getTags(MetadataScope scope, MetadataEntity metadataEntity) throws IOException {
    Metadata metadata = storage.read(new Read(metadataEntity, scope, MetadataKind.TAG));
    return metadata.getTags(scope);
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) throws IOException {
    storage.apply(new MetadataMutation.Remove(metadataEntity, MetadataScope.USER));
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity) throws IOException {
    storage.apply(new MetadataMutation.Remove(metadataEntity, MetadataScope.USER, MetadataKind.PROPERTY));
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity, Set<String> keys) throws IOException {
    storage.apply(new MetadataMutation.Remove(metadataEntity, keys.stream()
      .map(key -> new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.USER, key)).collect(Collectors.toSet())));
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity) throws IOException {
    storage.apply(new MetadataMutation.Remove(metadataEntity, MetadataScope.USER, MetadataKind.TAG));
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity, Set<String> tags) throws IOException {
    storage.apply(new MetadataMutation.Remove(metadataEntity, tags.stream()
      .map(tag -> new ScopedNameOfKind(MetadataKind.TAG, MetadataScope.USER, tag)).collect(Collectors.toSet())));
  }

  @Override
  public SearchResponse search(SearchRequest request) throws Exception {
    return filterAuthorizedSearchResult(storage.search(request));
  }

  /**
   * Filter a list of {@link MetadataRecord}s that ensures the logged-in user has a privilege on
   *
   * @param response the {@link SearchResponse} to filter
   * @return filtered {@link SearchResponse}
   */
  private SearchResponse filterAuthorizedSearchResult(final SearchResponse response)
    throws Exception {
    //noinspection ConstantConditions
    return new SearchResponse(
      response.getRequest(),
      response.getCursor(),
      response.getOffset(),
      response.getLimit(),
      response.getTotalResults(),
      ImmutableList.copyOf(
        AuthorizationUtil.isVisible(response.getResults(), authorizationEnforcer, authenticationContext.getPrincipal(),
                                    input -> EntityId.getSelfOrParentEntityId(input.getEntity()), null)));
  }
}
