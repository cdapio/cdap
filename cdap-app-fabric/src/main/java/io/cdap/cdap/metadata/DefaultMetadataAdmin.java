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

package io.cdap.cdap.metadata;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Implementation of {@link MetadataAdmin} that interacts directly with {@link MetadataStorage}.
 */
public class DefaultMetadataAdmin extends MetadataValidator implements MetadataAdmin {

  private final MetadataStorage storage;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  DefaultMetadataAdmin(MetadataStorage storage, CConfiguration cConf,
                       AuthorizationEnforcer authorizationEnforcer,
                       AuthenticationContext authenticationContext) {
    super(cConf);
    this.storage = storage;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @Override
  public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties, MutationOptions options)
    throws Exception {
    enforce(metadataEntity, Action.ADMIN);
    validateProperties(metadataEntity, properties);
    storage.apply(new MetadataMutation.Update(metadataEntity, new Metadata(MetadataScope.USER, properties)), options);
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, Set<String> tags, MutationOptions options) throws Exception {
    enforce(metadataEntity, Action.ADMIN);
    validateTags(metadataEntity, tags);
    storage.apply(new MetadataMutation.Update(metadataEntity, new Metadata(MetadataScope.USER, tags)), options);
  }

  @Override
  public Metadata getMetadata(MetadataEntity metadataEntity) throws Exception {
    return getMetadata(metadataEntity, null, null);
  }

  @Override
  public Metadata getMetadata(MetadataEntity metadataEntity, MetadataScope scope) throws Exception {
    return getMetadata(metadataEntity, scope, null);
  }

  @Override
  public Metadata getMetadata(MetadataEntity entity, @Nullable MetadataScope scope, @Nullable MetadataKind kind)
    throws Exception {
    if (isEntityType(entity)) {
      AuthorizationUtil.ensureAccess(EntityId.fromMetadataEntity(entity), authorizationEnforcer,
                                     authenticationContext.getPrincipal());
    }
    Read read = kind != null ? (scope != null ? new Read(entity, scope, kind) : new Read(entity, kind))
      : scope != null ? new Read(entity, scope) : new Read(entity);
    return storage.read(read);
  }

  @Override
  public Map<String, String> getProperties(MetadataEntity metadataEntity) throws Exception {
    return doGetProperties(null, metadataEntity);
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, MetadataEntity metadataEntity) throws Exception {
    return doGetProperties(scope, metadataEntity);
  }

  private Map<String, String> doGetProperties(@Nullable MetadataScope scope, MetadataEntity metadataEntity)
    throws Exception {
    // the auth enforcement is in getMetadata
    Metadata metadata = getMetadata(metadataEntity, scope, MetadataKind.PROPERTY);
    return metadata.getProperties().entrySet().stream().collect(Collectors.toMap(
      entry -> entry.getKey().getName(), Map.Entry::getValue));
  }

  @Override
  public Set<String> getTags(MetadataEntity metadataEntity) throws Exception {
    return doGetTags(null, metadataEntity);
  }

  @Override
  public Set<String> getTags(MetadataScope scope, MetadataEntity metadataEntity) throws Exception {
    return doGetTags(scope, metadataEntity);
  }

  private Set<String> doGetTags(@Nullable MetadataScope scope, MetadataEntity metadataEntity) throws Exception {
    // the auth enforcement is in getMetadata
    Metadata metadata = getMetadata(metadataEntity, scope, MetadataKind.TAG);
    return metadata.getTags().stream().map(ScopedName::getName).collect(Collectors.toSet());
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity, MutationOptions options) throws Exception {
    enforce(metadataEntity, Action.ADMIN);
    storage.apply(new MetadataMutation.Remove(metadataEntity, MetadataScope.USER), options);
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity, MutationOptions options) throws Exception {
    enforce(metadataEntity, Action.ADMIN);
    storage.apply(new MetadataMutation.Remove(metadataEntity, MetadataScope.USER, MetadataKind.PROPERTY), options);
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity, Set<String> keys, MutationOptions options)
    throws Exception {
    enforce(metadataEntity, Action.ADMIN);
    storage.apply(new MetadataMutation.Remove(metadataEntity, keys.stream()
      .map(key -> new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.USER, key))
      .collect(Collectors.toSet())), options);
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity, MutationOptions options) throws Exception {
    enforce(metadataEntity, Action.ADMIN);
    storage.apply(new MetadataMutation.Remove(metadataEntity, MetadataScope.USER, MetadataKind.TAG), options);
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity, Set<String> tags, MutationOptions options) throws Exception {
    enforce(metadataEntity, Action.ADMIN);
    storage.apply(new MetadataMutation.Remove(metadataEntity, tags.stream()
      .map(tag -> new ScopedNameOfKind(MetadataKind.TAG, MetadataScope.USER, tag))
      .collect(Collectors.toSet())), options);
  }


  @Override
  public void applyMutation(MetadataMutation mutation, MutationOptions options) throws IOException {
    // TODO: CDAP-17803 add auth for this method, now it is not needed since this method is only used by internal
    // REST endpoint
    storage.apply(mutation, options);
  }

  @Override
  public void applyMutations(List<? extends MetadataMutation> mutations, MutationOptions options) throws IOException {
    // TODO: CDAP-17803 add auth for this method, now it is not needed since this method is only used by internal
    // REST endpoint
    storage.batch(mutations, options);
  }

  @Override
  public SearchResponse search(SearchRequest request) throws Exception {
    return filterAuthorizedSearchResult(storage.search(request));
  }

  private void enforce(MetadataEntity metadataEntity, Action action) throws Exception {
    if (isEntityType(metadataEntity)) {
      authorizationEnforcer.enforce(EntityId.fromMetadataEntity(metadataEntity), authenticationContext.getPrincipal(),
                                    action);
    }
  }

  /**
   * Check if the metadata entity is an entity type. It is possible that metadata entity has a type which is not among
   * the entity ids. In those cases, do not enforce.
   */
  private boolean isEntityType(MetadataEntity metadataEntity) {
    try {
      EntityType.valueOf(metadataEntity.getType().toUpperCase());
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
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
