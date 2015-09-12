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

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataRecord;
import co.cask.cdap.data2.metadata.service.BusinessMetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetadataSearchResultRecord;
import co.cask.cdap.proto.MetadataSearchTargetType;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of {@link MetadataAdmin} that interacts directly with {@link BusinessMetadataStore}
 */
public class DefaultMetadataAdmin implements MetadataAdmin {
  private final AbstractNamespaceClient namespaceClient;
  private final BusinessMetadataStore businessMds;

  @Inject
  DefaultMetadataAdmin(AbstractNamespaceClient namespaceClient, BusinessMetadataStore businessMds) {
    this.namespaceClient = namespaceClient;
    this.businessMds = businessMds;
  }

  @Override
  public void addProperties(Id.NamespacedId entityId, Map<String, String> properties) throws NotFoundException {
    ensureEntityExists(entityId);
    // TODO: CDAP-3571 Validation
    // TODO: Check if app exists
    businessMds.setProperties(entityId, properties);
  }

  @Override
  public void addTags(Id.NamespacedId entityId, String... tags) throws NotFoundException {
    ensureEntityExists(entityId);
    businessMds.addTags(entityId, tags);
  }

  @Override
  public Map<String, String> getProperties(Id.NamespacedId entityId) throws NotFoundException {
    ensureEntityExists(entityId);
    return businessMds.getProperties(entityId);
  }

  @Override
  public Set<String> getTags(Id.NamespacedId entityId) throws NotFoundException {
    ensureEntityExists(entityId);
    return businessMds.getTags(entityId);
  }

  @Override
  public void removeProperties(Id.NamespacedId entityId) throws NotFoundException {
    ensureEntityExists(entityId);
    businessMds.removeProperties(entityId);
  }

  @Override
  public void removeProperties(Id.NamespacedId entityId, String... keys) throws NotFoundException {
    ensureEntityExists(entityId);
    businessMds.removeProperties(entityId, keys);
  }

  @Override
  public void removeTags(Id.NamespacedId entityId) throws NotFoundException {
    ensureEntityExists(entityId);
    businessMds.removeTags(entityId);
  }

  @Override
  public void removeTags(Id.NamespacedId entityId, String... tags) throws NotFoundException {
    ensureEntityExists(entityId);
    businessMds.removeTags(entityId, tags);
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadata(String searchQuery,
                                                        @Nullable MetadataSearchTargetType type)
    throws NotFoundException {
    Iterable<BusinessMetadataRecord> results;
    if (type == null) {
      results = businessMds.searchMetadata(searchQuery);
    } else {
      results = businessMds.searchMetadataOnType(searchQuery, type);
    }

    Set<MetadataSearchResultRecord> searchResultRecords = new LinkedHashSet<>();
    for (BusinessMetadataRecord bmr : results) {
      MetadataSearchResultRecord msr =
        new MetadataSearchResultRecord(bmr.getTargetId(),
                                       (type == null ? MetadataSearchTargetType.ALL : type));
      searchResultRecords.add(msr);
    }
    return searchResultRecords;
  }

  /**
   * Ensures that the specified {@link Id.NamespacedId} exists. Currently only verifies that the namespace exists.
   * TODO: Verify that the actual entity (app/program/stream/dataset) also exists.
   */
  private void ensureEntityExists(Id.NamespacedId entityId) throws NamespaceNotFoundException {
    try {
      namespaceClient.get(entityId.getNamespace());
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
