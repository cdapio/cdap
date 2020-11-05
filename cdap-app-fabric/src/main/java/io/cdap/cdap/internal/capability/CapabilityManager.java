/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import com.google.inject.Inject;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import io.cdap.cdap.proto.metadata.MetadataSearchResultRecord;
import io.cdap.cdap.spi.metadata.SearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class with helpful methods for managing capabilities
 */
public class CapabilityManager {

  private static final Logger LOG = LoggerFactory.getLogger(CapabilityManager.class);
  private static final String CAPABILITY = "capability:%s";
  private static final String APPLICATION_TAG = "application:%s";
  private static final String CAPABILITY_KEY = "capability";
  private static final String APPLICATION = "application";
  private final MetadataSearchClient metadataClient;
  private final CConfiguration cConf;

  @Inject
  CapabilityManager(MetadataSearchClient metadataClient, CConfiguration cConf) {
    this.metadataClient = metadataClient;
    this.cConf = cConf;
  }

  /**
   * Returns the list of applications that are having metadata tagged with the capability
   *
   * @param namespace  Namespace for which applications should be listed
   * @param capability Capability by which to filter
   * @param cursor Optional cursor from a previous response
   * @param offset Offset from where to start
   * @param limit Limit of records to fetch
   * @return
   * @throws Exception - Exception from meta data search if any
   */
  public EntityResult<ApplicationId> getApplications(NamespaceId namespace, String capability, @Nullable String cursor,
                                                     int offset, int limit) throws IOException {
    String capabilityTag = String.format(CAPABILITY, capability);
    SearchRequest searchRequest = SearchRequest.of(capabilityTag)
      .addNamespace(namespace.getNamespace())
      .addType(APPLICATION)
      .setScope(MetadataScope.SYSTEM)
      .setCursor(cursor)
      .setOffset(offset)
      .setLimit(limit)
      .build();
    MetadataSearchResponse searchResponse = metadataClient.search(searchRequest);
    Set<ApplicationId> applicationIds = searchResponse.getResults().stream()
      .map(MetadataSearchResultRecord::getMetadataEntity)
      .map(this::getApplicationId)
      .collect(Collectors.toSet());
    return new EntityResult<ApplicationId>(applicationIds, getCursorResponse(searchResponse),
                                           searchResponse.getOffset(), searchResponse.getLimit(),
                                           searchResponse.getTotal());
  }

  @Nullable
  private String getCursorResponse(MetadataSearchResponse searchResponse) {
    List<String> cursors = searchResponse.getCursors();
    if (cursors == null || cursors.isEmpty()) {
      return null;
    }
    return cursors.get(0);
  }

  /**
   * Returns boolean indicating whether application is disabled due to a disabled capability
   *
   * @param namespace
   * @param applicationName
   * @return
   * @throws Exception
   */
  public boolean isApplicationEnabled(String namespace, String applicationName) throws IOException {
    String applicationQuery = String.format(APPLICATION_TAG, applicationName);
    SearchRequest searchRequest = SearchRequest.of(applicationQuery)
      .addNamespace(namespace)
      .addType(APPLICATION)
      .setScope(MetadataScope.SYSTEM)
      .build();
    return metadataClient.search(searchRequest)
      .getResults().stream()
      .filter(this::hasCapabilityTagValue)
      .map(this::getCapabilityTagValue)
      .allMatch(this::isCapabilityEnabled);
  }

  @Nullable
  private String getCapabilityTagValue(MetadataSearchResultRecord metadataRecord) {
    return metadataRecord.getMetadata().get(MetadataScope.SYSTEM).getProperties().get(CAPABILITY_KEY);
  }

  private boolean hasCapabilityTagValue(MetadataSearchResultRecord metadataRecord) {
    String capabilityTagValue = getCapabilityTagValue(metadataRecord);
    return capabilityTagValue != null && !capabilityTagValue.isEmpty();
  }

  private boolean isApplicationType(MetadataEntity metadataEntity) {
    return MetadataEntity.APPLICATION.equals(metadataEntity.getType());
  }

  private ApplicationId getApplicationId(MetadataEntity metadataEntity) {
    return new ApplicationId(metadataEntity.getValue(MetadataEntity.NAMESPACE),
                             metadataEntity.getValue(MetadataEntity.APPLICATION),
                             metadataEntity.getValue(MetadataEntity.VERSION));
  }

  private boolean isCapabilityEnabled(String capability) {
    Collection<String> enabledCapabilities = cConf
      .getTrimmedStringCollection(Constants.AppFabric.ENABLED_CAPABILITIES_LIST);
    return enabledCapabilities.contains(capability);
  }
}
