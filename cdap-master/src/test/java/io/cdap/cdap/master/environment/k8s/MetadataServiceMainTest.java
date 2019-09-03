/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.client.MetadataClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import io.cdap.cdap.proto.metadata.MetadataSearchResultRecord;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for the {@link MetadataServiceMain}.
 */
public class MetadataServiceMainTest extends MasterServiceMainTestBase {

  @Test
  public void testMetadataService() throws Exception {
    Injector injector = getServiceMainInstance(MetadataServiceMain.class).getInjector();

    DatasetId datasetId = NamespaceId.DEFAULT.dataset("testds");

    // Create a dataset, a metadata should get published.
    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);

    long beforeCreation = System.currentTimeMillis();
    datasetFramework.addInstance(KeyValueTable.class.getName(), datasetId, DatasetProperties.EMPTY);

    // Query the metadata
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    Discoverable metadataEndpoint = new RandomEndpointStrategy(
      () -> discoveryServiceClient.discover(Constants.Service.METADATA_SERVICE)).pick(5, TimeUnit.SECONDS);

    Assert.assertNotNull(metadataEndpoint);

    // Try to query the metadata
    InetSocketAddress metadataAddr = metadataEndpoint.getSocketAddress();
    ConnectionConfig connConfig = ConnectionConfig.builder()
      .setSSLEnabled(URIScheme.HTTPS.isMatch(metadataEndpoint))
      .setHostname(metadataAddr.getHostName())
      .setPort(metadataAddr.getPort())
      .build();

    MetadataClient metadataClient = new MetadataClient(ClientConfig.builder()
                                                         .setVerifySSLCert(false)
                                                         .setConnectionConfig(connConfig)
                                                         .build());
    MetadataSearchResponse response = metadataClient.searchMetadata(datasetId.getNamespaceId(), "*", (String) null);

    Set<MetadataSearchResultRecord> results = response.getResults();
    Assert.assertFalse(results.isEmpty());

    long creationTime = results.stream()
      .filter(r -> datasetId.equals(r.getEntityId()))
      .map(MetadataSearchResultRecord::getMetadata)
      .map(metadata -> metadata.get(MetadataScope.SYSTEM).getProperties().get(MetadataConstants.CREATION_TIME_KEY))
      .map(Long::parseLong)
      .findFirst()
      .orElse(-1L);

    // The creation time should be between the beforeCreation time and the current time
    Assert.assertTrue(creationTime >= beforeCreation);
    Assert.assertTrue(creationTime <= System.currentTimeMillis());
  }
}
