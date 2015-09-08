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

package co.cask.cdap.data2.metadata;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetServiceTestBase;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataDataset;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataDatasetModule;
import co.cask.cdap.data2.metadata.service.MetadataHttpHandler;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link MetadataHttpHandler}
 */
public class MetadataHttpHandlerTest extends DatasetServiceTestBase {
  private static final Gson GSON = new Gson();

  @Before
  public void setUp() throws Exception {
    /*Injector injector = Guice.createInjector(new DiscoveryRuntimeModule().getInMemoryModules());
    injector.getInstance(DiscoveryServiceClient.class);
    DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), datasetInstance,
                                    "businessMetadataDataset",
                                    DatasetProperties.EMPTY, null, null);*/
    deployModule(Id.DatasetModule.from(Id.Namespace.SYSTEM, "business.metadata"), BusinessMetadataDatasetModule.class);
  }

  @Test
  public void testMetadata() throws IOException {
    HttpResponse response = addMetadata(Id.DatasetInstance.from(Id.Namespace.DEFAULT, "test"),
                                        ImmutableMap.of("key", "value"));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getResponseCode());
  }

  private HttpResponse addMetadata(Id.DatasetInstance instance, Map<String, String> metadata) throws IOException {
    HttpRequest request = HttpRequest.post(getUrl(instance.getNamespaceId(), "/datasets/" + instance.getId() +
      "/metadata"))
      .withBody(GSON.toJson(metadata)).build();
    return HttpRequests.execute(request);
  }

  private void removeMetadata(Id.DatasetInstance instance, String key) {

  }

  private Map<String, String> getMetadata(Id.DatasetInstance instance) {
    return null;
  }

  private void addTags(Id.DatasetInstance instance, String ... tags) {

  }

  private void removeTags(Id.DatasetInstance instance, String ... tags) {

  }

  private Iterable<String> getTags(Id.DatasetInstance instance) {
    return null;
  }
}
