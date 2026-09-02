/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.upgrade;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.PluginId;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Parameterized tests for {@link MetadataApplicationPluginMappingFetcher} using a parameter class.
 */
@RunWith(Parameterized.class)
public class MetadataApplicationPluginMappingFetcherTest {

  // A static inner class to hold the parameters for each test case
  static class TestCaseParams {

    final String name;
    final SearchResponse userResponse;
    final SearchResponse systemResponse;
    final List<ApplicationPluginMapping> expectedMappings;

    TestCaseParams(String name, SearchResponse userResponse, SearchResponse systemResponse,
        List<ApplicationPluginMapping> expectedMappings) {
      this.name = name;
      this.userResponse = userResponse;
      this.systemResponse = systemResponse;
      this.expectedMappings = expectedMappings;
    }
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<Object[]> data() {
    SearchResponse emptyResponse = new SearchResponse(SearchRequest.of("*").build(), null, 0,
        Integer.MAX_VALUE, 0, Collections.emptyList());

    SearchResponse userPluginResponse = new SearchResponse(SearchRequest.of("*").build(), null, 0,
        Integer.MAX_VALUE, 0, ImmutableList.of(new MetadataRecord(
        MetadataEntity.builder().append("namespace", "default")
            .append("artifact", "trash-plugin")
            .append("version", "1.2.0")
            .append(MetadataEntity.TYPE, "batchsink")
            .appendAsType(MetadataEntity.PLUGIN, "trash")
            .build(),
        new Metadata(
            MetadataScope.SYSTEM, ImmutableMap.of("default:pipeline_1", "1")))));

    SearchResponse systemPluginResponse = new SearchResponse(SearchRequest.of("*").build(), null, 0,
        Integer.MAX_VALUE, 0, ImmutableList.of(new MetadataRecord(
        MetadataEntity.builder().append("namespace", "system")
            .append("artifact", "google-cloud")
            .append("version", "0.24.0")
            .append(MetadataEntity.TYPE, "batchsource")
            .appendAsType(MetadataEntity.PLUGIN, "GCS")
            .build(),
        new Metadata(
            MetadataScope.SYSTEM, ImmutableMap.of("default:pipeline_1", "1")))));

    ApplicationPluginMapping applicationSystemPluginMapping =
        new ApplicationPluginMapping(new ApplicationId("default", "pipeline_1"),
            new PluginId("system", "google-cloud", "0.24.0", "GCS", "batchsource"));
    ApplicationPluginMapping applicationUserPluginMapping =
        new ApplicationPluginMapping(new ApplicationId("default", "pipeline_1"),
            new PluginId("default", "trash-plugin", "1.2.0", "trash", "batchsink"));

    return Arrays.asList(new Object[][]{
        {new TestCaseParams("No Plugin Mappings", emptyResponse, emptyResponse,
            Collections.emptyList())},
        {new TestCaseParams("Only System Plugin Mappings", emptyResponse, systemPluginResponse,
            ImmutableList.of(applicationSystemPluginMapping))},
        {new TestCaseParams("Only User Plugin Mappings", userPluginResponse, emptyResponse,
            ImmutableList.of(applicationUserPluginMapping))},
        {new TestCaseParams("Both Plugin Mappings present", userPluginResponse,
            systemPluginResponse,
            ImmutableList.of(applicationUserPluginMapping, applicationSystemPluginMapping))}
    });
  }

  @Mock
  private MetadataAdmin metadataAdmin;

  private MetadataApplicationPluginMappingFetcher mappingFetcher;

  private final TestCaseParams params;

  public MetadataApplicationPluginMappingFetcherTest(TestCaseParams params) {
    this.params = params;
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    mappingFetcher = new MetadataApplicationPluginMappingFetcher(metadataAdmin);
  }

  @Test
  public void testFetchApplicationPluginMapping() throws Exception {
    // Arrange
    SearchRequest userPluginRequest = SearchRequest.of("*").addType("plugin")
        .setLimit(Integer.MAX_VALUE)
        .addNamespace(NamespaceId.DEFAULT.getNamespace())
        .build();
    SearchRequest systemPluginRequest = SearchRequest.of("*").addType("plugin")
        .setLimit(Integer.MAX_VALUE)
        .addNamespace(NamespaceId.SYSTEM.getNamespace()).build();

    when(metadataAdmin.search(eq(userPluginRequest))).thenReturn(params.userResponse);
    when(metadataAdmin.search(eq(systemPluginRequest))).thenReturn(params.systemResponse);

    // Act
    List<ApplicationPluginMapping> actual = mappingFetcher.fetchApplicationPluginMapping(
        NamespaceId.DEFAULT);

    // Assert
    Assert.assertEquals(String.format("Test case '%s' failed.", params.name),
        params.expectedMappings, actual);
    verify(metadataAdmin, times(1)).search(userPluginRequest);
    verify(metadataAdmin, times(1)).search(systemPluginRequest);
  }
}