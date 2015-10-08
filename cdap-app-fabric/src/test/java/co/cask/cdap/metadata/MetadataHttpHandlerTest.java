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

import co.cask.cdap.AppWithDataset;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.WordCountMinusFlowApp;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link MetadataHttpHandler}
 */
public class MetadataHttpHandlerTest extends MetadataTestBase {

  private final Id.Application application =
    Id.Application.from(Id.Namespace.DEFAULT, AppWithDataset.class.getSimpleName());
  private final Id.Program pingService = Id.Program.from(application, ProgramType.SERVICE, "PingService");
  private final Id.DatasetInstance myds = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "myds");
  private final Id.Stream mystream = Id.Stream.from(Id.Namespace.DEFAULT, "mystream");
  private final Id.Application nonExistingApp = Id.Application.from("blah", AppWithDataset.class.getSimpleName());
  private final Id.Service nonExistingService = Id.Service.from(nonExistingApp, "PingService");
  private final Id.DatasetInstance nonExistingDataset = Id.DatasetInstance.from("blah", "myds");
  private final Id.Stream nonExistingStream = Id.Stream.from("blah", "mystream");

  @Before
  public void before() throws Exception {
    Assert.assertEquals(200, deploy(AppWithDataset.class).getStatusLine().getStatusCode());
  }

  @After
  public void after() throws Exception {
    deleteApp(application, 200);
  }

  @Test
  public void testProperties() throws Exception {
    // should fail because we haven't provided any metadata in the request
    addProperties(application, null, BadRequestException.class);
    Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue", "aK", "aV");
    addProperties(application, appProperties);
    // should fail because we haven't provided any metadata in the request
    addProperties(pingService, null, BadRequestException.class);
    Map<String, String> serviceProperties = ImmutableMap.of("sKey", "sValue", "sK", "sV");
    addProperties(pingService, serviceProperties);
    // should fail because we haven't provided any metadata in the request
    addProperties(myds, null, BadRequestException.class);
    Map<String, String> datasetProperties = ImmutableMap.of("dKey", "dValue", "dK", "dV");
    addProperties(myds, datasetProperties);
    // should fail because we haven't provided any metadata in the request
    addProperties(mystream, null, BadRequestException.class);
    Map<String, String> streamProperties = ImmutableMap.of("stKey", "stValue", "stK", "stV");
    addProperties(mystream, streamProperties);
    // retrieve properties and verify
    Map<String, String> properties = getProperties(application);
    Assert.assertEquals(appProperties, properties);
    properties = getProperties(pingService);
    Assert.assertEquals(serviceProperties, properties);
    properties = getProperties(myds);
    Assert.assertEquals(datasetProperties, properties);
    properties = getProperties(mystream);
    Assert.assertEquals(streamProperties, properties);

    // test search for stream
    Set<MetadataSearchResultRecord> searchProperties = searchMetadata(Id.Namespace.DEFAULT,
                                                                      "stKey:stValue", MetadataSearchTargetType.STREAM);
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(
      new MetadataSearchResultRecord(mystream)
    );
    Assert.assertEquals(expected, searchProperties);

    // test prefix search for service
    searchProperties = searchMetadata(Id.Namespace.DEFAULT, "sKey:s*", MetadataSearchTargetType.ALL);
    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(pingService)
    );
    Assert.assertEquals(expected, searchProperties);

    // search without any target param
    searchProperties = searchMetadata(Id.Namespace.DEFAULT, "sKey:s*", null);
    Assert.assertEquals(expected, searchProperties);

    // Should get empty
    searchProperties = searchMetadata(Id.Namespace.DEFAULT, "sKey:s", null);
    Assert.assertTrue(searchProperties.size() == 0);

    searchProperties = searchMetadata(Id.Namespace.DEFAULT, "s", null);
    Assert.assertTrue(searchProperties.size() == 0);

    // search non-existent property should return empty set
    searchProperties = searchMetadata(Id.Namespace.DEFAULT, "NullKey:s*", null);
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>of(), searchProperties);

    // search invalid ns should return empty set
    searchProperties = searchMetadata(Id.Namespace.from("invalidnamespace"), "sKey:s*", null);
    Assert.assertEquals(ImmutableSet.of(), searchProperties);

    // test removal
    removeProperties(application);
    Assert.assertTrue(getProperties(application).isEmpty());
    removeProperty(pingService, "sKey");
    removeProperty(pingService, "sK");
    Assert.assertTrue(getProperties(pingService).isEmpty());
    removeProperty(myds, "dKey");
    Assert.assertEquals(ImmutableMap.of("dK", "dV"), getProperties(myds));
    removeProperty(mystream, "stK");
    Assert.assertEquals(ImmutableMap.of("stKey", "stValue"), getProperties(mystream));
    // cleanup
    removeProperties(application);
    removeProperties(pingService);
    removeProperties(myds);
    removeProperties(mystream);
    Assert.assertTrue(getProperties(application).isEmpty());
    Assert.assertTrue(getProperties(pingService).isEmpty());
    Assert.assertTrue(getProperties(myds).isEmpty());
    Assert.assertTrue(getProperties(mystream).isEmpty());

    // non-existing namespace
    addProperties(nonExistingApp, appProperties, NotFoundException.class);
    addProperties(nonExistingService, serviceProperties, NotFoundException.class);
    addProperties(nonExistingDataset, datasetProperties, NotFoundException.class);
    addProperties(nonExistingStream, streamProperties, NotFoundException.class);
  }

  @Test
  public void testTags() throws Exception {
    // should fail because we haven't provided any metadata in the request
    addTags(application, null, BadRequestException.class);
    Set<String> appTags = ImmutableSet.of("aTag", "aT");
    addTags(application, appTags);
    // should fail because we haven't provided any metadata in the request
    addTags(pingService, null, BadRequestException.class);
    Set<String> serviceTags = ImmutableSet.of("sTag", "sT");
    addTags(pingService, serviceTags);
    addTags(myds, null, BadRequestException.class);
    Set<String> datasetTags = ImmutableSet.of("dTag", "dT");
    addTags(myds, datasetTags);
    addTags(mystream, null, BadRequestException.class);
    Set<String> streamTags = ImmutableSet.of("stTag", "stT");
    addTags(mystream, streamTags);
    // retrieve tags and verify
    Set<String> tags = getTags(application);
    Assert.assertTrue(tags.containsAll(appTags));
    Assert.assertTrue(appTags.containsAll(tags));
    tags = getTags(pingService);
    Assert.assertTrue(tags.containsAll(serviceTags));
    Assert.assertTrue(serviceTags.containsAll(tags));
    tags = getTags(myds);
    Assert.assertTrue(tags.containsAll(datasetTags));
    Assert.assertTrue(datasetTags.containsAll(tags));
    tags = getTags(mystream);
    Assert.assertTrue(tags.containsAll(streamTags));
    Assert.assertTrue(streamTags.containsAll(tags));
    // test search for stream
    Set<MetadataSearchResultRecord> searchTags =
      searchMetadata(Id.Namespace.DEFAULT, "stT*", MetadataSearchTargetType.STREAM);
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(
      new MetadataSearchResultRecord(mystream)
    );
    Assert.assertEquals(expected, searchTags);
    // test prefix search, should match stream and service programs
    searchTags = searchMetadata(Id.Namespace.DEFAULT, "s*", MetadataSearchTargetType.ALL);
    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(mystream),
      new MetadataSearchResultRecord(pingService)
    );
    Assert.assertEquals(expected, searchTags);

    // search without any target param
    searchTags = searchMetadata(Id.Namespace.DEFAULT, "s*", null);
    Assert.assertEquals(expected, searchTags);

    // search non-existent tags should return empty set
    searchTags = searchMetadata(Id.Namespace.DEFAULT, "NullKey", null);
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>of(), searchTags);

    // test removal
    removeTag(application, "aTag");
    Assert.assertEquals(ImmutableSet.of("aT"), getTags(application));
    removeTags(pingService);
    Assert.assertTrue(getTags(pingService).isEmpty());
    removeTags(pingService);
    Assert.assertTrue(getTags(pingService).isEmpty());
    removeTag(myds, "dT");
    Assert.assertEquals(ImmutableSet.of("dTag"), getTags(myds));
    removeTag(mystream, "stT");
    removeTag(mystream, "stTag");
    Assert.assertTrue(getTags(mystream).isEmpty());
    // cleanup
    removeTags(application);
    removeTags(pingService);
    removeTags(myds);
    removeTags(mystream);
    Assert.assertTrue(getTags(application).isEmpty());
    Assert.assertTrue(getTags(pingService).isEmpty());
    Assert.assertTrue(getTags(myds).isEmpty());
    Assert.assertTrue(getTags(mystream).isEmpty());
    // non-existing namespace
    addTags(nonExistingApp, appTags, NotFoundException.class);
    addTags(nonExistingService, serviceTags, NotFoundException.class);
    addTags(nonExistingDataset, datasetTags, NotFoundException.class);
    addTags(nonExistingStream, streamTags, NotFoundException.class);
  }

  @Test
  public void testMetadata() throws Exception {
    assertCleanState();
    // Remove when nothing exists
    removeAllMetadata();
    assertCleanState();
    // Add some properties and tags
    Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue");
    Map<String, String> serviceProperties = ImmutableMap.of("sKey", "sValue");
    Map<String, String> datasetProperties = ImmutableMap.of("dKey", "dValue");
    Map<String, String> streamProperties = ImmutableMap.of("stKey", "stValue");
    Set<String> appTags = ImmutableSet.of("aTag");
    Set<String> serviceTags = ImmutableSet.of("sTag");
    Set<String> datasetTags = ImmutableSet.of("dTag");
    Set<String> streamTags = ImmutableSet.of("stTag");
    addProperties(application, appProperties);
    addProperties(pingService, serviceProperties);
    addProperties(myds, datasetProperties);
    addProperties(mystream, streamProperties);
    addTags(application, appTags);
    addTags(pingService, serviceTags);
    addTags(myds, datasetTags);
    addTags(mystream, streamTags);
    // verify app
    Set<MetadataRecord> metadataRecords = getMetadata(application);
    Assert.assertEquals(1, metadataRecords.size());
    MetadataRecord metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(application, metadata.getEntityId());
    Assert.assertEquals(appProperties, metadata.getProperties());
    Assert.assertEquals(appTags, metadata.getTags());
    // verify service
    metadataRecords = getMetadata(pingService);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(pingService, metadata.getEntityId());
    Assert.assertEquals(serviceProperties, metadata.getProperties());
    Assert.assertEquals(serviceTags, metadata.getTags());
    // verify dataset
    metadataRecords = getMetadata(myds);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(myds, metadata.getEntityId());
    Assert.assertEquals(datasetProperties, metadata.getProperties());
    Assert.assertEquals(datasetTags, metadata.getTags());
    // verify service
    metadataRecords = getMetadata(mystream);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(mystream, metadata.getEntityId());
    Assert.assertEquals(streamProperties, metadata.getProperties());
    Assert.assertEquals(streamTags, metadata.getTags());
    // cleanup
    removeAllMetadata();
    assertCleanState();
  }

  @Test
  public void testDeleteApplication() throws Exception {
    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Id.Program program = Id.Program.from(TEST_NAMESPACE1, "WordCountApp", ProgramType.FLOW, "WordCountFlow");

    // Set some properties metadata
    Map<String, String> flowProperties = ImmutableMap.of("sKey", "sValue", "sK", "sV");
    addProperties(program, flowProperties);

    // Get properties
    Map<String, String> properties = getProperties(program);
    Assert.assertEquals(2, properties.size());

    //Delete the App after stopping the flow
    org.apache.http.HttpResponse response =
      doDelete(getVersionedAPIPath("apps/WordCountApp/", Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(getVersionedAPIPath("apps/WordCountApp/", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    // Now try to get from invalid entity should throw 404.
    getPropertiesFromInvalidEntity(program);
  }

  @Test
  public void testInvalidEntities() throws IOException {
    Id.Program nonExistingProgram = Id.Program.from(application, ProgramType.SERVICE, "NonExistingService");
    Id.DatasetInstance nonExistingDataset = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "NonExistingDataset");
    Id.Stream nonExistingStream = Id.Stream.from(Id.Namespace.DEFAULT, "NonExistingStream");
    Id.Application nonExistingApp = Id.Application.from(Id.Namespace.DEFAULT, "NonExistingStream");

    Map<String, String> properties = ImmutableMap.of("aKey", "aValue", "aK", "aV");
    addProperties(nonExistingApp, properties, NotFoundException.class);
    addProperties(nonExistingProgram, properties, NotFoundException.class);
    addProperties(nonExistingDataset, properties, NotFoundException.class);
    addProperties(nonExistingStream, properties, NotFoundException.class);
  }

  @Test
  public void testInvalidProperties() throws IOException {
    // Test length
    StringBuilder builder = new StringBuilder(100);
    for (int i = 0; i < 100; i++) {
      builder.append("a");
    }
    Map<String, String> properties = ImmutableMap.of("aKey", builder.toString());
    addProperties(application, properties, BadRequestException.class);
    properties = ImmutableMap.of(builder.toString(), "aValue");
    addProperties(application, properties, BadRequestException.class);

    // Try to add tag as property
    properties = ImmutableMap.of("tags", "aValue");
    addProperties(application, properties, BadRequestException.class);

    // Invalid chars
    properties = ImmutableMap.of("aKey$", "aValue");
    addProperties(application, properties, BadRequestException.class);

    properties = ImmutableMap.of("aKey", "aValue$");
    addProperties(application, properties, BadRequestException.class);
  }

  @Test
  public void testInvalidTags() throws IOException {
    // Invalid chars
    Set<String> tags = ImmutableSet.of("aTag$");
    addTags(application, tags, BadRequestException.class);

    // Test length
    StringBuilder builder = new StringBuilder(100);
    for (int i = 0; i < 100; i++) {
      builder.append("a");
    }
    tags = ImmutableSet.of(builder.toString());
    addTags(application, tags, BadRequestException.class);
  }

  @Test
  public void testDeletedProgramHandlerStage() throws Exception {
    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Id.Program program = Id.Program.from(TEST_NAMESPACE1, "WordCountApp", ProgramType.FLOW, "WordCountFlow");

    // Set some properties metadata
    Map<String, String> flowProperties = ImmutableMap.of("sKey", "sValue", "sK", "sV");
    addProperties(program, flowProperties);

    // Get properties
    Map<String, String> properties = getProperties(program);
    Assert.assertEquals(2, properties.size());

    // Deploy WordCount App without Flow program. No need to start/stop the flow.
    deploy(WordCountMinusFlowApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    // Get properties from deleted (flow) program - should return 404
    getPropertiesFromInvalidEntity(program);

    // Delete the App after stopping the flow
    final Id.Application wordCountApp = Id.Application.from(TEST_NAMESPACE1, "WordCountApp");
    deleteApp(wordCountApp, 200);
  }

  private void removeAllMetadata() throws Exception {
    removeMetadata(application);
    removeMetadata(pingService);
    removeMetadata(myds);
    removeMetadata(mystream);
  }

  private void assertCleanState() throws Exception {
    // Assert clean state
    Set<MetadataRecord> appMetadatas = getMetadata(application);
    // only user metadata right now.
    Assert.assertEquals(1, appMetadatas.size());
    MetadataRecord appMetadata = appMetadatas.iterator().next();
    Assert.assertTrue(appMetadata.getProperties().isEmpty());
    Assert.assertTrue(appMetadata.getTags().isEmpty());
    Set<MetadataRecord> serviceMetadatas = getMetadata(pingService);
    // only user metadata right now.
    Assert.assertEquals(1, serviceMetadatas.size());
    MetadataRecord serviceMetadata = serviceMetadatas.iterator().next();
    Assert.assertTrue(serviceMetadata.getProperties().isEmpty());
    Assert.assertTrue(serviceMetadata.getTags().isEmpty());
    Set<MetadataRecord> datasetMetadatas = getMetadata(myds);
    // only user metadata right now.
    Assert.assertEquals(1, datasetMetadatas.size());
    MetadataRecord datasetMetadata = datasetMetadatas.iterator().next();
    Assert.assertTrue(datasetMetadata.getProperties().isEmpty());
    Assert.assertTrue(datasetMetadata.getTags().isEmpty());
    Set<MetadataRecord> streamMetadatas = getMetadata(mystream);
    // only user metadata right now.
    Assert.assertEquals(1, streamMetadatas.size());
    MetadataRecord streamMetadata = streamMetadatas.iterator().next();
    Assert.assertTrue(streamMetadata.getProperties().isEmpty());
    Assert.assertTrue(streamMetadata.getTags().isEmpty());
  }
}
