/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.AppWithDataset;
import co.cask.cdap.StandaloneTester;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.WordCountMinusFlowApp;
import co.cask.cdap.api.Config;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.client.app.AllProgramsApp;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.system.AbstractSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.DatasetSystemMetadataWriter;
import co.cask.cdap.metadata.MetadataHttpHandler;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.metadata.Metadata;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.common.http.HttpRequest;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Tests for {@link MetadataHttpHandler}
 */
public class MetadataHttpHandlerTestRun extends MetadataTestBase {

  private final ApplicationId application = NamespaceId.DEFAULT.app(AppWithDataset.class.getSimpleName());
  private final ArtifactId artifactId = NamespaceId.DEFAULT.artifact(application.getApplication(), "1.0.0");
  private final ProgramId pingService = application.service("PingService");
  private final DatasetId myds = NamespaceId.DEFAULT.dataset("myds");
  private final StreamId mystream = NamespaceId.DEFAULT.stream("mystream");
  private final StreamViewId myview = mystream.view("myview");
  private final ApplicationId nonExistingApp = new ApplicationId("blah", AppWithDataset.class.getSimpleName());
  private final ProgramId nonExistingService = nonExistingApp.service("PingService");
  private final DatasetId nonExistingDataset = new DatasetId("blah", "myds");
  private final StreamId nonExistingStream = new StreamId("blah", "mystream");
  private final StreamViewId nonExistingView = nonExistingStream.view("myView");
  private final ArtifactId nonExistingArtifact = new ArtifactId("blah", "art", "1.0.0");

  @Before
  public void before() throws Exception {
    addAppArtifact(artifactId, AppWithDataset.class);
    AppRequest<Config> appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()));

    appClient.deploy(application, appRequest);
    FormatSpecification format = new FormatSpecification("csv", null, null);
    ViewSpecification viewSpec = new ViewSpecification(format, null);
    streamViewClient.createOrUpdate(myview, viewSpec);
  }

  @After
  public void after() throws Exception {
    appClient.delete(application);
    artifactClient.delete(artifactId);
    namespaceClient.delete(NamespaceId.DEFAULT);
  }

  @Test
  public void testProperties() throws Exception {
    // should fail because we haven't provided any metadata in the request
    addProperties(application, null, BadRequestException.class);
    String multiWordValue = "wow1 WoW2   -    WOW3 - wow4_woW5 wow6";
    Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue", "multiword", multiWordValue);
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
    Map<String, String> streamProperties = ImmutableMap.of("stKey", "stValue", "stK", "stV", "multiword",
                                                           multiWordValue);
    addProperties(mystream, streamProperties);
    addProperties(myview, null, BadRequestException.class);
    Map<String, String> viewProperties = ImmutableMap.of("viewKey", "viewValue", "viewK", "viewV");
    addProperties(myview, viewProperties);
    // should fail because we haven't provided any metadata in the request
    addProperties(artifactId, null, BadRequestException.class);
    Map<String, String> artifactProperties = ImmutableMap.of("rKey", "rValue", "rK", "rV");
    addProperties(artifactId, artifactProperties);
    // retrieve properties and verify
    Map<String, String> properties = getProperties(application, MetadataScope.USER);
    Assert.assertEquals(appProperties, properties);
    properties = getProperties(pingService, MetadataScope.USER);
    Assert.assertEquals(serviceProperties, properties);
    properties = getProperties(myds, MetadataScope.USER);
    Assert.assertEquals(datasetProperties, properties);
    properties = getProperties(mystream, MetadataScope.USER);
    Assert.assertEquals(streamProperties, properties);
    properties = getProperties(myview, MetadataScope.USER);
    Assert.assertEquals(viewProperties, properties);
    properties = getProperties(artifactId, MetadataScope.USER);
    Assert.assertEquals(artifactProperties, properties);

    // test search for application
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(
      new MetadataSearchResultRecord(application)
    );
    Set<MetadataSearchResultRecord> searchProperties = searchMetadata(NamespaceId.DEFAULT, "aKey:aValue",
                                                                      MetadataSearchTargetType.APP);
    Assert.assertEquals(expected, searchProperties);
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "multiword:wow1", MetadataSearchTargetType.APP);
    Assert.assertEquals(expected, searchProperties);
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "multiword:woW5", MetadataSearchTargetType.APP);
    Assert.assertEquals(expected, searchProperties);
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "WOW3", MetadataSearchTargetType.APP);
    Assert.assertEquals(expected, searchProperties);

    // test search for stream
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "stKey:stValue", MetadataSearchTargetType.STREAM);
    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(mystream));
    Assert.assertEquals(expected, searchProperties);

    // test search for view with lowercase key value when metadata was stored in mixed case
    searchProperties = searchMetadata(NamespaceId.DEFAULT,
                                      "viewkey:viewvalue", MetadataSearchTargetType.VIEW);
    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(myview)
    );
    Assert.assertEquals(expected, searchProperties);

    // test search for view with lowercase value when metadata was stored in mixed case
    searchProperties = searchMetadata(NamespaceId.DEFAULT,
                                      "viewvalue", MetadataSearchTargetType.VIEW);
    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(myview)
    );
    Assert.assertEquals(expected, searchProperties);

    // test search for artifact
    searchProperties = searchMetadata(NamespaceId.DEFAULT,
                                      "rKey:rValue", MetadataSearchTargetType.ARTIFACT);
    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(artifactId)
    );
    Assert.assertEquals(expected, searchProperties);

    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(application),
      new MetadataSearchResultRecord(mystream)
    );

    searchProperties = searchMetadata(NamespaceId.DEFAULT, "multiword:w*", MetadataSearchTargetType.ALL);
    Assert.assertEquals(2, searchProperties.size());
    Assert.assertEquals(expected, searchProperties);

    searchProperties = searchMetadata(NamespaceId.DEFAULT, "multiword:*", MetadataSearchTargetType.ALL);
    Assert.assertEquals(2, searchProperties.size());
    Assert.assertEquals(expected, searchProperties);

    searchProperties = searchMetadata(NamespaceId.DEFAULT, "wo*", MetadataSearchTargetType.ALL);
    Assert.assertEquals(2, searchProperties.size());
    Assert.assertEquals(expected, searchProperties);

    // test prefix search for service
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "sKey:s*", MetadataSearchTargetType.ALL);
    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(pingService)
    );
    Assert.assertEquals(expected, searchProperties);

    // search without any target param
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "sKey:s*");
    Assert.assertEquals(expected, searchProperties);

    // Should get empty
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "sKey:s");
    Assert.assertTrue(searchProperties.size() == 0);

    searchProperties = searchMetadata(NamespaceId.DEFAULT, "s");
    Assert.assertTrue(searchProperties.size() == 0);

    // search non-existent property should return empty set
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "NullKey:s*");
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>of(), searchProperties);

    // search invalid ns should return empty set
    searchProperties = searchMetadata(new NamespaceId("invalidnamespace"), "sKey:s*");
    Assert.assertEquals(ImmutableSet.of(), searchProperties);

    // test removal
    removeProperties(application);
    Assert.assertTrue(getProperties(application, MetadataScope.USER).isEmpty());
    removeProperty(pingService, "sKey");
    removeProperty(pingService, "sK");
    Assert.assertTrue(getProperties(pingService, MetadataScope.USER).isEmpty());
    removeProperty(myds, "dKey");
    Assert.assertEquals(ImmutableMap.of("dK", "dV"), getProperties(myds, MetadataScope.USER));
    removeProperty(mystream, "stK");
    removeProperty(mystream, "stKey");
    Assert.assertEquals(ImmutableMap.of("multiword", multiWordValue), getProperties(mystream, MetadataScope.USER));
    removeProperty(myview, "viewK");
    Assert.assertEquals(ImmutableMap.of("viewKey", "viewValue"), getProperties(myview, MetadataScope.USER));
    // cleanup
    removeProperties(myview);
    removeProperties(application);
    removeProperties(pingService);
    removeProperties(myds);
    removeProperties(mystream);
    removeProperties(artifactId);
    Assert.assertTrue(getProperties(application, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getProperties(pingService, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getProperties(myds, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getProperties(mystream, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getProperties(myview, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getProperties(artifactId, MetadataScope.USER).isEmpty());

    // non-existing namespace
    addProperties(nonExistingApp, appProperties, NotFoundException.class);
    addProperties(nonExistingService, serviceProperties, NotFoundException.class);
    addProperties(nonExistingDataset, datasetProperties, NotFoundException.class);
    addProperties(nonExistingStream, streamProperties, NotFoundException.class);
    addProperties(nonExistingView, streamProperties, NotFoundException.class);
    addProperties(nonExistingArtifact, artifactProperties, NotFoundException.class);
  }

  @Test
  public void testTags() throws Exception {
    // should fail because we haven't provided any metadata in the request
    addTags(application, null, BadRequestException.class);
    Set<String> appTags = ImmutableSet.of("aTag", "aT", "Wow-WOW1", "WOW_WOW2");
    addTags(application, appTags);
    // should fail because we haven't provided any metadata in the request
    addTags(pingService, null, BadRequestException.class);
    Set<String> serviceTags = ImmutableSet.of("sTag", "sT");
    addTags(pingService, serviceTags);
    addTags(myds, null, BadRequestException.class);
    Set<String> datasetTags = ImmutableSet.of("dTag", "dT");
    addTags(myds, datasetTags);
    addTags(mystream, null, BadRequestException.class);
    Set<String> streamTags = ImmutableSet.of("stTag", "stT", "Wow-WOW1", "WOW_WOW2");
    addTags(mystream, streamTags);
    addTags(myview, null, BadRequestException.class);
    Set<String> viewTags = ImmutableSet.of("viewTag", "viewT");
    addTags(myview, viewTags);
    Set<String> artifactTags = ImmutableSet.of("rTag", "rT");
    addTags(artifactId, artifactTags);
    // retrieve tags and verify
    Set<String> tags = getTags(application, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(appTags));
    Assert.assertTrue(appTags.containsAll(tags));
    tags = getTags(pingService, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(serviceTags));
    Assert.assertTrue(serviceTags.containsAll(tags));
    tags = getTags(myds, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(datasetTags));
    Assert.assertTrue(datasetTags.containsAll(tags));
    tags = getTags(mystream, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(streamTags));
    Assert.assertTrue(streamTags.containsAll(tags));
    tags = getTags(myview, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(viewTags));
    Assert.assertTrue(viewTags.containsAll(tags));
    tags = getTags(artifactId, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(artifactTags));
    Assert.assertTrue(artifactTags.containsAll(tags));
    // test search for stream
    Set<MetadataSearchResultRecord> searchTags =
      searchMetadata(NamespaceId.DEFAULT, "stT", MetadataSearchTargetType.STREAM);
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(
      new MetadataSearchResultRecord(mystream)
    );
    Assert.assertEquals(expected, searchTags);

    searchTags = searchMetadata(NamespaceId.DEFAULT, "Wow", MetadataSearchTargetType.STREAM);
    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(mystream)
    );

    Assert.assertEquals(expected, searchTags);
    // test search for view with lowercase tag when metadata was stored in mixed case
    searchTags =
      searchMetadata(NamespaceId.DEFAULT, "viewtag", MetadataSearchTargetType.VIEW);
    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(myview)
    );
    Assert.assertEquals(expected, searchTags);
    // test prefix search, should match stream and application
    searchTags = searchMetadata(NamespaceId.DEFAULT, "Wow*", MetadataSearchTargetType.ALL);
    expected = ImmutableSet.of(
      new MetadataSearchResultRecord(application),
      new MetadataSearchResultRecord(mystream)
    );
    Assert.assertEquals(expected, searchTags);

    // search without any target param
    searchTags = searchMetadata(NamespaceId.DEFAULT, "Wow*");
    Assert.assertEquals(expected, searchTags);

    // search non-existent tags should return empty set
    searchTags = searchMetadata(NamespaceId.DEFAULT, "NullKey");
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>of(), searchTags);

    // test removal
    removeTag(application, "aTag");
    Assert.assertEquals(ImmutableSet.of("aT", "Wow-WOW1", "WOW_WOW2"), getTags(application, MetadataScope.USER));
    removeTags(pingService);
    Assert.assertTrue(getTags(pingService, MetadataScope.USER).isEmpty());
    removeTags(pingService);
    Assert.assertTrue(getTags(pingService, MetadataScope.USER).isEmpty());
    removeTag(myds, "dT");
    Assert.assertEquals(ImmutableSet.of("dTag"), getTags(myds, MetadataScope.USER));
    removeTag(mystream, "stT");
    removeTag(mystream, "stTag");
    removeTag(mystream, "Wow-WOW1");
    removeTag(mystream, "WOW_WOW2");
    removeTag(myview, "viewT");
    removeTag(myview, "viewTag");
    Assert.assertTrue(getTags(mystream, MetadataScope.USER).isEmpty());
    removeTag(artifactId, "rTag");
    removeTag(artifactId, "rT");
    Assert.assertTrue(getTags(artifactId, MetadataScope.USER).isEmpty());
    // cleanup
    removeTags(application);
    removeTags(pingService);
    removeTags(myds);
    removeTags(mystream);
    removeTags(myview);
    removeTags(artifactId);
    Assert.assertTrue(getTags(application, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getTags(pingService, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getTags(myds, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getTags(mystream, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getTags(artifactId, MetadataScope.USER).isEmpty());
    // non-existing namespace
    addTags(nonExistingApp, appTags, NotFoundException.class);
    addTags(nonExistingService, serviceTags, NotFoundException.class);
    addTags(nonExistingDataset, datasetTags, NotFoundException.class);
    addTags(nonExistingStream, streamTags, NotFoundException.class);
    addTags(nonExistingView, streamTags, NotFoundException.class);
    addTags(nonExistingArtifact, artifactTags, NotFoundException.class);
  }

  @Test
  public void testMetadata() throws Exception {
    assertCleanState(MetadataScope.USER);
    // Remove when nothing exists
    removeAllMetadata();
    assertCleanState(MetadataScope.USER);
    // Add some properties and tags
    Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue");
    Map<String, String> serviceProperties = ImmutableMap.of("sKey", "sValue");
    Map<String, String> datasetProperties = ImmutableMap.of("dKey", "dValue");
    Map<String, String> streamProperties = ImmutableMap.of("stKey", "stValue");
    Map<String, String> viewProperties = ImmutableMap.of("viewKey", "viewValue");
    Map<String, String> artifactProperties = ImmutableMap.of("rKey", "rValue");
    Set<String> appTags = ImmutableSet.of("aTag");
    Set<String> serviceTags = ImmutableSet.of("sTag");
    Set<String> datasetTags = ImmutableSet.of("dTag");
    Set<String> streamTags = ImmutableSet.of("stTag");
    Set<String> viewTags = ImmutableSet.of("viewTag");
    Set<String> artifactTags = ImmutableSet.of("rTag");
    addProperties(application, appProperties);
    addProperties(pingService, serviceProperties);
    addProperties(myds, datasetProperties);
    addProperties(mystream, streamProperties);
    addProperties(myview, viewProperties);
    addProperties(artifactId, artifactProperties);
    addTags(application, appTags);
    addTags(pingService, serviceTags);
    addTags(myds, datasetTags);
    addTags(mystream, streamTags);
    addTags(myview, viewTags);
    addTags(artifactId, artifactTags);
    // verify app
    Set<MetadataRecord> metadataRecords = getMetadata(application, MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    MetadataRecord metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(application, metadata.getEntityId());
    Assert.assertEquals(appProperties, metadata.getProperties());
    Assert.assertEquals(appTags, metadata.getTags());
    // verify service
    metadataRecords = getMetadata(pingService, MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(pingService, metadata.getEntityId());
    Assert.assertEquals(serviceProperties, metadata.getProperties());
    Assert.assertEquals(serviceTags, metadata.getTags());
    // verify dataset
    metadataRecords = getMetadata(myds, MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(myds, metadata.getEntityId());
    Assert.assertEquals(datasetProperties, metadata.getProperties());
    Assert.assertEquals(datasetTags, metadata.getTags());
    // verify stream
    metadataRecords = getMetadata(mystream, MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(mystream, metadata.getEntityId());
    Assert.assertEquals(streamProperties, metadata.getProperties());
    Assert.assertEquals(streamTags, metadata.getTags());
    // verify view
    metadataRecords = getMetadata(myview, MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(myview, metadata.getEntityId());
    Assert.assertEquals(viewProperties, metadata.getProperties());
    Assert.assertEquals(viewTags, metadata.getTags());
    // verify artifact
    metadataRecords = getMetadata(artifactId, MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(artifactId, metadata.getEntityId());
    Assert.assertEquals(artifactProperties, metadata.getProperties());
    Assert.assertEquals(artifactTags, metadata.getTags());
    // cleanup
    removeAllMetadata();
    assertCleanState(MetadataScope.USER);
  }

  @Test
  public void testDeleteApplication() throws Exception {
    namespaceClient.create(new NamespaceMeta.Builder().setName(TEST_NAMESPACE1.toId()).build());
    appClient.deploy(TEST_NAMESPACE1, createAppJarFile(WordCountApp.class));
    ProgramId programId = TEST_NAMESPACE1.app("WordCountApp").flow("WordCountFlow");

    // Set some properties metadata
    Map<String, String> flowProperties = ImmutableMap.of("sKey", "sValue", "sK", "sV");
    addProperties(programId, flowProperties);

    // Get properties
    Map<String, String> properties = getProperties(programId, MetadataScope.USER);
    Assert.assertEquals(2, properties.size());

    // Delete the App after stopping the flow
    appClient.delete(TEST_NAMESPACE1.app(programId.getApplication()));

    // Delete again should throw not found exception
    try {
      appClient.delete(TEST_NAMESPACE1.app(programId.getApplication()));
      Assert.fail("Expected NotFoundException");
    } catch (NotFoundException e) {
      // expected
    }

    // Now try to get from invalid entity should throw 404.
    getPropertiesFromInvalidEntity(programId);
  }

  @Test
  public void testInvalidEntities() throws IOException {
    ProgramId nonExistingProgram = application.service("NonExistingService");
    DatasetId nonExistingDataset = new DatasetId(NamespaceId.DEFAULT.getNamespace(),
                                                                       "NonExistingDataset");
    StreamId nonExistingStream = NamespaceId.DEFAULT.stream("NonExistingStream");
    StreamViewId nonExistingView = nonExistingStream.view("NonExistingView");
    ApplicationId nonExistingApp = NamespaceId.DEFAULT.app("NonExistingApp");

    Map<String, String> properties = ImmutableMap.of("aKey", "aValue", "aK", "aV");
    addProperties(nonExistingApp, properties, NotFoundException.class);
    addProperties(nonExistingProgram, properties, NotFoundException.class);
    addProperties(nonExistingDataset, properties, NotFoundException.class);
    addProperties(nonExistingView, properties, NotFoundException.class);
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
    appClient.deploy(TEST_NAMESPACE1, createAppJarFile(WordCountApp.class));
    ProgramId program = TEST_NAMESPACE1.app("WordCountApp").flow("WordCountFlow");

    // Set some properties metadata
    Map<String, String> flowProperties = ImmutableMap.of("sKey", "sValue", "sK", "sV");
    addProperties(program, flowProperties);

    // Get properties
    Map<String, String> properties = getProperties(program, MetadataScope.USER);
    Assert.assertEquals(2, properties.size());

    // Deploy WordCount App without Flow program. No need to start/stop the flow.
    appClient.deploy(TEST_NAMESPACE1, createAppJarFile(WordCountMinusFlowApp.class));

    // Get properties from deleted (flow) program - should return 404
    getPropertiesFromInvalidEntity(program);

    // Delete the App after stopping the flow
    appClient.delete(TEST_NAMESPACE1.app("WordCountApp"));
  }

  @Test
  public void testSystemMetadataRetrieval() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(AllProgramsApp.class));
    // verify stream system metadata
    StreamId streamId = NamespaceId.DEFAULT.stream(AllProgramsApp.STREAM_NAME);
    Set<String> streamSystemTags = getTags(streamId, MetadataScope.SYSTEM);
    Assert.assertEquals(ImmutableSet.of(AllProgramsApp.STREAM_NAME, AbstractSystemMetadataWriter.EXPLORE_TAG),
                        streamSystemTags);

    Map<String, String> streamSystemProperties = getProperties(streamId, MetadataScope.SYSTEM);
    // Verify create time exists, and is within the past hour
    final String creationTime = "creation-time";
    String description = "description";
    String schema = "schema";
    String ttl = "ttl";
    Assert.assertTrue("Expected creation time to exist but it does not",
                      streamSystemProperties.containsKey(creationTime));
    long createTime = Long.parseLong(streamSystemProperties.get(creationTime));
    Assert.assertTrue("Stream create time should be within the last hour - " + createTime,
                      createTime > System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

    Assert.assertEquals(
      ImmutableMap.of(schema,
                      Schema.recordOf("stringBody",
                                      Schema.Field.of("body",
                                                      Schema.of(Schema.Type.STRING))).toString(),
                      ttl, String.valueOf(Long.MAX_VALUE),
                      description, "test stream",
                      creationTime, String.valueOf(createTime)
      ),
      streamSystemProperties
    );

    // Update stream properties and verify metadata got updated (except creation time and description)
    long newTtl = 100000L;
    streamClient.setStreamProperties(streamId, new StreamProperties(newTtl, null, null));
    streamSystemProperties = getProperties(streamId, MetadataScope.SYSTEM);
    Assert.assertEquals(
      ImmutableMap.of(schema,
                      Schema.recordOf("stringBody",
                                      Schema.Field.of("body",
                                                      Schema.of(Schema.Type.STRING))).toString(),
                      ttl, String.valueOf(newTtl * 1000),
                      description, "test stream",
                      creationTime, String.valueOf(createTime)
      ),
      streamSystemProperties
    );

    Set<MetadataRecord> streamSystemMetadata = getMetadata(streamId, MetadataScope.SYSTEM);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataRecord(streamId, MetadataScope.SYSTEM, streamSystemProperties,
                                         streamSystemTags)), streamSystemMetadata);

    // create view and verify view system metadata
    StreamViewId view = new StreamViewId(streamId.getNamespace(), streamId.getStream(), "view");
    Schema viewSchema = Schema.recordOf("record",
                                        Schema.Field.of("viewBody", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    streamViewClient.createOrUpdate(view, new ViewSpecification(new FormatSpecification("format", viewSchema)));
    Set<String> viewSystemTags = getTags(view, MetadataScope.SYSTEM);
    Assert.assertEquals(ImmutableSet.of("view", AllProgramsApp.STREAM_NAME), viewSystemTags);
    Map<String, String> viewSystemProperties = getProperties(view, MetadataScope.SYSTEM);
    Assert.assertEquals(viewSchema.toString(), viewSystemProperties.get(schema));
    ImmutableSet<String> viewUserTags = ImmutableSet.of("viewTag");
    addTags(view, viewUserTags);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataRecord(view, MetadataScope.USER, ImmutableMap.<String, String>of(),
                                         viewUserTags),
                      new MetadataRecord(view, MetadataScope.SYSTEM, viewSystemProperties,
                                         viewSystemTags)),
      getMetadata(view)
    );

    // verify dataset system metadata
    DatasetId datasetInstance = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME);
    Set<String> dsSystemTags = getTags(datasetInstance, MetadataScope.SYSTEM);
    Assert.assertEquals(
      ImmutableSet.of(AllProgramsApp.DATASET_NAME,
                      DatasetSystemMetadataWriter.BATCH_TAG,
                      AbstractSystemMetadataWriter.EXPLORE_TAG),
      dsSystemTags);

    Map<String, String> dsSystemProperties = getProperties(datasetInstance, MetadataScope.SYSTEM);
    // Verify create time exists, and is within the past hour
    Assert.assertTrue("Expected creation time to exist but it does not", dsSystemProperties.containsKey(creationTime));
    createTime = Long.parseLong(dsSystemProperties.get(creationTime));
    Assert.assertTrue("Dataset create time should be within the last hour - " + createTime,
                      createTime > System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

    // Now remove create time and assert all other system properties
    Assert.assertEquals(
      ImmutableMap.of(
        "type", KeyValueTable.class.getName(),
        description, "test dataset",
        creationTime, String.valueOf(createTime)
      ),
      dsSystemProperties
    );

    //Update properties, and make sure that system metadata gets updated (except create time)
    datasetClient.update(datasetInstance, ImmutableMap.of(Table.PROPERTY_TTL, "100000"));
    dsSystemProperties = getProperties(datasetInstance, MetadataScope.SYSTEM);
    Assert.assertEquals(
      ImmutableMap.of(
        "type", KeyValueTable.class.getName(),
        description, "test dataset",
        ttl, "100000",
        creationTime, String.valueOf(createTime)
      ),
      dsSystemProperties
    );

    // verify artifact metadata
    ArtifactId artifactId = getArtifactId();
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataRecord(artifactId, MetadataScope.SYSTEM, ImmutableMap.<String, String>of(),
                           ImmutableSet.of(AllProgramsApp.class.getSimpleName()))
      ),
      getMetadata(artifactId, MetadataScope.SYSTEM)
    );
    // verify app system metadata
    ApplicationId app = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    Assert.assertEquals(
      ImmutableMap.builder()
        .put(ProgramType.FLOW.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + AllProgramsApp.NoOpFlow.NAME,
             AllProgramsApp.NoOpFlow.NAME)
        .put(ProgramType.MAPREDUCE.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + AllProgramsApp.NoOpMR.NAME,
             AllProgramsApp.NoOpMR.NAME)
        .put(ProgramType.MAPREDUCE.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + AllProgramsApp.NoOpMR2.NAME,
             AllProgramsApp.NoOpMR2.NAME)
        .put(ProgramType.SERVICE.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR +
               AllProgramsApp.NoOpService.NAME, AllProgramsApp.NoOpService.NAME)
        .put(ProgramType.SPARK.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + AllProgramsApp.NoOpSpark.NAME,
             AllProgramsApp.NoOpSpark.NAME)
        .put(ProgramType.WORKER.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + AllProgramsApp.NoOpWorker.NAME,
             AllProgramsApp.NoOpWorker.NAME)
        .put(ProgramType.WORKFLOW.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR
               + AllProgramsApp.NoOpWorkflow.NAME, AllProgramsApp.NoOpWorkflow.NAME)
        .put("schedule" + MetadataDataset.KEYVALUE_SEPARATOR + AllProgramsApp.SCHEDULE_NAME,
             AllProgramsApp.SCHEDULE_NAME + MetadataDataset.KEYVALUE_SEPARATOR + AllProgramsApp.SCHEDULE_DESCRIPTION)
        .put("version", ApplicationId.DEFAULT_VERSION)
        .build(),
      getProperties(app, MetadataScope.SYSTEM));
    Assert.assertEquals(ImmutableSet.of(AllProgramsApp.class.getSimpleName(), AllProgramsApp.NAME),
                        getTags(app, MetadataScope.SYSTEM));
    // verify program system metadata
    assertProgramSystemMetadata(app.flow(AllProgramsApp.NoOpFlow.NAME), "Realtime");
    assertProgramSystemMetadata(app.worker(AllProgramsApp.NoOpWorker.NAME), "Realtime");
    assertProgramSystemMetadata(app.service(AllProgramsApp.NoOpService.NAME), "Realtime");
    assertProgramSystemMetadata(app.mr(AllProgramsApp.NoOpMR.NAME), "Batch");
    assertProgramSystemMetadata(app.spark(AllProgramsApp.NoOpSpark.NAME), "Batch");
    assertProgramSystemMetadata(app.workflow(AllProgramsApp.NoOpWorkflow.NAME), "Batch");

    // update dataset properties to add the workflow.local.dataset property to it.
    datasetClient.update(datasetInstance, ImmutableMap.of(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY,
                                                          "true"));

    dsSystemTags = getTags(datasetInstance, MetadataScope.SYSTEM);
    System.out.println(dsSystemTags);
    Assert.assertEquals(
      ImmutableSet.of(AllProgramsApp.DATASET_NAME,
                      DatasetSystemMetadataWriter.BATCH_TAG,
                      AbstractSystemMetadataWriter.EXPLORE_TAG,
                      DatasetSystemMetadataWriter.LOCAL_DATASET_TAG),
      dsSystemTags);
  }

  @Test
  public void testExploreSystemTags() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(AllProgramsApp.class));

    //verify stream is explorable
    StreamId streamInstance = NamespaceId.DEFAULT.stream(AllProgramsApp.STREAM_NAME);
    Set<String> streamSystemTags = getTags(streamInstance, MetadataScope.SYSTEM);
    Assert.assertEquals(
      ImmutableSet.of(AllProgramsApp.STREAM_NAME,
                      AbstractSystemMetadataWriter.EXPLORE_TAG),
      streamSystemTags);

    // verify fileSet is explorable
    DatasetId datasetInstance = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME4);
    Set<String> dsSystemTags = getTags(datasetInstance, MetadataScope.SYSTEM);
    Assert.assertEquals(
      ImmutableSet.of(AllProgramsApp.DATASET_NAME4,
                      DatasetSystemMetadataWriter.BATCH_TAG,
                      AbstractSystemMetadataWriter.EXPLORE_TAG),
      dsSystemTags);

    //verify partitionedFileSet is explorable
    DatasetId datasetInstance2 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME5);
    Set<String> dsSystemTags2 = getTags(datasetInstance2, MetadataScope.SYSTEM);
    Assert.assertEquals(
      ImmutableSet.of(AllProgramsApp.DATASET_NAME5,
                      DatasetSystemMetadataWriter.BATCH_TAG,
                      AbstractSystemMetadataWriter.EXPLORE_TAG),
      dsSystemTags2);

    //verify that fileSet that isn't set to explorable does not have explore tag
    DatasetId datasetInstance3 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME6);
    Set<String> dsSystemTags3 = getTags(datasetInstance3, MetadataScope.SYSTEM);
    Assert.assertFalse(dsSystemTags3.contains(AbstractSystemMetadataWriter.EXPLORE_TAG));
    Assert.assertTrue(dsSystemTags3.contains(DatasetSystemMetadataWriter.BATCH_TAG));

    //verify that partitioned fileSet that isn't set to explorable does not have explore tag
    DatasetId datasetInstance4 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME7);
    Set<String> dsSystemTags4 = getTags(datasetInstance4, MetadataScope.SYSTEM);
    Assert.assertFalse(dsSystemTags4.contains(AbstractSystemMetadataWriter.EXPLORE_TAG));
    Assert.assertTrue(dsSystemTags4.contains(DatasetSystemMetadataWriter.BATCH_TAG));
  }

  @Test
  public void testSearchUsingSystemMetadata() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(AllProgramsApp.class));
    ApplicationId app = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    ArtifactId artifact = getArtifactId();
    try {
      // search artifacts
      assertArtifactSearch();
      // search app
      assertAppSearch(app, artifact);
      // search programs
      assertProgramSearch(app);
      // search data entities
      assertDataEntitySearch();
    } finally {
      // cleanup
      appClient.delete(app);
      artifactClient.delete(artifact);
    }
  }

  @Test
  public void testSystemScopeArtifacts() throws Exception {
    // add a system artifact. currently can't do this through the rest api (by design)
    // so bypass it and use the repository directly
    ArtifactId systemId = ArtifactId.fromIdParts(
      Arrays.asList(NamespaceId.SYSTEM.getNamespace(), "wordcount", "1.0.0"));
    File systemArtifact = createArtifactJarFile(WordCountApp.class, "wordcount", "1.0.0", new Manifest());

    StandaloneTester tester = STANDALONE.get();
    tester.addSystemArtifact(systemId.getArtifact(), systemId.toId().getVersion(), systemArtifact, null);

    // verify that user metadata can be added for system-scope artifacts
    Map<String, String> userProperties = ImmutableMap.of("systemArtifactKey", "systemArtifactValue");
    ImmutableSet<String> userTags = ImmutableSet.of("systemArtifactTag");
    addProperties(systemId, userProperties);
    addTags(systemId, userTags);

    // verify that user and system metadata can be retrieved for system-scope artifacts
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataRecord(systemId, MetadataScope.USER, userProperties, userTags),
        new MetadataRecord(systemId, MetadataScope.SYSTEM,
                           ImmutableMap.<String, String>of(), ImmutableSet.of("wordcount"))
      ),
      getMetadata(systemId)
    );

    // verify that system scope artifacts can be returned by a search in the default namespace
    // with no target type
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(systemId)),
      searchMetadata(NamespaceId.DEFAULT, "system*")
    );
    // with target type as artifact
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(systemId)),
      searchMetadata(NamespaceId.DEFAULT, "system*", MetadataSearchTargetType.ARTIFACT)
    );

    // verify that user metadata can be deleted for system-scope artifacts
    removeMetadata(systemId);
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataRecord(systemId, MetadataScope.USER, ImmutableMap.<String, String>of(),
                           ImmutableSet.<String>of()),
        new MetadataRecord(systemId, MetadataScope.SYSTEM,
                           ImmutableMap.<String, String>of(), ImmutableSet.of("wordcount"))
      ),
      getMetadata(systemId)
    );
    artifactClient.delete(systemId);
  }

  @Test
  public void testScopeQueryParam() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(WordCountApp.class));
    ApplicationId app = NamespaceId.DEFAULT.app(WordCountApp.class.getSimpleName());
    RESTClient restClient = new RESTClient(clientConfig);
    URL url = clientConfig.resolveNamespacedURLV3(NamespaceId.DEFAULT, "apps/WordCountApp/metadata?scope=system");
    Assert.assertEquals(
      HttpResponseStatus.OK.getCode(),
      restClient.execute(HttpRequest.get(url).build(), null).getResponseCode()
    );
    url = clientConfig.resolveNamespacedURLV3(NamespaceId.DEFAULT,
                                              "datasets/mydataset/metadata/properties?scope=SySTeM");
    Assert.assertEquals(
      HttpResponseStatus.OK.getCode(),
      restClient.execute(HttpRequest.get(url).build(), null).getResponseCode()
    );
    url = clientConfig.resolveNamespacedURLV3(NamespaceId.DEFAULT,
                                              "apps/WordCountApp/flows/WordCountFlow/metadata/tags?scope=USER");
    Assert.assertEquals(
      HttpResponseStatus.OK.getCode(),
      restClient.execute(HttpRequest.get(url).build(), null).getResponseCode()
    );
    url = clientConfig.resolveNamespacedURLV3(NamespaceId.DEFAULT, "streams/text/metadata?scope=user");
    Assert.assertEquals(
      HttpResponseStatus.OK.getCode(),
      restClient.execute(HttpRequest.get(url).build(), null).getResponseCode()
    );
    url = clientConfig.resolveNamespacedURLV3(NamespaceId.DEFAULT, "streams/text/metadata?scope=blah");
    Assert.assertEquals(
      HttpResponseStatus.BAD_REQUEST.getCode(),
      restClient.execute(HttpRequest.get(url).build(), null, HttpResponseStatus.BAD_REQUEST.getCode()).getResponseCode()
    );
    appClient.delete(app);

    // deleting the app does not delete the dataset and stream, delete them explicitly to clear their system metadata
    datasetClient.delete(NamespaceId.DEFAULT.dataset("mydataset"));
    streamClient.delete(NamespaceId.DEFAULT.stream("text"));
  }

  @Test
  public void testSearchTargetType() throws Exception {
    NamespaceId namespace = Ids.namespace("testSearchTargetType");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());

    appClient.deploy(namespace, createAppJarFile(AllProgramsApp.class));

    // Add metadata to app
    Set<String> tags = ImmutableSet.of("utag1", "utag2");
    ApplicationId appId = Ids.namespace(namespace.getNamespace()).app(AllProgramsApp.NAME);
    addTags(appId, tags);

    // Add metadata to stream
    tags = ImmutableSet.of("utag11");
    StreamId streamId = Ids.namespace(namespace.getNamespace()).stream(AllProgramsApp.STREAM_NAME);
    addTags(streamId, tags);

    // Add metadata to dataset
    tags = ImmutableSet.of("utag21");
    DatasetId datasetId = Ids.namespace(namespace.getNamespace()).dataset(AllProgramsApp.DATASET_NAME);
    addTags(datasetId, tags);

    // Search for single target type
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(appId)),
                        searchMetadata(namespace, "utag*", MetadataSearchTargetType.APP));
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(datasetId)),
                        searchMetadata(namespace, "utag*", MetadataSearchTargetType.DATASET));

    // Search for multiple target types
    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(datasetId),
                          new MetadataSearchResultRecord(streamId)
                        ),
                        searchMetadata(namespace, "utag*",
                                       ImmutableSet.of(
                                         MetadataSearchTargetType.DATASET,
                                         MetadataSearchTargetType.STREAM
                                       )
                        ));

    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(datasetId),
                          new MetadataSearchResultRecord(appId)
                        ),
                        searchMetadata(namespace, "utag*",
                                       ImmutableSet.of(
                                         MetadataSearchTargetType.APP,
                                         MetadataSearchTargetType.DATASET
                                       )
                        ));

    // Search for all target types
    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(datasetId),
                          new MetadataSearchResultRecord(appId),
                          new MetadataSearchResultRecord(streamId)
                        ),
                        searchMetadata(namespace, "utag*", MetadataSearchTargetType.ALL));
    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(datasetId),
                          new MetadataSearchResultRecord(appId),
                          new MetadataSearchResultRecord(streamId)
                        ),
                        searchMetadata(namespace, "utag*",
                                       ImmutableSet.of(
                                         MetadataSearchTargetType.DATASET,
                                         MetadataSearchTargetType.ALL
                                       )
                        ));
  }

  @Test
  public void testSearchMetadata() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(AllProgramsApp.class));

    Map<NamespacedEntityId, Metadata> expectedUserMetadata = new HashMap<>();

    // Add metadata to app
    Map<String, String> props = ImmutableMap.of("key1", "value1");
    Set<String> tags = ImmutableSet.of("tag1", "tag2");
    ApplicationId appId = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    addProperties(appId, props);
    addTags(appId, tags);
    expectedUserMetadata.put(appId, new Metadata(props, tags));

    // Add metadata to stream
    props = ImmutableMap.of("key10", "value10", "key11", "value11");
    tags = ImmutableSet.of("tag11");
    StreamId streamId = NamespaceId.DEFAULT.stream(AllProgramsApp.STREAM_NAME);
    addProperties(streamId, props);
    addTags(streamId, tags);
    expectedUserMetadata.put(streamId, new Metadata(props, tags));

    Set<MetadataSearchResultRecord> results =
      super.searchMetadata(NamespaceId.DEFAULT, "value*", ImmutableSet.<MetadataSearchTargetType>of());

    // Verify results
    Assert.assertEquals(expectedUserMetadata.keySet(), getEntities(results));
    for (MetadataSearchResultRecord result : results) {
      // User metadata has to match exactly since we know what we have set
      Assert.assertEquals(expectedUserMetadata.get(result.getEntityId()), result.getMetadata().get(MetadataScope.USER));
      // Make sure system metadata is returned, we cannot check for exact match since we haven't set it
      Metadata systemMetadata = result.getMetadata().get(MetadataScope.SYSTEM);
      Assert.assertNotNull(systemMetadata);
      Assert.assertFalse(systemMetadata.getProperties().isEmpty());
      Assert.assertFalse(systemMetadata.getTags().isEmpty());
    }
  }

  @Test
  public void testSearchMetadataDelete() throws Exception {
    NamespaceId namespace = new NamespaceId("ns1");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());

    // Deploy app
    appClient.deploy(namespace, createAppJarFile(WordCountApp.class, WordCountApp.class.getSimpleName(), "1.0"));

    Set<String> tags = ImmutableSet.of("tag1", "tag2");
    ArtifactId artifact = namespace.artifact("WordCountApp", "1.0");
    ApplicationId app = namespace.app("WordCountApp");
    ProgramId flow = app.flow("WordCountFlow");
    ProgramId service = app.service("WordFrequencyService");
    StreamId stream = namespace.stream("text");
    DatasetId datasetInstance = namespace.dataset("mydataset");
    StreamViewId view = stream.view("view");
    streamViewClient.createOrUpdate(view, new ViewSpecification(new FormatSpecification("csv", null, null)));

    // Add metadata
    addTags(app, tags);
    addTags(flow, tags);
    addTags(stream, tags);
    addTags(datasetInstance, tags);
    addTags(view, tags);

    // Assert metadata
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(stream),
                                        new MetadataSearchResultRecord(view)),
                        searchMetadata(namespace, "text"));
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(datasetInstance)),
                        searchMetadata(namespace, "mydataset"));
    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(app),
                          new MetadataSearchResultRecord(flow),
                          new MetadataSearchResultRecord(artifact),
                          new MetadataSearchResultRecord(service)
                        ),
                        searchMetadata(namespace, "word*"));
    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(app),
                          new MetadataSearchResultRecord(flow),
                          new MetadataSearchResultRecord(stream),
                          new MetadataSearchResultRecord(datasetInstance),
                          new MetadataSearchResultRecord(view)
                        ),
                        searchMetadata(namespace, "tag1"));

    // Delete entities
    appClient.delete(app);
    streamViewClient.delete(view);
    streamClient.delete(stream);
    datasetClient.delete(datasetInstance);
    artifactClient.delete(artifact);

    // Assert no metadata
    Assert.assertEquals(ImmutableSet.of(), searchMetadata(namespace, "text"));
    Assert.assertEquals(ImmutableSet.of(), searchMetadata(namespace, "mydataset"));
    Assert.assertEquals(ImmutableSet.of(), searchMetadata(namespace, "word*"));
    Assert.assertEquals(ImmutableSet.of(), searchMetadata(namespace, "tag1"));
  }

  @Test
  public void testSearchMetadataDeleteNamespace() throws Exception {
    NamespaceId namespace = new NamespaceId("ns2");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());

    // Deploy app
    appClient.deploy(namespace, createAppJarFile(WordCountApp.class, WordCountApp.class.getSimpleName(), "1.0"));

    Set<String> tags = ImmutableSet.of("tag1", "tag2");
    ArtifactId artifact = namespace.artifact("WordCountApp", "1.0");
    ApplicationId app = namespace.app("WordCountApp");
    ProgramId flow = app.flow("WordCountFlow");
    ProgramId service = app.service("WordFrequencyService");
    StreamId stream = namespace.stream("text");
    DatasetId datasetInstance = namespace.dataset("mydataset");
    StreamViewId view = stream.view("view");
    streamViewClient.createOrUpdate(view, new ViewSpecification(new FormatSpecification("csv", null, null)));


    // Add metadata
    addTags(app, tags);
    addTags(flow, tags);
    addTags(stream, tags);
    addTags(datasetInstance, tags);
    addTags(view, tags);

    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(stream),
                                        new MetadataSearchResultRecord(view)),
                        searchMetadata(namespace, "text"));
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(datasetInstance)),
                        searchMetadata(namespace, "mydataset"));
    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(app),
                          new MetadataSearchResultRecord(flow),
                          new MetadataSearchResultRecord(artifact),
                          new MetadataSearchResultRecord(service)
                        ),
                        searchMetadata(namespace, "word*"));
    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(app),
                          new MetadataSearchResultRecord(flow),
                          new MetadataSearchResultRecord(stream),
                          new MetadataSearchResultRecord(datasetInstance),
                          new MetadataSearchResultRecord(view)
                        ),
                        searchMetadata(namespace, "tag1"));

    // Delete namespace
    namespaceClient.delete(namespace);

    // Assert no metadata
    Assert.assertEquals(ImmutableSet.of(), searchMetadata(namespace, "text"));
    Assert.assertEquals(ImmutableSet.of(), searchMetadata(namespace, "mydataset"));
    Assert.assertEquals(ImmutableSet.of(), searchMetadata(namespace, "word*"));
    Assert.assertEquals(ImmutableSet.of(), searchMetadata(namespace, "tag1"));
  }

  private Set<NamespacedEntityId> getEntities(Set<MetadataSearchResultRecord> results) {
    return Sets.newHashSet(
      Iterables.transform(results, new Function<MetadataSearchResultRecord, NamespacedEntityId>() {
        @Override
        public NamespacedEntityId apply(MetadataSearchResultRecord input) {
          return input.getEntityId();
        }
      })
    );
  }

  private void assertProgramSystemMetadata(ProgramId programId, String mode) throws Exception {
    Assert.assertEquals(ImmutableMap.of("version", ApplicationId.DEFAULT_VERSION),
                        getProperties(programId, MetadataScope.SYSTEM));
    Set<String> expected = ImmutableSet.of(programId.getProgram(), programId.getType().getPrettyName(), mode);
    if (ProgramType.WORKFLOW == programId.getType()) {
      expected = ImmutableSet.of(programId.getProgram(), programId.getType().getPrettyName(), mode,
                                 AllProgramsApp.NoOpAction.class.getSimpleName(), AllProgramsApp.NoOpMR.NAME);
    }
    Assert.assertEquals(expected, getTags(programId, MetadataScope.SYSTEM));
  }

  private void assertArtifactSearch() throws Exception {
    // add a plugin artifact.
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE,
                                     AllProgramsApp.AppPlugin.class.getPackage().getName());
    ArtifactId pluginArtifact = NamespaceId.DEFAULT.artifact("plugins", "1.0.0");
    addPluginArtifact(pluginArtifact, AllProgramsApp.AppPlugin.class, manifest, null);
    // search using artifact name
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(
      new MetadataSearchResultRecord(pluginArtifact));
    Set<MetadataSearchResultRecord> results = searchMetadata(NamespaceId.DEFAULT, "plugins");
    Assert.assertEquals(expected, results);
    // search the artifact given a plugin
    results = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.PLUGIN_TYPE);
    Assert.assertEquals(expected, results);
    results = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.PLUGIN_NAME + ":" + AllProgramsApp.PLUGIN_TYPE);
    Assert.assertEquals(expected, results);
    results = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.PLUGIN_NAME, MetadataSearchTargetType.ARTIFACT);
    Assert.assertEquals(expected, results);
    // add a user tag to the application with the same name as the plugin
    addTags(application, ImmutableSet.of(AllProgramsApp.PLUGIN_NAME));
    // search for all entities with plugin name. Should return both artifact and application
    results = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.PLUGIN_NAME);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(application),
                      new MetadataSearchResultRecord(pluginArtifact)),
      results);
    // search for all entities for a plugin with the plugin name. Should return only the artifact, since for the
    // application, its just a tag, not a plugin
    results = searchMetadata(NamespaceId.DEFAULT, "plugin:" + AllProgramsApp.PLUGIN_NAME + ":*");
    Assert.assertEquals(expected, results);
  }

  private void assertAppSearch(ApplicationId app, ArtifactId artifact) throws Exception {
    // using app name
    ImmutableSet<MetadataSearchResultRecord> expected = ImmutableSet.of(
      new MetadataSearchResultRecord(app));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NAME));
    // using artifact name: both app and artifact should match
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(app),
                      new MetadataSearchResultRecord(artifact)),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.class.getSimpleName()));
    // using program names
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpFlow.NAME,
                                                 MetadataSearchTargetType.APP));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpMR.NAME,
                                                 MetadataSearchTargetType.APP));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpService.NAME,
                                                 MetadataSearchTargetType.APP));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpSpark.NAME,
                                                 MetadataSearchTargetType.APP));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpWorker.NAME,
                                                 MetadataSearchTargetType.APP));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpWorkflow.NAME,
                                                 MetadataSearchTargetType.APP));
    // using program types
    Assert.assertEquals(
      expected, searchMetadata(NamespaceId.DEFAULT,
                               ProgramType.FLOW.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                               MetadataSearchTargetType.APP));
    Assert.assertEquals(
      expected, searchMetadata(NamespaceId.DEFAULT,
                               ProgramType.MAPREDUCE.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                               MetadataSearchTargetType.APP));
    Assert.assertEquals(
      ImmutableSet.builder().addAll(expected).add(new MetadataSearchResultRecord(application)).build(),
      searchMetadata(NamespaceId.DEFAULT,
                     ProgramType.SERVICE.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                     MetadataSearchTargetType.APP));
    Assert.assertEquals(
      expected, searchMetadata(NamespaceId.DEFAULT,
                               ProgramType.SPARK.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                               MetadataSearchTargetType.APP));
    Assert.assertEquals(
      expected, searchMetadata(NamespaceId.DEFAULT,
                               ProgramType.WORKER.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                               MetadataSearchTargetType.APP));
    Assert.assertEquals(
      expected, searchMetadata(NamespaceId.DEFAULT,
                               ProgramType.WORKFLOW.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                               MetadataSearchTargetType.APP));

    // using schedule
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.SCHEDULE_NAME));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, "EveryMinute"));
  }

  private void assertProgramSearch(ApplicationId app) throws Exception {
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(app.mr(AllProgramsApp.NoOpMR.NAME)),
        new MetadataSearchResultRecord(app.mr(AllProgramsApp.NoOpMR2.NAME)),
        new MetadataSearchResultRecord(app.workflow(AllProgramsApp.NoOpWorkflow.NAME)),
        new MetadataSearchResultRecord(app.spark(AllProgramsApp.NoOpSpark.NAME)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME2)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME3)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME4)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME5)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME6)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME7)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DS_WITH_SCHEMA_NAME)),
        new MetadataSearchResultRecord(myds)
      ),
      searchMetadata(NamespaceId.DEFAULT, "Batch"));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(app.flow(AllProgramsApp.NoOpFlow.NAME)),
        new MetadataSearchResultRecord(app.service(AllProgramsApp.NoOpService.NAME)),
        new MetadataSearchResultRecord(app.worker(AllProgramsApp.NoOpWorker.NAME)),
        new MetadataSearchResultRecord(
          NamespaceId.DEFAULT.app(AppWithDataset.class.getSimpleName()).service("PingService"))
      ),
      searchMetadata(NamespaceId.DEFAULT, "Realtime"));

    // Using program names
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.flow(AllProgramsApp.NoOpFlow.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpFlow.NAME, MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.mr(AllProgramsApp.NoOpMR.NAME)),
        new MetadataSearchResultRecord(
          app.workflow(AllProgramsApp.NoOpWorkflow.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpMR.NAME, MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.service(AllProgramsApp.NoOpService.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpService.NAME, MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.spark(AllProgramsApp.NoOpSpark.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpSpark.NAME, MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.worker(AllProgramsApp.NoOpWorker.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpWorker.NAME, MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.workflow(AllProgramsApp.NoOpWorkflow.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpWorkflow.NAME, MetadataSearchTargetType.PROGRAM));

    // using program types
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.flow(AllProgramsApp.NoOpFlow.NAME))),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.FLOW.getPrettyName(), MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.mr(AllProgramsApp.NoOpMR.NAME)),
        new MetadataSearchResultRecord(
          app.mr(AllProgramsApp.NoOpMR2.NAME))),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.MAPREDUCE.getPrettyName(), MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.service(AllProgramsApp.NoOpService.NAME)),
        new MetadataSearchResultRecord(pingService)),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.SERVICE.getPrettyName(), MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.spark(AllProgramsApp.NoOpSpark.NAME))),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.SPARK.getPrettyName(), MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.worker(AllProgramsApp.NoOpWorker.NAME))),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.WORKER.getPrettyName(), MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.workflow(AllProgramsApp.NoOpWorkflow.NAME))),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.WORKFLOW.getPrettyName(), MetadataSearchTargetType.PROGRAM));
  }

  private void assertDataEntitySearch() throws Exception {
    DatasetId datasetInstance = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME);
    DatasetId datasetInstance2 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME2);
    DatasetId datasetInstance3 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME3);
    DatasetId datasetInstance4 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME4);
    DatasetId datasetInstance5 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME5);
    DatasetId datasetInstance6 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME6);
    DatasetId datasetInstance7 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME7);
    DatasetId dsWithSchema = NamespaceId.DEFAULT.dataset(AllProgramsApp.DS_WITH_SCHEMA_NAME);
    StreamId streamId = NamespaceId.DEFAULT.stream(AllProgramsApp.STREAM_NAME);
    StreamViewId view = streamId.view("view");

    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(
      new MetadataSearchResultRecord(streamId),
      new MetadataSearchResultRecord(mystream)
    );

    Set<MetadataSearchResultRecord> expectedWithView = ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(expected)
      .add(new MetadataSearchResultRecord(myview)).build();

    // schema search with fieldname
    Set<MetadataSearchResultRecord> metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "body");
    Assert.assertEquals(expectedWithView, metadataSearchResultRecords);

    // schema search with fieldname and fieldtype
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "body:" + Schema.Type.STRING.toString());
    Assert.assertEquals(expected, metadataSearchResultRecords);

    // schema search for partial fieldname
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "bo*");
    Assert.assertEquals(expectedWithView, metadataSearchResultRecords);

    // schema search with fieldname and all/partial fieldtype
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "body:STR*");
    Assert.assertEquals(expected, metadataSearchResultRecords);

    // schema search for a field with the given fieldname:fieldtype
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "body:STRING+field1:STRING");
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>builder()
                          .addAll(expected)
                          .add(new MetadataSearchResultRecord(dsWithSchema))
                          .build(),
                        metadataSearchResultRecords);

    // create a view
    Schema viewSchema = Schema.recordOf("record",
                                        Schema.Field.of("viewBody", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    streamViewClient.createOrUpdate(view, new ViewSpecification(new FormatSpecification("format", viewSchema)));

    // search all entities that have a defined schema
    // add a user property with "schema" as key
    Map<String, String> datasetProperties = ImmutableMap.of("schema", "schemaValue");
    addProperties(datasetInstance, datasetProperties);

    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "schema:*");
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>builder()
                          .addAll(expectedWithView)
                          .add(new MetadataSearchResultRecord(datasetInstance))
                          .add(new MetadataSearchResultRecord(dsWithSchema))
                          .add(new MetadataSearchResultRecord(view))
                          .build(),
                        metadataSearchResultRecords);

    // search dataset
    ImmutableSet<MetadataSearchResultRecord> expectedKvTables = ImmutableSet.of(
      new MetadataSearchResultRecord(datasetInstance),
      new MetadataSearchResultRecord(datasetInstance2),
      new MetadataSearchResultRecord(datasetInstance3),
      new MetadataSearchResultRecord(myds)
    );

    ImmutableSet<MetadataSearchResultRecord> expectedExplorableDatasets =
      ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(expectedKvTables)
      .add(new MetadataSearchResultRecord(datasetInstance4))
      .add(new MetadataSearchResultRecord(datasetInstance5))
      .add(new MetadataSearchResultRecord(dsWithSchema))
      .build();
    ImmutableSet<MetadataSearchResultRecord> expectedAllDatasets = ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(expectedExplorableDatasets)
      .add(new MetadataSearchResultRecord(datasetInstance6))
      .add(new MetadataSearchResultRecord(datasetInstance7))
      .build();
    ImmutableSet<MetadataSearchResultRecord> expectedExplorables =
      ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(expectedExplorableDatasets)
      .add(new MetadataSearchResultRecord(streamId))
      .add(new MetadataSearchResultRecord(mystream))
      .build();
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "explore");
    Assert.assertEquals(expectedExplorables, metadataSearchResultRecords);
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, KeyValueTable.class.getName());
    Assert.assertEquals(expectedKvTables, metadataSearchResultRecords);
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "type:*");
    Assert.assertEquals(expectedAllDatasets, metadataSearchResultRecords);

    // search using ttl
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "ttl:*");
    Assert.assertEquals(expected, metadataSearchResultRecords);

    // search using names
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.STREAM_NAME);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(streamId),
                      new MetadataSearchResultRecord(view)),
      metadataSearchResultRecords);

    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.STREAM_NAME,
                                                 MetadataSearchTargetType.STREAM);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(streamId)),
                        metadataSearchResultRecords);
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.STREAM_NAME,
                                                 MetadataSearchTargetType.VIEW);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(view)),
                        metadataSearchResultRecords);
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "view",
                                                 MetadataSearchTargetType.VIEW);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(view)),
                        metadataSearchResultRecords);
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.DATASET_NAME);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(datasetInstance)),
                        metadataSearchResultRecords);
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.DS_WITH_SCHEMA_NAME);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(dsWithSchema)),
                        metadataSearchResultRecords);
  }

  private void removeAllMetadata() throws Exception {
    removeMetadata(application);
    removeMetadata(pingService);
    removeMetadata(myds);
    removeMetadata(mystream);
    removeMetadata(myview);
    removeMetadata(artifactId);
  }

  private void assertCleanState(@Nullable MetadataScope scope) throws Exception {
    assertEmptyMetadata(getMetadata(application, scope), scope);
    assertEmptyMetadata(getMetadata(pingService, scope), scope);
    assertEmptyMetadata(getMetadata(myds, scope), scope);
    assertEmptyMetadata(getMetadata(mystream, scope), scope);
    assertEmptyMetadata(getMetadata(myview, scope), scope);
    assertEmptyMetadata(getMetadata(artifactId, scope), scope);
  }

  private void assertEmptyMetadata(Set<MetadataRecord> entityMetadata, @Nullable MetadataScope scope) {
    // should have two metadata records - one for each scope, both should have empty properties and tags
    int expectedRecords = (scope == null) ? 2 : 1;
    Assert.assertEquals(expectedRecords, entityMetadata.size());
    for (MetadataRecord metadataRecord : entityMetadata) {
      Assert.assertTrue(metadataRecord.getProperties().isEmpty());
      Assert.assertTrue(metadataRecord.getTags().isEmpty());
    }
  }

  /**
   * Returns the artifact id of the deployed application. Need this because we don't know the exact version.
   */
  private ArtifactId getArtifactId() throws Exception {
    Iterable<ArtifactSummary> filtered =
      Iterables.filter(artifactClient.list(NamespaceId.DEFAULT), new Predicate<ArtifactSummary>() {
        @Override
        public boolean apply(ArtifactSummary artifactSummary) {
          return AllProgramsApp.class.getSimpleName().equals(artifactSummary.getName());
        }
      });
    ArtifactSummary artifact = Iterables.getOnlyElement(filtered);
    return NamespaceId.DEFAULT.artifact(artifact.getName(), artifact.getVersion());
  }

  private Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespaceId, String query,
                                                         MetadataSearchTargetType target) throws Exception {
    return searchMetadata(namespaceId, query, ImmutableSet.of(target));
  }

  private Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespaceId, String query) throws Exception {
    return searchMetadata(namespaceId, query, ImmutableSet.<MetadataSearchTargetType>of());
  }

  /**
   * strips metadata from search results
   */
  protected Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespaceId, String query,
                                                           Set<MetadataSearchTargetType> targets) throws Exception {
    Set<MetadataSearchResultRecord> results = super.searchMetadata(namespaceId, query, targets);
    Set<MetadataSearchResultRecord> transformed = new HashSet<>();
    for (MetadataSearchResultRecord result : results) {
      transformed.add(new MetadataSearchResultRecord(result.getEntityId()));
    }
    return transformed;
  }
}
