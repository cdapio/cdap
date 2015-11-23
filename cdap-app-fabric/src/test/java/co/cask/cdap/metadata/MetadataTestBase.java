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

import co.cask.cdap.client.LineageClient;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import com.google.common.collect.Iterators;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Base class for metadata tests.
 */
public abstract class MetadataTestBase extends AppFabricTestBase {
  private static MetadataClient metadataClient;
  private static LineageClient lineageClient;

  @BeforeClass
  public static void setup() throws MalformedURLException {
    DiscoveryServiceClient discoveryClient = getInjector().getInstance(DiscoveryServiceClient.class);
    ServiceDiscovered metadataHttpDiscovered = discoveryClient.discover(Constants.Service.METADATA_SERVICE);
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(metadataHttpDiscovered);
    Discoverable discoverable = endpointStrategy.pick(1, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);
    String host = "127.0.0.1";
    int port = discoverable.getSocketAddress().getPort();
    ConnectionConfig connectionConfig = ConnectionConfig.builder().setHostname(host).setPort(port).build();
    ClientConfig config = ClientConfig.builder().setConnectionConfig(connectionConfig).build();
    metadataClient = new MetadataClient(config);
    lineageClient = new LineageClient(config);
  }

  protected void addProperties(Id.Application app, @Nullable Map<String, String> properties) throws Exception {
    metadataClient.addProperties(app, properties);
  }

  protected void addProperties(final Id.Application app, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(app, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addProperties(Id.Artifact artifact, @Nullable Map<String, String> properties) throws Exception {
    metadataClient.addProperties(artifact, properties);
  }

  protected void addProperties(final Id.Artifact artifact, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(artifact, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addProperties(Id.Program program, @Nullable Map<String, String> properties) throws Exception {
    metadataClient.addProperties(program, properties);
  }

  protected void addProperties(final Id.Program program, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(program, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addProperties(Id.DatasetInstance dataset, @Nullable Map<String, String> properties) throws Exception {
    metadataClient.addProperties(dataset, properties);
  }

  protected void addProperties(final Id.DatasetInstance dataset, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(dataset, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addProperties(Id.Stream stream, @Nullable Map<String, String> properties)
    throws Exception {
    metadataClient.addProperties(stream, properties);
  }

  protected void addProperties(Id.Stream.View view, @Nullable Map<String, String> properties)
    throws Exception {
    metadataClient.addProperties(view, properties);
  }

  protected void addProperties(final Id.Stream stream, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(stream, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addProperties(final Id.Stream.View view, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(view, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected Set<MetadataRecord> getMetadata(Id.Application app) throws Exception {
    return metadataClient.getMetadata(app);
  }

  protected Set<MetadataRecord> getMetadata(Id.Artifact artifact) throws Exception {
    return metadataClient.getMetadata(artifact);
  }

  protected Set<MetadataRecord> getMetadata(Id.Program program) throws Exception {
    return metadataClient.getMetadata(program);
  }

  protected Set<MetadataRecord> getMetadata(Id.DatasetInstance dataset) throws Exception {
    return metadataClient.getMetadata(dataset);
  }

  protected Set<MetadataRecord> getMetadata(Id.Stream stream) throws Exception {
    return metadataClient.getMetadata(stream);
  }

  protected Set<MetadataRecord> getMetadata(Id.Stream.View view) throws Exception {
    return metadataClient.getMetadata(view);
  }

  // Currently, getMetadata(entity) returns a single-element set, so we can get the properties from there
  protected Map<String, String> getProperties(Id.Application app) throws Exception {
    return Iterators.getOnlyElement(getMetadata(app).iterator()).getProperties();
  }

  protected Map<String, String> getProperties(Id.Artifact artifact) throws Exception {
    return Iterators.getOnlyElement(getMetadata(artifact).iterator()).getProperties();
  }

  protected Map<String, String> getProperties(Id.Program program) throws Exception {
    return Iterators.getOnlyElement(getMetadata(program).iterator()).getProperties();
  }

  protected Map<String, String> getProperties(Id.DatasetInstance dataset) throws Exception {
    return Iterators.getOnlyElement(getMetadata(dataset).iterator()).getProperties();
  }

  protected Map<String, String> getProperties(Id.Stream stream) throws Exception {
    return Iterators.getOnlyElement(getMetadata(stream).iterator()).getProperties();
  }

  protected Map<String, String> getProperties(Id.Stream.View view) throws Exception {
    return Iterators.getOnlyElement(getMetadata(view).iterator()).getProperties();
  }

  protected void getPropertiesFromInvalidEntity(Id.Application app) throws Exception {
    try {
      getProperties(app);
      Assert.fail("Expected not to be able to get properties from invalid entity: " + app);
    } catch (NotFoundException expected) {
      // expected
    }
  }

  protected void getPropertiesFromInvalidEntity(Id.Program program) throws Exception {
    try {
      getProperties(program);
      Assert.fail("Expected not to be able to get properties from invalid entity: " + program);
    } catch (NotFoundException expected) {
      // expected
    }
  }

  protected void getPropertiesFromInvalidEntity(Id.DatasetInstance dataset) throws Exception {
    try {
      getProperties(dataset);
      Assert.fail("Expected not to be able to get properties from invalid entity: " + dataset);
    } catch (NotFoundException expected) {
      // expected
    }
  }

  protected void getPropertiesFromInvalidEntity(Id.Stream stream) throws Exception {
    try {
      getProperties(stream);
      Assert.fail("Expected not to be able to get properties from invalid entity: " + stream);
    } catch (NotFoundException expected) {
      // expected
    }
  }
  protected void getPropertiesFromInvalidEntity(Id.Stream.View view) throws Exception {
    try {
      getProperties(view);
      Assert.fail("Expected not to be able to get properties from invalid entity: " + view);
    } catch (NotFoundException expected) {
      // expected
    }
  }

  protected void removeMetadata(Id.Application app) throws Exception {
    metadataClient.removeMetadata(app);
  }

  protected void removeMetadata(Id.Artifact artifact) throws Exception {
    metadataClient.removeMetadata(artifact);
  }

  protected void removeMetadata(Id.Program program) throws Exception {
    metadataClient.removeMetadata(program);
  }

  protected void removeMetadata(Id.DatasetInstance dataset) throws Exception {
    metadataClient.removeMetadata(dataset);
  }

  protected void removeMetadata(Id.Stream stream) throws Exception {
    metadataClient.removeMetadata(stream);
  }

  protected void removeMetadata(Id.Stream.View view) throws Exception {
    metadataClient.removeMetadata(view);
  }

  protected void removeProperties(Id.Application app) throws Exception {
    metadataClient.removeProperties(app);
  }

  private void removeProperty(Id.Application app, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(app, propertyToRemove);
  }

  protected void removeProperties(Id.Artifact artifact) throws Exception {
    metadataClient.removeProperties(artifact);
  }

  private void removeProperty(Id.Artifact artifact, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(artifact, propertyToRemove);
  }

  protected void removeProperties(Id.Program program) throws Exception {
    metadataClient.removeProperties(program);
  }

  protected void removeProperty(Id.Program program, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(program, propertyToRemove);
  }

  protected void removeProperties(Id.DatasetInstance dataset) throws Exception {
    metadataClient.removeProperties(dataset);
  }

  protected void removeProperty(Id.DatasetInstance dataset, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(dataset, propertyToRemove);
  }

  protected void removeProperties(Id.Stream stream) throws Exception {
    metadataClient.removeProperties(stream);
  }

  protected void removeProperties(Id.Stream.View view) throws Exception {
    metadataClient.removeProperties(view);
  }

  protected void removeProperty(Id.Stream stream, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(stream, propertyToRemove);
  }

  protected void removeProperty(Id.Stream.View view, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(view, propertyToRemove);
  }

  protected void addTags(Id.Application app, @Nullable Set<String> tags) throws Exception {
    metadataClient.addTags(app, tags);
  }

  protected void addTags(final Id.Application app, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(app, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addTags(Id.Artifact artifact, @Nullable Set<String> tags) throws Exception {
    metadataClient.addTags(artifact, tags);
  }

  protected void addTags(final Id.Artifact artifact, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(artifact, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addTags(Id.Program program, @Nullable Set<String> tags)
    throws Exception {
    metadataClient.addTags(program, tags);
  }

  protected void addTags(final Id.Program program, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(program, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addTags(Id.DatasetInstance dataset, @Nullable Set<String> tags) throws Exception {
    metadataClient.addTags(dataset, tags);
  }

  protected void addTags(final Id.DatasetInstance dataset, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(dataset, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addTags(Id.Stream stream, @Nullable Set<String> tags) throws Exception {
    metadataClient.addTags(stream, tags);
  }

  protected void addTags(Id.Stream.View view, @Nullable Set<String> tags) throws Exception {
    metadataClient.addTags(view, tags);
  }

  protected void addTags(final Id.Stream stream, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(stream, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addTags(final Id.Stream.View view, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(view, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected Set<MetadataSearchResultRecord> searchMetadata(Id.Namespace namespaceId, String query,
                                                           MetadataSearchTargetType target) throws Exception {
    return metadataClient.searchMetadata(namespaceId, query, target);
  }

  // Currently, getMetadata(entity) returns a single-element set, so we can get the tags from there
  protected Set<String> getTags(Id.Application app) throws Exception {
    return Iterators.getOnlyElement(getMetadata(app).iterator()).getTags();
  }

  protected Set<String> getTags(Id.Artifact artifact) throws Exception {
    return Iterators.getOnlyElement(getMetadata(artifact).iterator()).getTags();
  }

  protected Set<String> getTags(Id.Program program) throws Exception {
    return Iterators.getOnlyElement(getMetadata(program).iterator()).getTags();
  }

  protected Set<String> getTags(Id.DatasetInstance dataset) throws Exception {
    return Iterators.getOnlyElement(getMetadata(dataset).iterator()).getTags();
  }

  protected Set<String> getTags(Id.Stream stream) throws Exception {
    return Iterators.getOnlyElement(getMetadata(stream).iterator()).getTags();
  }

  protected Set<String> getTags(Id.Stream.View view) throws Exception {
    return Iterators.getOnlyElement(getMetadata(view).iterator()).getTags();
  }

  protected void removeTags(Id.Application app) throws Exception {
    metadataClient.removeTags(app);
  }

  protected void removeTag(Id.Application app, String tagToRemove) throws Exception {
    metadataClient.removeTag(app, tagToRemove);
  }

  protected void removeTags(Id.Artifact artifact) throws Exception {
    metadataClient.removeTags(artifact);
  }

  protected void removeTag(Id.Artifact artifact, String tagToRemove) throws Exception {
    metadataClient.removeTag(artifact, tagToRemove);
  }

  protected void removeTags(Id.Program program) throws Exception {
    metadataClient.removeTags(program);
  }

  private void removeTag(Id.Program program, String tagToRemove) throws Exception {
    metadataClient.removeTag(program, tagToRemove);
  }

  protected void removeTags(Id.DatasetInstance dataset) throws Exception {
    metadataClient.removeTags(dataset);
  }

  protected void removeTag(Id.DatasetInstance dataset, String tagToRemove) throws Exception {
    metadataClient.removeTag(dataset, tagToRemove);
  }

  protected void removeTags(Id.Stream stream) throws Exception {
    metadataClient.removeTags(stream);
  }

  protected void removeTags(Id.Stream.View view) throws Exception {
    metadataClient.removeTags(view);
  }

  protected void removeTag(Id.Stream stream, String tagToRemove) throws Exception {
    metadataClient.removeTag(stream, tagToRemove);
  }

  protected void removeTag(Id.Stream.View view, String tagToRemove) throws Exception {
    metadataClient.removeTag(view, tagToRemove);
  }

  // expect an exception during fetching of lineage
  protected void fetchLineage(Id.DatasetInstance datasetInstance, long start, long end, int levels,
                              Class<? extends Exception> expectedExceptionClass) throws Exception {
    fetchLineage(datasetInstance, Long.toString(start), Long.toString(end), levels, expectedExceptionClass);
  }

  // expect an exception during fetching of lineage
  protected void fetchLineage(final Id.DatasetInstance datasetInstance, final String start, final String end,
                              final int levels, Class<? extends Exception> expectedExceptionClass) throws Exception {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        fetchLineage(datasetInstance, start, end, levels);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected LineageRecord fetchLineage(Id.DatasetInstance datasetInstance, long start, long end,
                                       int levels) throws Exception {
    return lineageClient.getLineage(datasetInstance, start, end, levels);
  }

  protected LineageRecord fetchLineage(Id.DatasetInstance datasetInstance, String start, String end,
                                       int levels) throws Exception {
    return lineageClient.getLineage(datasetInstance, start, end, levels);
  }

  protected LineageRecord fetchLineage(Id.Stream stream, long start, long end, int levels) throws Exception {
    return lineageClient.getLineage(stream, start, end, levels);
  }

  protected LineageRecord fetchLineage(Id.Stream stream, String start, String end, int levels) throws Exception {
    return lineageClient.getLineage(stream, start, end, levels);
  }

  protected Set<MetadataRecord> fetchRunMetadata(Id.Run run) throws Exception {
    return metadataClient.getMetadata(run);
  }

  protected void assertRunMetadataNotFound(Id.Run run) throws Exception {
    try {
      fetchRunMetadata(run);
      Assert.fail("Excepted not to fetch Metadata for a nonexistent Run.");
    } catch (NotFoundException expected) {
      // expected
    }
  }

  private <T> void expectException(Callable<T> callable, Class<? extends Exception> expectedExceptionClass) {
    try {
      callable.call();
      Assert.fail("Expected to have exception of class: " + expectedExceptionClass);
    } catch (Exception e) {
      Assert.assertTrue(e.getClass() == expectedExceptionClass);
    }
  }
}
