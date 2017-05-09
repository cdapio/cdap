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

import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.lineage.CollapseType;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import com.google.common.collect.Iterators;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Base class for metadata tests.
 */
public abstract class MetadataTestBase extends ClientTestBase {

  protected static final NamespaceId TEST_NAMESPACE1 = new NamespaceId("testnamespace1");

  private MetadataClient metadataClient;
  private LineageClient lineageClient;
  protected ArtifactClient artifactClient;
  protected NamespaceClient namespaceClient;
  protected ApplicationClient appClient;
  protected ProgramClient programClient;
  protected StreamClient streamClient;
  protected StreamViewClient streamViewClient;
  protected DatasetClient datasetClient;

  @Before
  public void beforeTest() throws IOException {
    metadataClient = new MetadataClient(getClientConfig());
    lineageClient = new LineageClient(getClientConfig());
    artifactClient = new ArtifactClient(getClientConfig());
    namespaceClient = new NamespaceClient(getClientConfig());
    appClient = new ApplicationClient(getClientConfig());
    programClient = new ProgramClient(getClientConfig());
    streamClient = new StreamClient(getClientConfig());
    streamViewClient = new StreamViewClient(getClientConfig());
    datasetClient = new DatasetClient(getClientConfig());
  }

  protected void addAppArtifact(ArtifactId artifactId, Class<?> cls) throws Exception {
    artifactClient.add(artifactId, null, Files.newInputStreamSupplier(createAppJarFile(cls)));
  }

  protected void addPluginArtifact(ArtifactId artifactId, Class<?> cls, Manifest manifest,
                                   @Nullable Set<ArtifactRange> parents) throws Exception {
    artifactClient.add(artifactId, parents, Files.newInputStreamSupplier(createArtifactJarFile(cls, manifest)));
  }

  protected void addProperties(ApplicationId app, @Nullable Map<String, String> properties) throws Exception {
    metadataClient.addProperties(app.toId(), properties);
  }

  protected void addProperties(final ApplicationId app, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(app, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addProperties(ArtifactId artifact, @Nullable Map<String, String> properties) throws Exception {
    metadataClient.addProperties(artifact.toId(), properties);
  }

  protected void addProperties(final ArtifactId artifact, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(artifact, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addProperties(ProgramId program, @Nullable Map<String, String> properties) throws Exception {
    metadataClient.addProperties(program.toId(), properties);
  }

  protected void addProperties(final ProgramId program, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(program, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addProperties(DatasetId dataset, @Nullable Map<String, String> properties) throws Exception {
    metadataClient.addProperties(dataset.toId(), properties);
  }

  protected void addProperties(final DatasetId dataset, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(dataset, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addProperties(StreamId stream, @Nullable Map<String, String> properties)
    throws Exception {
    metadataClient.addProperties(stream.toId(), properties);
  }

  protected void addProperties(StreamViewId view, @Nullable Map<String, String> properties)
    throws Exception {
    metadataClient.addProperties(view.toId(), properties);
  }

  protected void addProperties(final StreamId stream, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(stream, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addProperties(final StreamViewId view, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addProperties(view, properties);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected Set<MetadataRecord> getMetadata(ApplicationId app) throws Exception {
    return getMetadata(app, null);
  }

  protected Set<MetadataRecord> getMetadata(ArtifactId artifact) throws Exception {
    return getMetadata(artifact, null);
  }

  protected Set<MetadataRecord> getMetadata(ProgramId program) throws Exception {
    return getMetadata(program, null);
  }

  protected Set<MetadataRecord> getMetadata(DatasetId dataset) throws Exception {
    return getMetadata(dataset, null);
  }

  protected Set<MetadataRecord> getMetadata(StreamId stream) throws Exception {
    return getMetadata(stream, null);
  }

  protected Set<MetadataRecord> getMetadata(StreamViewId view) throws Exception {
    return getMetadata(view, null);
  }

  protected Set<MetadataRecord> getMetadata(ApplicationId app, @Nullable MetadataScope scope) throws Exception {
    return metadataClient.getMetadata(app.toId(), scope);
  }

  protected Set<MetadataRecord> getMetadata(ArtifactId artifact, @Nullable MetadataScope scope) throws Exception {
    return metadataClient.getMetadata(artifact.toId(), scope);
  }

  protected Set<MetadataRecord> getMetadata(ProgramId program, @Nullable MetadataScope scope) throws Exception {
    return metadataClient.getMetadata(program.toId(), scope);
  }

  protected Set<MetadataRecord> getMetadata(DatasetId dataset,
                                            @Nullable MetadataScope scope) throws Exception {
    return metadataClient.getMetadata(dataset.toId(), scope);
  }

  protected Set<MetadataRecord> getMetadata(StreamId stream, @Nullable MetadataScope scope) throws Exception {
    return metadataClient.getMetadata(stream.toId(), scope);
  }

  protected Set<MetadataRecord> getMetadata(StreamViewId view, @Nullable MetadataScope scope) throws Exception {
    return metadataClient.getMetadata(view.toId(), scope);
  }

  protected Map<String, String> getProperties(ApplicationId app, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(app, scope).iterator()).getProperties();
  }

  protected Map<String, String> getProperties(ArtifactId artifact, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(artifact, scope).iterator()).getProperties();
  }

  protected Map<String, String> getProperties(ProgramId program, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(program, scope).iterator()).getProperties();
  }

  protected Map<String, String> getProperties(DatasetId dataset, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(dataset, scope).iterator()).getProperties();
  }

  protected Map<String, String> getProperties(StreamId stream, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(stream, scope).iterator()).getProperties();
  }

  protected Map<String, String> getProperties(StreamViewId view, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(view, scope).iterator()).getProperties();
  }

  protected void getPropertiesFromInvalidEntity(ApplicationId app) throws Exception {
    try {
      getProperties(app, MetadataScope.USER);
      Assert.fail("Expected not to be able to get properties from invalid entity: " + app);
    } catch (NotFoundException expected) {
      // expected
    }
  }

  protected void getPropertiesFromInvalidEntity(ProgramId program) throws Exception {
    try {
      getProperties(program, MetadataScope.USER);
      Assert.fail("Expected not to be able to get properties from invalid entity: " + program);
    } catch (NotFoundException expected) {
      // expected
    }
  }

  protected void getPropertiesFromInvalidEntity(DatasetId dataset) throws Exception {
    try {
      getProperties(dataset, MetadataScope.USER);
      Assert.fail("Expected not to be able to get properties from invalid entity: " + dataset);
    } catch (NotFoundException expected) {
      // expected
    }
  }

  protected void getPropertiesFromInvalidEntity(StreamId stream) throws Exception {
    try {
      getProperties(stream, MetadataScope.USER);
      Assert.fail("Expected not to be able to get properties from invalid entity: " + stream);
    } catch (NotFoundException expected) {
      // expected
    }
  }
  protected void getPropertiesFromInvalidEntity(StreamViewId view) throws Exception {
    try {
      getProperties(view, MetadataScope.USER);
      Assert.fail("Expected not to be able to get properties from invalid entity: " + view);
    } catch (NotFoundException expected) {
      // expected
    }
  }

  protected void removeMetadata(ApplicationId app) throws Exception {
    metadataClient.removeMetadata(app.toId());
  }

  protected void removeMetadata(ArtifactId artifact) throws Exception {
    metadataClient.removeMetadata(artifact.toId());
  }

  protected void removeMetadata(ProgramId program) throws Exception {
    metadataClient.removeMetadata(program.toId());
  }

  protected void removeMetadata(DatasetId dataset) throws Exception {
    metadataClient.removeMetadata(dataset.toId());
  }

  protected void removeMetadata(StreamId stream) throws Exception {
    metadataClient.removeMetadata(stream.toId());
  }

  protected void removeMetadata(StreamViewId view) throws Exception {
    metadataClient.removeMetadata(view.toId());
  }

  protected void removeProperties(ApplicationId app) throws Exception {
    metadataClient.removeProperties(app.toId());
  }

  private void removeProperty(ApplicationId app, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(app.toId(), propertyToRemove);
  }

  protected void removeProperties(ArtifactId artifact) throws Exception {
    metadataClient.removeProperties(artifact.toId());
  }

  private void removeProperty(ArtifactId artifact, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(artifact.toId(), propertyToRemove);
  }

  protected void removeProperties(ProgramId program) throws Exception {
    metadataClient.removeProperties(program.toId());
  }

  protected void removeProperty(ProgramId program, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(program.toId(), propertyToRemove);
  }

  protected void removeProperties(DatasetId dataset) throws Exception {
    metadataClient.removeProperties(dataset.toId());
  }

  protected void removeProperty(DatasetId dataset, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(dataset.toId(), propertyToRemove);
  }

  protected void removeProperties(StreamId stream) throws Exception {
    metadataClient.removeProperties(stream.toId());
  }

  protected void removeProperties(StreamViewId view) throws Exception {
    metadataClient.removeProperties(view.toId());
  }

  protected void removeProperty(StreamId stream, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(stream.toId(), propertyToRemove);
  }

  protected void removeProperty(StreamViewId view, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(view.toId(), propertyToRemove);
  }

  protected void addTags(ApplicationId app, @Nullable Set<String> tags) throws Exception {
    metadataClient.addTags(app.toId(), tags);
  }

  protected void addTags(final ApplicationId app, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(app, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addTags(ArtifactId artifact, @Nullable Set<String> tags) throws Exception {
    metadataClient.addTags(artifact.toId(), tags);
  }

  protected void addTags(final ArtifactId artifact, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(artifact, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addTags(ProgramId program, @Nullable Set<String> tags)
    throws Exception {
    metadataClient.addTags(program.toId(), tags);
  }

  protected void addTags(final ProgramId program, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(program, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addTags(DatasetId dataset, @Nullable Set<String> tags) throws Exception {
    metadataClient.addTags(dataset.toId(), tags);
  }

  protected void addTags(final DatasetId dataset, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(dataset, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addTags(StreamId stream, @Nullable Set<String> tags) throws Exception {
    metadataClient.addTags(stream.toId(), tags);
  }

  protected void addTags(StreamViewId view, @Nullable Set<String> tags) throws Exception {
    metadataClient.addTags(view.toId(), tags);
  }

  protected void addTags(final StreamId stream, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(stream, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected void addTags(final StreamViewId view, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) throws IOException {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        addTags(view, tags);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespaceId, String query,
                                                           Set<EntityTypeSimpleName> targets) throws Exception {
    // Note: Can't delegate this to the next method. This is because MetadataHttpHandlerTestRun overrides these two
    // methods, to strip out metadata from search results for easier assertions.
    return metadataClient.searchMetadata(namespaceId.toId(), query, targets).getResults();
  }

  protected Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespaceId, String query,
                                                           Set<EntityTypeSimpleName> targets,
                                                           @Nullable String sort) throws Exception {
    return metadataClient.searchMetadata(namespaceId.toId(), query, targets,
                                         sort, 0, Integer.MAX_VALUE, 0, null, false).getResults();
  }

  protected MetadataSearchResponse searchMetadata(NamespaceId namespaceId, String query,
                                                  Set<EntityTypeSimpleName> targets,
                                                  @Nullable String sort, int offset, int limit, int numCursors,
                                                  @Nullable String cursor, boolean showHiddden) throws Exception {
    return metadataClient.searchMetadata(namespaceId.toId(), query, targets, sort, offset, limit, numCursors,
                                         cursor, showHiddden);
  }

  protected Set<String> getTags(ApplicationId app, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(app, scope).iterator()).getTags();
  }

  protected Set<String> getTags(ArtifactId artifact, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(artifact, scope).iterator()).getTags();
  }

  protected Set<String> getTags(ProgramId program, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(program, scope).iterator()).getTags();
  }

  protected Set<String> getTags(DatasetId dataset, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(dataset, scope).iterator()).getTags();
  }

  protected Set<String> getTags(StreamId stream, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(stream, scope).iterator()).getTags();
  }

  protected Set<String> getTags(StreamViewId view, MetadataScope scope) throws Exception {
    return Iterators.getOnlyElement(getMetadata(view, scope).iterator()).getTags();
  }

  protected void removeTags(ApplicationId app) throws Exception {
    metadataClient.removeTags(app.toId());
  }

  protected void removeTag(ApplicationId app, String tagToRemove) throws Exception {
    metadataClient.removeTag(app.toId(), tagToRemove);
  }

  protected void removeTags(ArtifactId artifact) throws Exception {
    metadataClient.removeTags(artifact.toId());
  }

  protected void removeTag(ArtifactId artifact, String tagToRemove) throws Exception {
    metadataClient.removeTag(artifact.toId(), tagToRemove);
  }

  protected void removeTags(ProgramId program) throws Exception {
    metadataClient.removeTags(program.toId());
  }

  private void removeTag(ProgramId program, String tagToRemove) throws Exception {
    metadataClient.removeTag(program.toId(), tagToRemove);
  }

  protected void removeTags(DatasetId dataset) throws Exception {
    metadataClient.removeTags(dataset.toId());
  }

  protected void removeTag(DatasetId dataset, String tagToRemove) throws Exception {
    metadataClient.removeTag(dataset.toId(), tagToRemove);
  }

  protected void removeTags(StreamId stream) throws Exception {
    metadataClient.removeTags(stream.toId());
  }

  protected void removeTags(StreamViewId view) throws Exception {
    metadataClient.removeTags(view.toId());
  }

  protected void removeTag(StreamId stream, String tagToRemove) throws Exception {
    metadataClient.removeTag(stream.toId(), tagToRemove);
  }

  protected void removeTag(StreamViewId view, String tagToRemove) throws Exception {
    metadataClient.removeTag(view.toId(), tagToRemove);
  }

  // expect an exception during fetching of lineage
  protected void fetchLineage(DatasetId datasetInstance, long start, long end, int levels,
                              Class<? extends Exception> expectedExceptionClass) throws Exception {
    fetchLineage(datasetInstance, Long.toString(start), Long.toString(end), levels, expectedExceptionClass);
  }

  // expect an exception during fetching of lineage
  protected void fetchLineage(final DatasetId datasetInstance, final String start, final String end,
                              final int levels, Class<? extends Exception> expectedExceptionClass) throws Exception {
    expectException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        fetchLineage(datasetInstance, start, end, levels);
        return null;
      }
    }, expectedExceptionClass);
  }

  protected LineageRecord fetchLineage(DatasetId datasetInstance, long start, long end,
                                       int levels) throws Exception {
    return lineageClient.getLineage(datasetInstance, start, end, levels);
  }

  protected LineageRecord fetchLineage(DatasetId datasetInstance, long start, long end,
                                       Set<CollapseType> collapseTypes, int levels) throws Exception {
    return lineageClient.getLineage(datasetInstance, start, end, collapseTypes, levels);
  }

  protected LineageRecord fetchLineage(DatasetId datasetInstance, String start, String end,
                                       int levels) throws Exception {
    return lineageClient.getLineage(datasetInstance, start, end, levels);
  }

  protected LineageRecord fetchLineage(StreamId stream, long start, long end, int levels) throws Exception {
    return lineageClient.getLineage(stream, start, end, levels);
  }

  protected LineageRecord fetchLineage(StreamId stream, String start, String end, int levels) throws Exception {
    return lineageClient.getLineage(stream, start, end, levels);
  }

  protected LineageRecord fetchLineage(StreamId stream, long start, long end, Set<CollapseType> collapseTypes,
                                       int levels) throws Exception {
    return lineageClient.getLineage(stream, start, end, collapseTypes, levels);
  }

  protected Set<MetadataRecord> fetchRunMetadata(ProgramRunId run) throws Exception {
    return metadataClient.getMetadata(run.toId());
  }

  protected void assertRunMetadataNotFound(ProgramRunId run) throws Exception {
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
