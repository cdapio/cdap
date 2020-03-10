/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.common.metadata.MetadataRecord;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import io.cdap.cdap.proto.metadata.lineage.CollapseType;
import io.cdap.cdap.proto.metadata.lineage.FieldLineageSummary;
import io.cdap.cdap.proto.metadata.lineage.LineageRecord;
import org.junit.Assert;
import org.junit.Before;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Base class for metadata tests.
 */
@SuppressWarnings({"WeakerAccess", "SameParameterValue"})
public abstract class MetadataTestBase extends ClientTestBase {

  protected static final NamespaceId TEST_NAMESPACE1 = new NamespaceId("testnamespace1");

  private MetadataClient metadataClient;
  private LineageClient lineageClient;
  protected ArtifactClient artifactClient;
  protected NamespaceClient namespaceClient;
  protected ApplicationClient appClient;
  protected ProgramClient programClient;
  protected DatasetClient datasetClient;

  @Before
  public void beforeTest() {
    metadataClient = new MetadataClient(getClientConfig());
    lineageClient = new LineageClient(getClientConfig());
    artifactClient = new ArtifactClient(getClientConfig());
    namespaceClient = new NamespaceClient(getClientConfig());
    appClient = new ApplicationClient(getClientConfig());
    programClient = new ProgramClient(getClientConfig());
    datasetClient = new DatasetClient(getClientConfig());
  }

  protected void addAppArtifact(ArtifactId artifactId, Class<?> cls) throws Exception {
    artifactClient.add(artifactId, null, () -> Files.newInputStream(createAppJarFile(cls).toPath()));
  }

  protected void addPluginArtifact(ArtifactId artifactId, Class<?> cls, Manifest manifest,
                                   @Nullable Set<ArtifactRange> parents) throws Exception {
    artifactClient.add(artifactId, parents, () -> Files.newInputStream(createArtifactJarFile(cls, manifest).toPath()));
  }

  protected void addProperties(MetadataEntity metadataEntity, @Nullable Map<String, String> properties)
    throws Exception {
    metadataClient.addProperties(metadataEntity, properties);
  }
  protected void addProperties(EntityId entityId, @Nullable Map<String, String> properties) throws Exception {
    addProperties(entityId.toMetadataEntity(), properties);
  }

  protected void addProperties(final EntityId entityId, @Nullable final Map<String, String> properties,
                               Class<? extends Exception> expectedExceptionClass) {
    expectException((Callable<Void>) () -> {
      addProperties(entityId, properties);
      return null;
    }, expectedExceptionClass);
  }

  protected Set<MetadataRecord> getMetadata(MetadataEntity metadataEntity) throws Exception {
    return getMetadata(metadataEntity, null);
  }

  protected Set<MetadataRecord> getMetadata(MetadataEntity metadataEntity, @Nullable MetadataScope scope)
    throws Exception {
    return metadataClient.getMetadata(metadataEntity, scope);
  }

  protected Map<String, String> getProperties(MetadataEntity metadataEntity, MetadataScope scope) throws Exception {
    return metadataClient.getProperties(metadataEntity, scope);
  }

  protected Map<String, String> getProperties(EntityId entityId, MetadataScope scope) throws Exception {
    return getProperties(entityId.toMetadataEntity(), scope);
  }

  protected void removeMetadata(MetadataEntity metadataEntity) throws Exception {
    metadataClient.removeMetadata(metadataEntity);
  }

  protected void removeMetadata(EntityId entityId) throws Exception {
    removeMetadata(entityId.toMetadataEntity());
  }

  protected void removeProperties(MetadataEntity metadataEntity) throws Exception {
    metadataClient.removeProperties(metadataEntity);
  }

  protected void removeProperties(EntityId entityId) throws Exception {
    removeProperties(entityId.toMetadataEntity());
  }

  public void removeProperty(EntityId entityId, String propertyToRemove) throws Exception {
    removeProperty(entityId.toMetadataEntity(), propertyToRemove);
  }

  private void removeProperty(MetadataEntity metadataEntity, String propertyToRemove) throws Exception {
    metadataClient.removeProperty(metadataEntity, propertyToRemove);
  }

  protected void addTags(MetadataEntity metadataEntity, @Nullable Set<String> tags)
    throws Exception {
    metadataClient.addTags(metadataEntity, tags);
  }

  protected void addTags(EntityId entityId, @Nullable Set<String> tags)
    throws Exception {
    addTags(entityId.toMetadataEntity(), tags);
  }

  protected void addTags(final EntityId entityId, @Nullable final Set<String> tags,
                         Class<? extends Exception> expectedExceptionClass) {
    expectException((Callable<Void>) () -> {
      addTags(entityId, tags);
      return null;
    }, expectedExceptionClass);
  }

  protected MetadataSearchResponse searchMetadata(NamespaceId namespaceId, String query,
                                                  Set<String> targets,
                                                  @Nullable String sort, int offset, int limit, int numCursors,
                                                  @Nullable String cursor, boolean showHiddden) throws Exception {
    return searchMetadata(namespaceId == null ? null : ImmutableList.of(namespaceId),
                          query, targets, sort, offset, limit, numCursors, cursor, showHiddden);
  }

  protected MetadataSearchResponse searchMetadata(List<NamespaceId> namespaceIds, String query,
                                                  Set<String> targets,
                                                  @Nullable String sort, int offset, int limit, int numCursors,
                                                  @Nullable String cursor, boolean showHiddden) throws Exception {
    return metadataClient.searchMetadata(namespaceIds, query, targets, sort, offset, limit, numCursors,
                                         cursor, showHiddden);
  }

  protected Set<String> getTags(MetadataEntity metadataEntity, MetadataScope scope) throws Exception {
    return metadataClient.getTags(metadataEntity, scope);
  }
  protected Set<String> getTags(EntityId entityId, MetadataScope scope) throws Exception {
    return getTags(entityId.toMetadataEntity(), scope);
  }

  protected void removeTag(MetadataEntity metadataEntity, String tagToRemove) throws Exception {
    metadataClient.removeTag(metadataEntity, tagToRemove);
  }

  protected void removeTags(MetadataEntity metadataEntity) throws Exception {
    metadataClient.removeTags(metadataEntity);
  }

  protected void removeTag(EntityId entityId, String tagToRemove) throws Exception {
    removeTag(entityId.toMetadataEntity(), tagToRemove);
  }

  protected void removeTags(EntityId entityId) throws Exception {
    removeTags(entityId.toMetadataEntity());
  }

  // expect an exception during fetching of lineage
  protected void fetchLineage(DatasetId datasetInstance, long start, long end, int levels,
                              Class<? extends Exception> expectedExceptionClass) {
    fetchLineage(datasetInstance, Long.toString(start), Long.toString(end), levels, expectedExceptionClass);
  }

  // expect an exception during fetching of lineage
  protected void fetchLineage(final DatasetId datasetInstance, final String start, final String end,
                              final int levels, Class<? extends Exception> expectedExceptionClass) {
    expectException((Callable<Void>) () -> {
      fetchLineage(datasetInstance, start, end, levels);
      return null;
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

  protected FieldLineageSummary fetchFieldLineage(DatasetId datasetId,
                                                  long start, long end, String direction) throws Exception {
    return lineageClient.getFieldLineage(datasetId, start, end, direction);
  }

  // expect an exception during fetching of field lineage
  protected void fetchFieldLineage(DatasetId datasetId, long start, long end, String direction,
                                   Class<? extends Exception> expectedExceptionClass) {
    expectException((Callable<Void>) () -> {
      fetchFieldLineage(datasetId, start, end, direction);
      return null;
    }, expectedExceptionClass);
  }

  private <T> void expectException(Callable<T> callable, Class<? extends Exception> expectedExceptionClass) {
    try {
      callable.call();
      Assert.fail("Expected to have exception of class: " + expectedExceptionClass);
    } catch (Exception e) {
      if (e.getClass() != expectedExceptionClass) {
        Assert.fail(String.format("Expected %s but received %s. %s", expectedExceptionClass.getSimpleName(), e
          .getClass().getSimpleName(), e));
      }
    }
  }
}
