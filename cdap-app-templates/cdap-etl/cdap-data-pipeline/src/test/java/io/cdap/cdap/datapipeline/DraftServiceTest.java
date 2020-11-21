/*
 * Copyright © 2020 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.datapipeline.draft.Draft;
import io.cdap.cdap.datapipeline.draft.DraftId;
import io.cdap.cdap.datapipeline.draft.DraftNotFoundException;
import io.cdap.cdap.datapipeline.draft.DraftStoreRequest;
import io.cdap.cdap.datapipeline.service.StudioUtil;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DraftServiceTest extends DataPipelineServiceTest {

  private static final Gson GSON = new GsonBuilder()
      .setPrettyPrinting()
      .registerTypeAdapter(Draft.class, new DraftTypeAdapter())
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();

  private enum DraftType {
    Batch,
    Streaming
  }

  @Test
  public void testGetDraftError() throws IOException {
    //Attempt to fetch an invalid draft from a valid namespace
    NamespaceSummary namespace = new NamespaceSummary(NamespaceId.DEFAULT.getNamespace(), "", 0);
    DraftId invalidId = new DraftId(namespace, "non-existent", "");
    HttpResponse response = fetchDraft(invalidId);
    DraftNotFoundException draftError = new DraftNotFoundException(invalidId);
    Assert.assertEquals(404, response.getResponseCode());
    Assert.assertEquals(draftError.getMessage(), response.getResponseBodyAsString());

    //Attempt to fetch a valid draft but invalid namespace
    DraftId draftId = new DraftId(namespace, "test-draft", "");
    createBatchPipelineDraft(draftId, "TestPipeline1", "This is a test pipeline.");
    NamespaceSummary invalidNamespace = new NamespaceSummary("non-existent", "", 0);
    response = fetchDraft(new DraftId(invalidNamespace, "test-draft", ""));
    Assert.assertEquals(500, response.getResponseCode());

    // Sanity check, get the draft we just created
    getDraft(draftId);

    //Clean up
    deleteDraftAndCheck(draftId);
  }

  @Test
  public void testDeleteDraftError() throws IOException {
    //Attempt to delete a draft that does not exist
    NamespaceSummary namespace = new NamespaceSummary(NamespaceId.DEFAULT.getNamespace(), "", 0);
    HttpResponse response = deleteDraft(new DraftId(namespace, "non-existent", ""));
    Assert.assertEquals(404, response.getResponseCode());
  }

  @Test
  public void testCreateAndDeleteBatchDraft()
      throws Exception {
    testCreateAndDeleteDraft(DraftType.Batch);
  }

  @Test
  public void testCreateAndDeleteStreamingDraft()
      throws Exception {
    testCreateAndDeleteDraft(DraftType.Streaming);
  }

  private void testCreateAndDeleteDraft(
      DraftType draftType) throws IOException, TimeoutException, InterruptedException, ExecutionException {
    NamespaceSummary namespace = new NamespaceSummary(NamespaceId.DEFAULT.getNamespace(), "", 0);

    DraftId draftId = new DraftId(namespace, "test-1", "admin");
    Draft expectedDraft;
    if (draftType == DraftType.Batch) {
      expectedDraft = createBatchPipelineDraft(draftId, "TestPipeline1",
          "This is a test pipeline.");
    } else {
      expectedDraft = createStreamingPipelineDraft(draftId, "TestPipeline1",
          "This is a test pipeline.");
    }

    Draft fetchedDraft = getDraft(draftId);
    Assert.assertTrue(sameDraft(fetchedDraft, expectedDraft));
    validateMetric(1, Constants.Metrics.DRAFT_COUNT);

    deleteDraftAndCheck(draftId);
    validateMetric(0, Constants.Metrics.DRAFT_COUNT);
  }

  @Test
  public void testListBatchDraftFilter()
      throws Exception {
    testListDraftFilter(DraftType.Batch);
  }

  @Test
  public void testListStreamingDraftFilter()
      throws Exception {
    testListDraftFilter(DraftType.Streaming);
  }

  private void testListDraftFilter(DraftType draftType)
    throws IOException, TimeoutException, InterruptedException, ExecutionException {
    //Create 2 drafts per namespace, each belonging to a different user
    NamespaceSummary namespace = new NamespaceSummary(NamespaceId.DEFAULT.getNamespace(), "", 0);
    String user = "";

    List<DraftId> draftsToCleanUp = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      String id = String.format("draft-%d", i);
      String name = String.format("draft-name-%d", i);
      DraftId draftId = new DraftId(namespace, id, user);
      draftsToCleanUp.add(draftId);
      if (draftType == DraftType.Batch) {
        createBatchPipelineDraft(draftId, name, "This is a test pipeline.");
      } else {
        createStreamingPipelineDraft(draftId, name, "This is a test pipeline.");
      }
    }

    // Test getting drafts for the default namespace
    List<Draft> drafts = listDrafts(NamespaceId.DEFAULT.getNamespace(), false, "", "", "");
    Assert.assertEquals(4, drafts.size());
    drafts.forEach(draft -> Assert.assertNull(draft.getConfig()));
    validateMetric(4, Constants.Metrics.DRAFT_COUNT);

    //Check that the default sorting is correct
    String[] expectedOrder = new String[]{"draft-name-0", "draft-name-1", "draft-name-2",
        "draft-name-3"};
    Assert.assertArrayEquals(drafts.stream().map(DraftStoreRequest::getName).toArray(), expectedOrder);

    // Test getting drafts for the default namespace and sorting on id column with descending order
    drafts = listDrafts(NamespaceId.DEFAULT.getNamespace(), false, "id", "DESC", "");
    Assert.assertEquals(4, drafts.size());
    String[] expectedReverseOrder = new String[]{"draft-3", "draft-2", "draft-1", "draft-0"};
    Assert.assertArrayEquals(drafts.stream().map(Draft::getId).toArray(), expectedReverseOrder);

    // Test filtering on draft name using a filter with 0 matches
    drafts = listDrafts(NamespaceId.DEFAULT.getNamespace(), false, "", "", "nomatches");
    Assert.assertEquals(drafts.size(), 0);

    // Test filtering on draft name using a filter with 1 match and include the config
    Draft specialDraft;
    DraftId specialDraftId = new DraftId(namespace, "newDraft", "");
    draftsToCleanUp.add(specialDraftId);
    if (draftType == DraftType.Batch) {
      specialDraft = createBatchPipelineDraft(specialDraftId, "SpecialDraft",
          "This is a special pipeline.");
    } else {
      specialDraft = createStreamingPipelineDraft(specialDraftId, "SpecialDraft",
          "This is a special pipeline.");
    }

    drafts = listDrafts(NamespaceId.DEFAULT.getNamespace(), true, "", "", "spe");
    Assert.assertEquals(drafts.size(), 1);
    Assert.assertTrue(
        sameDraft(drafts.get(0), specialDraft)); //This confirms that the config was included

    //List with all options enabled
    drafts = listDrafts(NamespaceId.DEFAULT.getNamespace(), true, "id", "DESC", "draft");
    Assert.assertEquals(drafts.size(), 4); //Confirm special draft is not present
    drafts.forEach(
        draft -> Assert.assertNotNull(draft.getConfig())); //Confirm that config was included
    Assert.assertArrayEquals(drafts.stream().map(Draft::getId).toArray(),
        expectedReverseOrder); // Confirm sorting

    //Cleanup
    for (DraftId draftId : draftsToCleanUp) {
      deleteDraftAndCheck(draftId);
    }
  }

  private List<Draft> listDrafts(String namespaceName, boolean includeConfig, String sortBy,
      String sortOrder,
      String filter) throws IOException {
    List<String> queryParams = new ArrayList<>();
    if (includeConfig) {
      queryParams.add("includeConfig=true");
    }
    if (!Strings.isNullOrEmpty(sortBy)) {
      queryParams.add(String.format("sortBy=%s", URLEncoder.encode(sortBy, "UTF-8")));
    }
    if (!Strings.isNullOrEmpty(sortOrder)) {
      queryParams.add(String.format("sortOrder=%s", URLEncoder.encode(sortOrder, "UTF-8")));
    }
    if (!Strings.isNullOrEmpty(filter)) {
      queryParams.add(String.format("filter=%s", URLEncoder.encode(filter, "UTF-8")));
    }

    URL draftURL = serviceURI
        .resolve(String
            .format("v1/contexts/%s/drafts/?%s", namespaceName, String.join("&", queryParams)))
        .toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.GET, draftURL)
        .build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<ArrayList<Draft>>() {
    }.getType());
  }

  private HttpResponse deleteDraft(DraftId draftId) throws IOException {
    URL draftURL = serviceURI
        .resolve(String
            .format("v1/contexts/%s/drafts/%s", draftId.getNamespace().getName(), draftId.getId()))
        .toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.DELETE, draftURL)
        .build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    return response;
  }

  private void deleteDraftAndCheck(DraftId draftId) throws IOException {
    HttpResponse response = deleteDraft(draftId);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private HttpResponse fetchDraft(DraftId draftId) throws IOException {
    URL draftURL = serviceURI
        .resolve(String
            .format("v1/contexts/%s/drafts/%s", draftId.getNamespace().getName(), draftId.getId()))
        .toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.GET, draftURL)
        .build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    return response;
  }

  private Draft getDraft(DraftId draftId) throws IOException {
    HttpResponse response = fetchDraft(draftId);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), Draft.class);
  }

  private Draft createBatchPipelineDraft(DraftId draftId, String name, String description)
      throws IOException {
    ArtifactSummary artifact = new ArtifactSummary("cdap-data-pipeline", "1.0.0");
    ETLBatchConfig config = ETLBatchConfig.builder()
        .addStage(new ETLStage("src", MockSource.getPlugin("dummy1")))
        .addStage(new ETLStage("sink", MockSink.getPlugin("dummy2")))
        .addConnection("src", "sink")
        .setEngine(Engine.SPARK)
        .build();

    DraftStoreRequest<ETLBatchConfig> batchDraftStoreRequest = new DraftStoreRequest<>(config, "", name,
                                                                                       description, 0, artifact);

    long now = System.currentTimeMillis();
    Draft expectedDraft = new Draft(config, name, description, artifact, draftId.getId(), now, now);

    createPipelineDraft(draftId, batchDraftStoreRequest);
    return expectedDraft;
  }

  private Draft createStreamingPipelineDraft(DraftId draftId, String name, String description)
      throws IOException {
    ArtifactSummary artifact = new ArtifactSummary("cdap-data-streams", "1.0.0");
    DataStreamsConfig config = DataStreamsConfig.builder()
        .addStage(new ETLStage("src", MockSource.getPlugin("dummy1")))
        .addStage(new ETLStage("sink", MockSink.getPlugin("dummy2")))
        .addConnection("src", "sink")
        .setCheckpointDir("temp/dir")
        .build();

    DraftStoreRequest<DataStreamsConfig> batchDraftStoreRequest = new DraftStoreRequest<>(config, "", name,
                                                                                          description, 0, artifact);

    long now = System.currentTimeMillis();
    Draft expectedDraft = new Draft(config, name, description, artifact, draftId.getId(), now, now);

    createPipelineDraft(draftId, batchDraftStoreRequest);
    return expectedDraft;
  }

  private void createPipelineDraft(DraftId draftId, DraftStoreRequest draftStoreRequest) throws IOException {
    URL createDraftURL = serviceURI
        .resolve(String
            .format("v1/contexts/%s/drafts/%s", draftId.getNamespace().getName(), draftId.getId()))
        .toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, createDraftURL)
        .withBody(GSON.toJson(draftStoreRequest))
        .build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());
  }

  /**
   * Helper method for checking if two drafts have all the same properties and create/update times
   * that are within 1 second of each other.
   *
   * @param d1 draft to compare
   * @param d2 draft to compare
   * @return True if the drafts have all the same properties and create/update times within 1sec
   */
  private boolean sameDraft(Draft d1, Draft d2) {
    boolean sameCreateTime = Math.abs(d1.getCreatedTimeMillis() - d2.getCreatedTimeMillis()) < 1000;
    boolean sameUpdateTime = Math.abs(d1.getUpdatedTimeMillis() - d2.getUpdatedTimeMillis()) < 1000;
    boolean sameProperties = d1.getRevision() == d2.getRevision() &&
        Objects.equals(d1.getConfig(), d2.getConfig()) &&
        Objects.equals(d1.getPreviousHash(), d2.getPreviousHash()) &&
        Objects.equals(d1.getName(), d2.getName()) &&
        Objects.equals(d1.getDescription(), d2.getDescription()) &&
        Objects.equals(d1.getId(), d2.getId()) &&
        Objects.equals(d1.getArtifact(), d2.getArtifact());

    return sameProperties && sameCreateTime && sameUpdateTime;
  }

  private void validateMetric(long expected, String metric)
    throws TimeoutException, InterruptedException, ExecutionException {

    getMetricsManager()
        .waitForExactMetricCount(Collections.EMPTY_MAP, "user." + metric, expected, 20,
            TimeUnit.SECONDS);
    Assert.assertEquals(expected,
        getMetricsManager().getTotalMetric(Collections.EMPTY_MAP, "user." + metric));
  }

  /**
   * Type adapter that is only needed for unit tests to deserialize drafts returned by REST API
   */
  private static class DraftTypeAdapter implements JsonDeserializer<Draft> {

    @Override
    public Draft deserialize(JsonElement jsonElement, Type type,
        JsonDeserializationContext context) throws JsonParseException {
      Gson gson = new GsonBuilder().create();
      Draft draft = gson.fromJson(jsonElement, Draft.class);
      ETLConfig config;
      if (StudioUtil.isBatchPipeline(draft.getArtifact())) {
        config = context
            .deserialize(jsonElement.getAsJsonObject().get("config"), ETLBatchConfig.class);
      } else if (StudioUtil.isStreamingPipeline(draft.getArtifact())) {
        config = context
            .deserialize(jsonElement.getAsJsonObject().get("config"), DataStreamsConfig.class);
      } else {
        throw new IllegalArgumentException(String
            .format(
                "Artifact name '%s' is not supported. Valid options are %s or %s",
                draft.getArtifact().getName(),
                StudioUtil.ARTIFACT_BATCH_NAME,
                StudioUtil.ARTIFACT_STREAMING_NAME));
      }
      return new Draft(config, draft.getName(), draft.getDescription(), draft.getArtifact(),
          draft.getId(),          draft.getCreatedTimeMillis(), draft.getUpdatedTimeMillis());
    }
  }
}
