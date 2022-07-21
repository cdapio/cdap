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

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.datapipeline.draft.DraftId;
import io.cdap.cdap.datapipeline.draft.DraftService;
import io.cdap.cdap.datapipeline.draft.DraftStoreRequest;
import io.cdap.cdap.datapipeline.draft.SortRequest;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLConfig;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Handler of drafts
 */
//@Path("v1/contexts/{context}")
public class DraftHandler extends AbstractDataPipelineHandler {
  private static final String API_VERSION = "v1";
  private static final Gson GSON = new GsonBuilder()
    .setPrettyPrinting()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  // Injected by CDAP
  @SuppressWarnings("unused")
  private Metrics metrics;

  private DraftService draftService;
  private ContextAccessEnforcer contextAccessEnforcer;

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    contextAccessEnforcer = context.getContextAccessEnforcer();
    this.draftService = new DraftService(context, this.metrics);
  }

  /**
   * Returns a list of drafts associated with a namespace.
   */
  @GET
  @Path(API_VERSION + "/contexts/{context}/drafts/")
  public void listDrafts(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespaceName,
                         @QueryParam("includeConfig") @DefaultValue("false") boolean includeConfig,
                         @QueryParam("sortBy") @DefaultValue("name") String sortBy,
                         @QueryParam("sortOrder") @DefaultValue("ASC") String sortOrder,
                         @QueryParam("filter") @Nullable String filter) {

    respond(namespaceName, responder, (namespace) -> {
      if (!draftService.fieldExists(sortBy)) {
        throw new IllegalArgumentException(String.format(
          "Invalid value '%s' for sortBy. This field does not exist in the Drafts table.", sortBy));
      }

      SortRequest sortRequest = new SortRequest(sortBy, sortOrder);
      String userId = "";
      responder.sendJson(draftService.listDrafts(namespace, userId, includeConfig, sortRequest, filter));
    });
  }

  /**
   * Gets the details of a draft
   */
  @GET
  @Path(API_VERSION + "/contexts/{context}/drafts/{draft}/")
  public void getDraft(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespaceName,
                       @PathParam("draft") String draftId) {
    respond(namespaceName, responder, (namespace) -> {
      String userId = "";
      DraftId id = new DraftId(namespace, draftId, userId);
      responder.sendJson(draftService.getDraft(id));
    });
  }

  /**
   * Creates or updates a draft
   */
  @PUT
  @Path(API_VERSION + "/contexts/{context}/drafts/{draft}/")
  public void putDraft(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespaceName,
                       @PathParam("draft") String draftId) {

    contextAccessEnforcer.enforce(new NamespaceId(namespaceName), StandardPermission.UPDATE);
    respond(namespaceName, responder, (namespace) -> {

      String requestStr = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      DraftStoreRequest<ETLConfig> draftStoreRequest = deserializeDraftStoreRequest(requestStr);
      // Expose draft to all users within the same namespace.
      String userId = "";
      DraftId id = new DraftId(namespace, draftId, userId);
      draftService.writeDraft(id, draftStoreRequest);

      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  /**
   * Deletes a draft
   */
  @DELETE
  @Path(API_VERSION + "/contexts/{context}/drafts/{draft}/")
  public void deleteDraft(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespaceName,
                          @PathParam("draft") String draftId) {
    contextAccessEnforcer.enforce(new NamespaceId(namespaceName), StandardPermission.UPDATE);
    respond(namespaceName, responder, (namespace) -> {
      String userId = "";
      DraftId id = new DraftId(namespace, draftId, userId);

      draftService.deleteDraft(id);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  /**
   * Utility method to correct deserialize the config field in the {@link DraftStoreRequest} object
   *
   * @param jsonStr the json string representing the DraftStoreRequest
   * @return {@link DraftStoreRequest} object
   */
  private DraftStoreRequest<ETLConfig> deserializeDraftStoreRequest(String jsonStr) {
    try {
      DraftStoreRequest<ETLConfig> request = GSON
        .fromJson(jsonStr, new TypeToken<DraftStoreRequest<ETLConfig>>() { }.getType());

      if (request.getArtifact() == null) {
        throw new IllegalArgumentException("artifact is null");
      }

      if (StudioUtil.isBatchPipeline(request.getArtifact())) {
        return GSON.fromJson(jsonStr, new TypeToken<DraftStoreRequest<ETLBatchConfig>>() { }.getType());
      }
      if (StudioUtil.isStreamingPipeline(request.getArtifact())) {
        return GSON.fromJson(jsonStr, new TypeToken<DraftStoreRequest<DataStreamsConfig>>() { }.getType());
      }

      throw new IllegalArgumentException(String.format(
        "Invalid config: artifact '%s' is not supported, valid options are: '%s' or '%s'",
        request.getArtifact().getName(), StudioUtil.ARTIFACT_BATCH_NAME, StudioUtil.ARTIFACT_STREAMING_NAME));
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException("Unable to decode request body: " + e.getMessage(), e);
    }
  }
}
