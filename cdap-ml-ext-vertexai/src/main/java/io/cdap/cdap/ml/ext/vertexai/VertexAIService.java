/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.ml.ext.vertexai;

import com.google.cloud.aiplatform.v1.Content;
import com.google.cloud.aiplatform.v1.GenerateContentRequest;
import com.google.cloud.aiplatform.v1.Part;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.ml.spi.AIService;
import io.cdap.cdap.proto.ApplicationDetail;
import java.io.IOException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Implementation of the AIService interface for interacting with Vertex AI services.
 */
public class VertexAIService implements AIService {

  private VertexAIConfiguration configuration;
  private VertexAIHttpClient vertexAIHttpClient;

  public VertexAIService(CConfiguration configuration) {
    this.configuration = new VertexAIConfiguration(configuration);
  }

  /**
   * Summarizes the application details in the specified format using Vertex AI. Currently, this
   * method is not implemented and returning a null string.
   *
   * @param applicationDetail Details of the application to be summarized.
   * @param format The format in which the summary should be provided.
   * @return This method currently returning a null string.
   */
  @Override
  public String summarizeApp(ApplicationDetail applicationDetail, String format) {

    Gson gson = new Gson();
    String pipelineDetail = gson.toJson(applicationDetail);
    String projectId = configuration.getProjectId();
    String location = configuration.getLocation();
    String modelName = configuration.getModelName();
    String vertexAIEndpoint = "https://us-west1-aiplatform.googleapis.com/v1/projects/" + projectId
        + "/locations/" + location + "/publishers/google/models/" + modelName + ":generateContent";
    vertexAIHttpClient = new VertexAIHttpClient(vertexAIEndpoint);
    String prompt = pipelineDetail + "\n" + configuration.getPrompt().getPipelineMarkdownSummary();
    GenerateContentRequest request = GenerateContentRequest.newBuilder().addContents(
        Content.newBuilder().addParts(Part.newBuilder().setText(prompt).build()).setRole("USER")
            .build()).build();
    String payload = null;
    try {
      payload = JsonFormat.printer().print(request);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    String responseBody = null;
    try {
      responseBody = new VertexAIHttpClient(vertexAIEndpoint).generateContent(payload);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
    String jsonResponse = jsonObject.toString();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = null;
    try {
      rootNode = mapper.readTree(jsonResponse);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String summary = rootNode.get("candidates").get(0).get("content").get("parts").get(0)
        .get("text").asText();
    return summary;
  }
}

