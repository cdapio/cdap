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

package io.cdap.cdap.ai.ext.vertexai;

import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.Content;
import com.google.cloud.vertexai.api.GenerateContentRequest;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.api.Part;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import com.google.cloud.vertexai.generativeai.ResponseHandler;
import com.google.gson.Gson;
import io.cdap.cdap.ai.spi.AIProvider;
import io.cdap.cdap.ai.spi.AIProviderContext;
import io.cdap.cdap.proto.ApplicationDetail;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the AIService interface for interacting with Vertex AI services.
 */
public class VertexAIProvider implements AIProvider {

  private static final Gson GSON = new Gson();

  private VertexAIConfiguration conf;

  private GenerativeModel model;

  public VertexAIProvider() {
  }

  @Override
  public String getName() {
    return "gcp-vertexai";
  }

  @Override
  public void initialize(AIProviderContext context) throws Exception {
    this.conf = new VertexAIConfiguration(context.getProperties());
    VertexAI vertexAI = new VertexAI.Builder()
        .setProjectId(conf.getProjectId())
        .setLocation(conf.getLocation())
        .setCredentials(ComputeEngineCredentials.getOrCreate(null))
        .build();
    this.model = new GenerativeModel(conf.getModelName(), vertexAI);
  }

  private Content createContent(String text) {
    return Content.newBuilder()
        .addParts(Part.newBuilder().setText(text).build())
        .setRole("USER")
        .build();
  }

  /**
   * Summarizes the application details in the specified format using Vertex AI.
   *
   * @param applicationDetail Details of the application to be summarized.
   * @param format The format in which the summary should be provided.
   * @return This method will be returning a string which is summary in required format.
   */
  @Override
  public String summarizeApp(ApplicationDetail applicationDetail, String format) {
    try {
      String pipelineDetail = GSON.toJson(applicationDetail);
      String prompt = conf.getPrompt().getPipelineMarkdownSummary();
      String payload = pipelineDetail + "\n" + prompt;
      GenerateContentResponse response = model.generateContent(createContent(payload));
      return ResponseHandler.getText(response);
    } catch (IOException ex) {
      ex.printStackTrace();
      return null;
    }
  }
}
