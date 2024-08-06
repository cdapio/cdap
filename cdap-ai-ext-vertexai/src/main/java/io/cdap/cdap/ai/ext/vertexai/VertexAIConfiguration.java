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

import java.util.Map;
import com.google.api.client.util.Preconditions;

/**
 * Configuration for Vertex AI provider extension.
 */
public class VertexAIConfiguration {

  public static final String PROJECT_ID = "project.id";
  public static final String LOCATION_NAME = "location";
  public static final String MODEL_NAME = "model";
  private final String projectId;
  private final String location;
  private final String modelName;
  private final Prompts prompts;

  private VertexAIConfiguration(Map<String, String> properties) {
    Preconditions.checkArgument(properties.containsKey(PROJECT_ID),
            "Missing required property: " + PROJECT_ID);
    Preconditions.checkArgument(properties.containsKey(LOCATION_NAME),
            "Missing required property: " + LOCATION_NAME);
    Preconditions.checkArgument(properties.containsKey(MODEL_NAME),
            "Missing required property: " + MODEL_NAME);
    this.projectId = properties.get(PROJECT_ID);
    this.location = properties.get(LOCATION_NAME);
    this.modelName = properties.get(MODEL_NAME);
    this.prompts = new Prompts(properties);
  }

  /**
   * Constructs a {@link VertexAIConfiguration} instance using the provided properties.
   *
   * @param properties The properties containing Vertex AI configuration parameters.
   * @return VertexAIConfiguration
   */
  public static VertexAIConfiguration create(Map<String, String> properties){
    return new VertexAIConfiguration(properties);
  }
  /**
   * Gets the project ID for Vertex AI service.
   *
   * @return The project ID.
   */
  public String getProjectId() {
    return this.projectId;
  }


  /**
   * Gets the GCP location for Vertex AI service.
   *
   * @return The GCP location.
   */
  public String getLocation() {
    return this.location;
  }

  /**
   * Gets the Vertex AI model name.
   *
   * @return The Vertex AI model name.
   */
  public String getModelName() {
    return this.modelName;
  }

  /**
   * Prompt configuration for Vertex AI provider extension.
   *
   * @return The prompt configuration.
   */
  public Prompts getPrompt() {
    return this.prompts;
  }

  /**
   * Vertex AI prompts configuration.
   */
  public class Prompts {

    public static final String PIPELINE_MARKDOWN_SUMMARY = "prompts.markdown.pipeline.summary";
    private final String pipelineMarkdownSummary;

    /**
     * Constructs a {@link Prompts} instance using the provided properties.
     *
     * @param conf The properties containing prompt configuration.
     */

    public Prompts(Map<String, String> conf) {
      Preconditions.checkArgument(conf.containsKey(PIPELINE_MARKDOWN_SUMMARY),
              "Missing required property: " + PIPELINE_MARKDOWN_SUMMARY);
      this.pipelineMarkdownSummary = conf.get(PIPELINE_MARKDOWN_SUMMARY);
    }

    /**
     * Gets the prompt for summarizing pipeline.
     *
     * @return The pipeline Markdown summary prompt configured for Vertex AI.
     */
    public String getPipelineMarkdownSummary() {
      return this.pipelineMarkdownSummary;
    }
  }
}

