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

import io.cdap.cdap.common.conf.CConfiguration;

/**
 * Configuration class that retrieves Vertex AI-specific configuration parameters from the given
 * {@link CConfiguration}.
 */
public class VertexAIConfiguration {

  public static final String PROJECT_ID = "ai.vertexai.project.id";
  public static final String LOCATION_NAME = "ai.vertexai.location";
  public static final String MODEL_NAME = "ai.vertexai.model";
  private final String projectId;
  private final String location;
  private final String modelName;
  private final Prompts prompts;


  /**
   * Constructs a {@link VertexAIConfiguration} instance using the provided {@link CConfiguration}.
   * Initializes project ID, location, model name, and prompts.
   *
   * @param configuration The {@link CConfiguration} containing Vertex AI configuration
   *     parameters.
   */
  public VertexAIConfiguration(CConfiguration configuration) {
    this.projectId = configuration.get(PROJECT_ID);
    this.location = configuration.get(LOCATION_NAME);
    this.modelName = configuration.get(MODEL_NAME);
    this.prompts = new Prompts(configuration);
  }

  /**
   * @return The project name configured for Vertex AI.
   */
  public String getProjectId() {
    return this.projectId;
  }

  /**
   * @return The location configured for Vertex AI.
   */
  public String getLocation() {
    return this.location;
  }

  /**
   * @return The model name configured for Vertex AI.
   */
  public String getModelName() {
    return this.modelName;
  }

  /**
   * @return The prompts configuration for Vertex AI, including the prompt to summarize the app and
   *     give summary in markdown.
   */
  public Prompts getPrompt() {
    return this.prompts;
  }

  /**
   * Inner class representing Vertex AI prompts configuration.
   */
  public class Prompts {

    public static final String PIPELINE_MARKDOWN_SUMMARY = "ai.vertexai.prompts.markdown.pipeline.summary";
    private final String pipelineMarkdownSummary;

    /**
     * Constructs a {@link Prompts} instance using the provided {@link CConfiguration}. Initializes
     * the pipeline Markdown summary prompt.
     *
     * @param conf The {@link CConfiguration} containing prompt configuration.
     */
    public Prompts(CConfiguration conf) {
      this.pipelineMarkdownSummary = conf.get(PIPELINE_MARKDOWN_SUMMARY);
    }

    /**
     * @return The pipeline Markdown summary prompt configured for Vertex AI.
     */
    public String getPipelineMarkdownSummary() {
      return this.pipelineMarkdownSummary;
    }
  }

}

