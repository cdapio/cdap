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

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class VertexAIConfigurationTest {

  private static final String TEST_PROJECT_ID = "test-project-id";

  private static final String TEST_LOCATION = "test-location";

  private static final String TEST_MODEL_NAME = "test-model";

  private static final String SUMMARY_PROMPT = "Summary prompt";

  @Test
  public void testVertexAIConfigurationWithAllProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(VertexAIConfiguration.PROJECT_ID, TEST_PROJECT_ID);
    properties.put(VertexAIConfiguration.LOCATION_NAME, TEST_LOCATION);
    properties.put(VertexAIConfiguration.MODEL_NAME, TEST_MODEL_NAME);
    properties.put(VertexAIConfiguration.Prompts.PIPELINE_MARKDOWN_SUMMARY, SUMMARY_PROMPT);

    VertexAIConfiguration config = VertexAIConfiguration.create(properties);

    Assert.assertEquals(TEST_PROJECT_ID, config.getProjectId());
    Assert.assertEquals(TEST_LOCATION, config.getLocation());
    Assert.assertEquals(TEST_MODEL_NAME, config.getModelName());
    Assert.assertEquals(SUMMARY_PROMPT, config.getPrompt().getPipelineMarkdownSummary());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testVertexAIConfigurationWithMissingProjectId() {
    Map<String, String> properties = new HashMap<>();
    properties.put(VertexAIConfiguration.LOCATION_NAME, TEST_LOCATION);
    properties.put(VertexAIConfiguration.MODEL_NAME, TEST_MODEL_NAME);
    properties.put(VertexAIConfiguration.Prompts.PIPELINE_MARKDOWN_SUMMARY, SUMMARY_PROMPT);

    VertexAIConfiguration.create(properties);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testVertexAIConfigurationWithMissingLocation() {
    Map<String, String> properties = new HashMap<>();
    properties.put(VertexAIConfiguration.PROJECT_ID, TEST_PROJECT_ID);
    properties.put(VertexAIConfiguration.MODEL_NAME, TEST_MODEL_NAME);
    properties.put(VertexAIConfiguration.Prompts.PIPELINE_MARKDOWN_SUMMARY, SUMMARY_PROMPT);

    VertexAIConfiguration.create(properties);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testVertexAIConfigurationWithMissingModelName() {
    Map<String, String> properties = new HashMap<>();
    properties.put(VertexAIConfiguration.PROJECT_ID, TEST_PROJECT_ID);
    properties.put(VertexAIConfiguration.LOCATION_NAME, TEST_LOCATION);
    properties.put(VertexAIConfiguration.Prompts.PIPELINE_MARKDOWN_SUMMARY, SUMMARY_PROMPT);

    VertexAIConfiguration.create(properties);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testVertexAIConfigurationWithMissingPrompt() {
    Map<String, String> properties = new HashMap<>();
    properties.put(VertexAIConfiguration.PROJECT_ID, TEST_PROJECT_ID);
    properties.put(VertexAIConfiguration.LOCATION_NAME, TEST_LOCATION);
    properties.put(VertexAIConfiguration.MODEL_NAME, TEST_MODEL_NAME);

    VertexAIConfiguration.create(properties);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testVertexAIConfigurationWithEmptyProperties() {
    Map<String, String> properties = new HashMap<>();
    VertexAIConfiguration.create(properties);
  }
}

