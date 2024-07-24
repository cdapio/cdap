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
import io.cdap.cdap.ml.spi.AIService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.common.NotImplementedException;

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
    return null;
  }
}

