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

import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.HttpClient;
import org.apache.http.util.EntityUtils;

/**
 * HTTP client for interacting with Vertex AI services.
 */
public class VertexAIHttpClient {

  private HttpClient httpClient;
  private String vertexAIEndpoint;

  public VertexAIHttpClient(String requestURI) {
    this.vertexAIEndpoint = requestURI;
    httpClient = HttpClients.createDefault();
  }

  /**
   * Generates content based on the provided payload. Currently, this method is not implemented and
   * will return a null string.
   *
   * @param payload The payload to be used for generating content.
   * @return This method is not implemented yet and currently returning null string.
   */
  public String generateContent(String payload) throws IOException {
    HttpPost httpPost = new HttpPost(vertexAIEndpoint);
    StringEntity entity = new StringEntity(payload, ContentType.APPLICATION_JSON);
    httpPost.setEntity(entity);
    HttpResponse response = httpClient.execute(httpPost);
    HttpEntity responseEntity = response.getEntity();
    String responseBody = EntityUtils.toString(responseEntity);
    return responseBody;
  }
}
