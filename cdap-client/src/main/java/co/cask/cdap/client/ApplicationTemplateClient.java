/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.template.ApplicationTemplateDetail;
import co.cask.cdap.proto.template.ApplicationTemplateMeta;
import co.cask.cdap.proto.template.PluginMeta;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Application Templates.
 */
public class ApplicationTemplateClient {

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public ApplicationTemplateClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public ApplicationTemplateClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  public List<ApplicationTemplateMeta> list() throws IOException, UnauthorizedException {
    URL url = config.resolveURLV3("templates");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ApplicationTemplateMeta>>() { })
      .getResponseObject();
  }

  public ApplicationTemplateDetail get(String templateId)
    throws NotFoundException, IOException, UnauthorizedException {

    Id.ApplicationTemplate id = Id.ApplicationTemplate.from(templateId);
    URL url = config.resolveURLV3("templates/" + templateId);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(id);
    }
    return ObjectResponse.fromJsonBody(response, ApplicationTemplateDetail.class).getResponseObject();
  }

  public List<PluginMeta> getPlugins(String templateId, String pluginType)
    throws NotFoundException, IOException, UnauthorizedException {

    Id.ApplicationTemplate id = Id.ApplicationTemplate.from(templateId);
    URL url = config.resolveURLV3("templates/" + templateId + "/extensions/" + pluginType);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(id);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<PluginMeta>>() { }).getResponseObject();
  }
}
