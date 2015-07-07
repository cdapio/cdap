/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.Version;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP.
 */
public class MetaClient {

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public MetaClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public MetaClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  public void ping() throws IOException, UnauthorizedException {
    restClient.execute(HttpMethod.GET, config.resolveURLNoVersion("ping"), config.getAccessToken());
  }

  public Version getVersion() throws IOException, UnauthorizedException {
    HttpResponse response = restClient.execute(HttpMethod.GET, config.resolveURL("version"), config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, Version.class).getResponseObject();
  }

  public Map<String, ConfigEntry> getCDAPConfig() throws IOException, UnauthorizedException {
    return getConfig("config/cdap");
  }

  public Map<String, ConfigEntry> getHadoopConfig() throws IOException, UnauthorizedException {
    return getConfig("config/hadoop");
  }

  private Map<String, ConfigEntry> getConfig(String url) throws IOException, UnauthorizedException {
    HttpResponse response = restClient.execute(HttpMethod.GET, config.resolveURL(url), config.getAccessToken());
    List<ConfigEntry> responseObject =
      ObjectResponse.fromJsonBody(response, new TypeToken<List<ConfigEntry>>() { }).getResponseObject();
    Map<String, ConfigEntry> config = Maps.newHashMap();
    for (ConfigEntry configEntry : responseObject) {
      config.put(configEntry.getName(), configEntry);
    }
    return config;
  }
}
