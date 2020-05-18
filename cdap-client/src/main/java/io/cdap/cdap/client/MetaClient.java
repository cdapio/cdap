/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.ConfigEntry;
import io.cdap.cdap.proto.Version;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides ways to interact with CDAP.
 */
@Beta
public class MetaClient {

  private final RESTClient restClient;
  private final ClientConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(MetaClient.class);

  @Inject
  public MetaClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public MetaClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  public void ping() throws IOException, UnauthenticatedException, UnauthorizedException {
    HttpResponse response = restClient.execute(
      HttpMethod.GET, config.resolveURLNoVersion("ping"), config.getAccessToken());
    if (!Objects.equals(response.getResponseBodyAsString(), "OK.\n")) {
      LOG.info("Jay Pandya ping response " + response.getResponseBodyAsString());
      throw new IOException("Unexpected response body");
    }
  }

  public Version getVersion() throws IOException, UnauthenticatedException, UnauthorizedException {
    HttpResponse response = restClient.execute(HttpMethod.GET, config.resolveURL("version"), config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, Version.class).getResponseObject();
  }

  public Map<String, ConfigEntry> getCDAPConfig() throws IOException, UnauthenticatedException, UnauthorizedException {
    return getConfig("config/cdap");
  }

  public Map<String, ConfigEntry> getHadoopConfig()
    throws IOException, UnauthenticatedException, UnauthorizedException {
    return getConfig("config/hadoop");
  }

  private Map<String, ConfigEntry> getConfig(String url)
    throws IOException, UnauthenticatedException, UnauthorizedException {
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
