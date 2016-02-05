/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.tool.console;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.net.URL;

/**
 * Not putting this in cdap-clients because nobody should know about this.
 *
 * the api works like this:
 *
 * get returns
 * {
 *   "id": "",
 *   "property": {
 *     "adapterDrafts": {
 *       "[name]": {
 *         "artifact": { "name": "cdap-etl-batch", "scope": "SYSTEM", "version": "3.2.1" },
 *         "config": { OldETLBatchConfig },
 *         "description": "",
 *         "ui": { ... }
 *       },
 *       "[name]": {
 *         "artifact": { "name": "cdap-etl-realtime", "scope": "SYSTEM", "version": "3.2.1" },
 *         "config": { OldETLRealtimeConfig },
 *         "description": "",
 *         "ui": { ... }
 *       },
 *       "[name]": {
 *         "artifact": { "name": "cdap-etl-realtime", "scope": "SYSTEM", "version": "3.3.0" },
 *         "config": { ETLRealtimeConfig },
 *         "description": "",
 *         "ui": { ... }
 *       },
 *     },
 *     "pluginTemplates": {
 *       "[namespace]": {
 *         "cdap-etl-batch": {
 *           "batchsource": {
 *             "[templatename]": {
 *               ...
 *             }
 *           }
 *         }
 *       }
 *     },
 *     [whatever else the UI stores]
 *   }
 * }
 * for hydrator upgrade, we don't want to touch [whatever else the UI stores], so {@link #get()} is going to return
 * a JsonObject that the caller will need to check for "adapterDrafts" and "pluginTemplates" keys and decode them into
 * the corresponding Map<String, PipelineDraft> and
 * Map<String, Map<String, Map<String, Map<String, PluginTemplate>>>> objects.
 *
 *
 * set takes as its body, the value of the "property" key from the get call
 */
public class ConsoleClient {
  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  public ConsoleClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  public JsonObject get() throws IOException, UnauthorizedException {
    URL url = config.resolveURLV3("/configuration/user");
    HttpRequest request = HttpRequest.get(url).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, JsonObject.class).getResponseObject();
  }

  public void set(JsonObject val) throws IOException, UnauthorizedException {
    URL url = config.resolveURLV3("/configuration/user");
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(val)).build();

    restClient.execute(request, config.getAccessToken());
  }
}
