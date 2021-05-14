/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.provision;

import com.google.gson.Gson;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Fecth and refresh provisioner token periodically.
 */
public class TaskWorkerTokenFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerTokenFetcher.class);
  private static final Gson GSON = new Gson();

  /**
   * Maximum delay that may potentially incur between the time a token is created at a worker pod
   * and the time it's being processed in provisioning service inside app fabric.
   */
  private static final long MAX_DELAY = 30;

  private final CConfiguration cConf;
  private final RemoteClient remoteClient;
  private final ScheduledExecutorService scheduler;

  public TaskWorkerTokenFetcher(CConfiguration cConf, RemoteClient remoteClient) {
    this.cConf = cConf;
    this.remoteClient = remoteClient;
    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  /**
   * Fetches a token from task worker, and put it in the provided map.
   * Also, periodically refreshes the token as long as the map has not been GC'ed.
   *
   * @param map the map for setting the token
   */
  public void submit(Map<String, String> map) {
    if (!cConf.getBoolean(Constants.TaskWorker.POOL_ENABLE) ||
      !cConf.getBoolean(Constants.ProvisioningService.TASK_WORKER_TOKEN_ENABLE)) {
      return;
    }

    setToken(new WeakReference<>(map));
  }

  private void setToken(WeakReference<Map<String, String>> map) {
    Map<String, String> properties = map.get();
    if (properties == null) {
      return;
    }

    try {
      HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(
        HttpMethod.GET, String.format("/worker/token"));
      HttpResponse response = remoteClient.execute(requestBuilder.build());

      if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
        LOG.error("error fetching token " + response.getResponseBodyAsString());
        return;
      }

      properties.put(Constants.ProvisioningService.ACCESS_TOKEN, response.getResponseBodyAsString());

      Map token = GSON.fromJson(response.getResponseBodyAsString(), Map.class);
      Double expiration = (Double) token.get("expires_in");
      scheduler.schedule(() -> setToken(map), expiration.longValue() - MAX_DELAY, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("error fetching token" + e);
    }
  }

}
