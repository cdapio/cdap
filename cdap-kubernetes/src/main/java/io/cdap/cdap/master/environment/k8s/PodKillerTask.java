/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentTask;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of {@link MasterEnvironmentTask} for periodically killing pods.
 */
final class PodKillerTask implements MasterEnvironmentTask {

  private static final Logger LOG = LoggerFactory.getLogger(PodKillerTask.class);

  private final String namespace;
  private final String podSelector;
  private final long delayMillis;
  private volatile CoreV1Api coreApi;

  PodKillerTask(String namespace, String podSelector, long delayMillis) {
    this.namespace = namespace;
    this.podSelector = podSelector;
    this.delayMillis = delayMillis;
  }

  @Override
  public long run(MasterEnvironmentContext context) {
    try {
      CoreV1Api api = getCoreApi();
      LOG.debug("Terminating pods using selector {}", podSelector);
      api.deleteCollectionNamespacedPod(namespace, null, null, null, podSelector, null, null, null, null);
      LOG.debug("Pods termination completed");
    } catch (IOException e) {
      LOG.warn("IO Exception raised when connecting to Kubernetes API server", e);
    } catch (ApiException e) {
      LOG.warn("API exception raised when trying to delete pods, code=" + e.getCode()
                 + ", body=" + e.getResponseBody(), e);
    }

    return delayMillis;
  }


  /**
   * Returns a {@link CoreV1Api} instance for interacting with the API server.
   *
   * @throws IOException if exception was raised during creation of {@link CoreV1Api}
   */
  private CoreV1Api getCoreApi() throws IOException {
    CoreV1Api api = coreApi;
    if (api != null) {
      return api;
    }

    synchronized (this) {
      api = coreApi;
      if (api != null) {
        return api;
      }
      coreApi = api = new CoreV1Api(Config.defaultClient());
      return api;
    }
  }
}
