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

package io.cdap.cdap.k8s.runtime;

import com.squareup.okhttp.Call;
import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.k8s.common.ResourceChangeListener;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.util.Config;
import org.apache.twill.common.Cancellable;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A thread for monitoring statefulset state change.
 */
public final class StatefulSetWatcher extends AbstractWatcherThread<V1StatefulSet> {

  private final String selector;
  private final Queue<ResourceChangeListener<V1StatefulSet>> listeners;
  private volatile AppsV1Api appsApi;

  public StatefulSetWatcher(String namespace, String selector) {
    super("kube-run-watcher", namespace);
    setDaemon(true);
    this.selector = selector;
    this.listeners = new ConcurrentLinkedQueue<>();
  }

  public Cancellable addListener(ResourceChangeListener<V1StatefulSet> listener) {
    // Wrap the listener for removal
    ResourceChangeListener<V1StatefulSet> wrappedListener = wrapListener(listener);
    listeners.add(wrappedListener);
    resetWatch();
    return () -> listeners.remove(wrappedListener);
  }

  @Nullable
  @Override
  protected String getSelector() {
    return selector;
  }

  @Override
  public void resourceAdded(V1StatefulSet statefulset) {
    listeners.forEach(l -> l.resourceAdded(statefulset));
  }

  @Override
  public void resourceModified(V1StatefulSet statefulset) {
    listeners.forEach(l -> l.resourceModified(statefulset));
  }

  @Override
  public void resourceDeleted(V1StatefulSet statefulset) {
    listeners.forEach(l -> l.resourceDeleted(statefulset));
  }

  @Override
  protected Call createCall(String namespace, @Nullable String labelSelector) throws IOException, ApiException {
    return getAppsApi().listNamespacedStatefulSetCall(namespace, null, null, null, labelSelector,
                                                      null, null, null, true, null, null);
  }

  @Override
  protected ApiClient getApiClient() throws IOException {
    return getAppsApi().getApiClient();
  }

  /**
   * Returns a {@link AppsV1Api} instance for interacting with the API server.
   *
   * @throws IOException if exception was raised during creation of {@link AppsV1Api}
   */
  private AppsV1Api getAppsApi() throws IOException {
    AppsV1Api api = appsApi;
    if (api != null) {
      return api;
    }

    synchronized (this) {
      api = appsApi;
      if (api != null) {
        return api;
      }

      ApiClient client = Config.defaultClient();

      // Set a reasonable timeout for the watch.
      client.getHttpClient().setReadTimeout(5, TimeUnit.MINUTES);

      appsApi = api = new AppsV1Api(client);
      return api;
    }
  }

  private ResourceChangeListener<V1StatefulSet> wrapListener(ResourceChangeListener<V1StatefulSet> listener) {
    return new ResourceChangeListener<V1StatefulSet>() {

      @Override
      public void resourceAdded(V1StatefulSet resource) {
        listener.resourceAdded(resource);
      }

      @Override
      public void resourceModified(V1StatefulSet resource) {
        listener.resourceModified(resource);
      }

      @Override
      public void resourceDeleted(V1StatefulSet resource) {
        listener.resourceDeleted(resource);
      }
    };
  }
}
