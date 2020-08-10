/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.util.Config;
import org.apache.twill.common.Cancellable;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A thread for monitoring Kubernetes application resource state change.
 *
 * @param <T> type of Kubernetes resource for which state changes to be monitored
 */
abstract class AppResourceWatcherThread<T> extends AbstractWatcherThread<T> {

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1Deployment} events.
   */
  static AppResourceWatcherThread<V1Deployment> createDeploymentWatcher(String namespace, String selector) {
    return new AppResourceWatcherThread<V1Deployment>("kube-deployment-watch", namespace, selector) {
      @Override
      protected Call createCall(String namespace, @Nullable String labelSelector) throws IOException, ApiException {
        return getAppsApi().listNamespacedDeploymentCall(namespace, null, null, null, labelSelector,
                                                         null, null, null, true, null, null);
      }
    };
  }

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1StatefulSet} events.
   */
  static AppResourceWatcherThread<V1StatefulSet> createStatefulSetWatcher(String namespace, String selector) {
    return new AppResourceWatcherThread<V1StatefulSet>("kube-statefulset-watch", namespace, selector) {
      @Override
      protected Call createCall(String namespace, @Nullable String labelSelector) throws IOException, ApiException {
        return getAppsApi().listNamespacedStatefulSetCall(namespace, null, null, null, labelSelector,
                                                          null, null, null, true, null, null);
      }
    };
  }

  private final String selector;
  private final Queue<ResourceChangeListener<T>> listeners;
  private volatile AppsV1Api appsApi;

  private AppResourceWatcherThread(String threadName, String namespace, String selector) {
    super(threadName, namespace);
    setDaemon(true);
    this.selector = selector;
    this.listeners = new ConcurrentLinkedQueue<>();
  }

  Cancellable addListener(ResourceChangeListener<T> listener) {
    // Wrap the listener for removal
    ResourceChangeListener<T> wrappedListener = wrapListener(listener);
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
  public void resourceAdded(T resource) {
    listeners.forEach(l -> l.resourceAdded(resource));
  }

  @Override
  public void resourceModified(T resource) {
    listeners.forEach(l -> l.resourceModified(resource));
  }

  @Override
  public void resourceDeleted(T resource) {
    listeners.forEach(l -> l.resourceDeleted(resource));
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
  AppsV1Api getAppsApi() throws IOException {
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

  private ResourceChangeListener<T> wrapListener(ResourceChangeListener<T> listener) {
    return new ResourceChangeListener<T>() {

      @Override
      public void resourceAdded(T resource) {
        listener.resourceAdded(resource);
      }

      @Override
      public void resourceModified(T resource) {
        listener.resourceModified(resource);
      }

      @Override
      public void resourceDeleted(T resource) {
        listener.resourceDeleted(resource);
      }
    };
  }
}
