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

import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.k8s.common.ResourceChangeListener;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.util.generic.options.ListOptions;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.twill.common.Cancellable;

/**
 * A thread for monitoring Kubernetes application resource state change.
 *
 * @param <T> type of Kubernetes resource for which state changes to be monitored
 */
abstract class AppResourceWatcherThread<T extends KubernetesObject> extends
    AbstractWatcherThread<T> {

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1Deployment} events.
   */
  static AppResourceWatcherThread<V1Deployment> createDeploymentWatcher(String namespace,
      String selector,
      ApiClientFactory apiClientFactory) {
    return new AppResourceWatcherThread<V1Deployment>("apps", "v1", "deployments",
        namespace, selector, apiClientFactory) {
    };
  }

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1StatefulSet} events.
   */
  static AppResourceWatcherThread<V1StatefulSet> createStatefulSetWatcher(String namespace,
      String selector,
      ApiClientFactory apiClientFactory) {
    return new AppResourceWatcherThread<V1StatefulSet>("apps", "v1", "statefulsets",
        namespace, selector, apiClientFactory) {
    };
  }

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1Job} events.
   */
  static AppResourceWatcherThread<V1Job> createJobWatcher(String namespace, String selector,
      ApiClientFactory apiClientFactory) {
    return new AppResourceWatcherThread<V1Job>("batch", "v1", "jobs",
        namespace, selector, apiClientFactory) {
    };
  }

  private final String selector;
  private final Queue<ResourceChangeListener<T>> listeners;

  private AppResourceWatcherThread(String group, String version, String plural, String namespace,
      String selector,
      ApiClientFactory apiClientFactory) {
    super("kube-" + plural + "-watch", namespace, group, version, plural, apiClientFactory);
    setDaemon(true);
    this.selector = selector;
    this.listeners = new ConcurrentLinkedQueue<>();
  }

  Cancellable addListener(ResourceChangeListener<T> listener) {
    // Wrap the listener for removal
    ResourceChangeListener<T> wrappedListener = wrapListener(listener);
    listeners.add(wrappedListener);
    closeWatch();
    return () -> listeners.remove(wrappedListener);
  }


  @Override
  protected void updateListOptions(ListOptions options) {
    if (selector != null) {
      options.setLabelSelector(selector);
    }
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
