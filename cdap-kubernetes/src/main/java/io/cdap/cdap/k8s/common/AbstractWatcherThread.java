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

package io.cdap.cdap.k8s.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watchable;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesListObject;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.options.ListOptions;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class to help implementing {@link Runnable} that watch for resource changes in K8s.
 *
 * @param <T> Resource type to watch
 */
public abstract class AbstractWatcherThread<T extends KubernetesObject>
    extends Thread implements AutoCloseable, ResourceChangeListener<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractWatcherThread.class);

  private static final Range<Integer> FAILURE_RETRY_RANGE = Range.closedOpen(100, 2000);

  // The response type from K8s when a resource was added
  private static final String ADDED = "ADDED";
  // The response type from K8s when a resource was deleted
  private static final String DELETED = "DELETED";
  // The response type from K8s when a resource was modified
  private static final String MODIFIED = "MODIFIED";
  // The response type from K8s when there is error from the watch
  private static final String ERROR = "ERROR";

  protected final String group;
  protected final String version;
  protected final String plural;

  private final String namespace;
  private final Random random;
  private final Type resourceType;
  private final CachingResourceChangeListener changeListener;
  private final ApiClientFactory apiClientFactory;
  private volatile boolean stopped;
  private volatile ApiClient apiClient;
  private Watchable<DynamicKubernetesObject> watch;

  @VisibleForTesting
  protected AbstractWatcherThread(String threadName, String namespace, String group, String version,
      String plural,
      ApiClientFactory apiClientFactory) {
    super(threadName);
    setDaemon(true);
    this.namespace = namespace;
    this.group = group;
    this.version = version;
    this.plural = plural;
    this.random = new Random();

    // Resolve <T> to form the concrete type for Watch.Response<T>
    this.resourceType = TypeToken.of(getClass()).resolveType(
        AbstractWatcherThread.class.getTypeParameters()[0]).getType();
    this.changeListener = new CachingResourceChangeListener();
    this.apiClientFactory = apiClientFactory;
  }


  /**
   * Updates the given {@link ListOptions} for the resource listing call. Typically, sub-class can
   * set override to set field and label selectors.
   */
  protected void updateListOptions(ListOptions options) {
    // no-op
  }

  /**
   * Close the existing watch. Children class can call this method to force closing the existing
   * watch.
   */
  @VisibleForTesting
  protected final synchronized void closeWatch() {
    try {
      if (watch != null) {
        watch.close();
      }
      watch = null;
    } catch (IOException e) {
      LOG.trace("Exception raised when closing watch", e);
    }
  }

  @Override
  public final void run() {
    LOG.info("Start watching for changes in kubernetes resource type {}", resourceType);

    int failureCount = 0;

    while (!stopped) {
      // Create a new Watchable and start watching for events
      // The watch is supposed to be a never ending iterator which blocks on the hasNext() call.
      // In case if the while loop exited, it must either be due to stopped or internal watcher state changed
      // such that the iterator is no longer connected to the API server.
      // In both cases, we reset the watcher so that it won't get reused.
      try (Watchable<DynamicKubernetesObject> watch = createWatchable()) {
        // If we can create the watch, reset the failure count.
        failureCount = 0;

        // The hasNext() will block until there are new data or watch is closed
        while (!stopped && watch.hasNext()) {
          Watch.Response<DynamicKubernetesObject> response = watch.next();
          switch (response.type) {
            case ADDED:
              changeListener.resourceAdded(decodeResource(response.object));
              break;
            case MODIFIED:
              changeListener.resourceModified(decodeResource(response.object));
              break;
            case DELETED:
              changeListener.resourceDeleted(decodeResource(response.object));
              break;
            case ERROR:
              LOG.warn("Encountered error while watching for '{}/{}/{}' with status {}",
                  group, version, plural, response.status);
              // If the resourceVersion we provided was out-of-date when the watch was created,
              // the server will respond with ERROR. We close the watch to start with a fresh fetch.
              closeWatch();
              break;
            default:
              LOG.warn("Ignore unsupported response type {}", response.type);
          }
        }
      } catch (Exception e) {
        // Ignore the exception if it is during stopping of the thread, which is expected to happen
        if (stopped) {
          break;
        }

        // We just retry on any form of exceptions
        Throwable cause = e.getCause();
        if (cause instanceof IOException || e instanceof IllegalStateException) {
          // Log at lower level if it is caused by IOException or IllegalStateException, which will happen
          // if connection to the API server is lost or the watch is closed
          LOG.trace("Exception raised when watching for changes in resource of type {}",
              resourceType, e);
        } else {
          LOG.warn("Exception raised when watching for changes in resource of type {}",
              resourceType, e);
        }

        try {
          // If not stopped and failed more than once, sleep for some random second.
          // We only sleep when fail more than one time because exception could be thrown when a new service
          // is being added, the watch would get closed, hence throwing exception.
          if (!stopped && failureCount++ > 0) {
            // Sleep for some random milliseconds before retrying
            int sleepMs = random.nextInt(FAILURE_RETRY_RANGE.upperEndpoint())
                + FAILURE_RETRY_RANGE.lowerEndpoint();
            TimeUnit.MILLISECONDS.sleep(sleepMs);
          }
        } catch (InterruptedException ex) {
          // Can only happen on stopping
          break;
        }
      }
    }
    LOG.info("Stopped watching for changes in kubernetes resource type {}", resourceType);
  }

  @Override
  public void close() {
    stopped = true;
    interrupt();
    closeWatch();
  }

  /**
   * Returns a {@link ApiClient} for communicating with the k8s server.
   */
  protected final ApiClient getApiClient() throws IOException {
    ApiClient client = apiClient;
    if (client != null) {
      return client;
    }

    synchronized (this) {
      client = apiClient;
      if (client != null) {
        return client;
      }

      client = apiClientFactory.create();

      // Set a reasonable timeout for the watch.
      client.setReadTimeout((int) TimeUnit.MINUTES.toMillis(5));
      apiClient = client;
      return client;
    }
  }

  /**
   * Creates a new {@link Watchable} for watching for changes.
   *
   * @return a {@link Watchable}
   */
  private Watchable<DynamicKubernetesObject> createWatchable() throws IOException, ApiException {

    ApiClient apiClient = getApiClient();
    DynamicKubernetesApi api = new DynamicKubernetesApi(group, version, plural, apiClient);
    DynamicKubernetesListObject listObject;
    Watchable<DynamicKubernetesObject> watch;

    synchronized (this) {
      ListOptions options = new ListOptions();
      updateListOptions(options);

      KubernetesApiResponse<DynamicKubernetesListObject> listResult = api.list(namespace, options);

      // Throw exception if the list call failed
      listResult.throwsApiException();

      listObject = listResult.getObject();
      String resourceVersion = listObject.getMetadata().getResourceVersion();

      LOG.trace("Start watching '{}/{}/{}' starting at resource version {}", group, version, plural,
          resourceVersion);

      // Create the new watch
      // It is important to use the same ListOptions (for selector) as the one used in the list call above
      options.setResourceVersion(resourceVersion);
      watch = wrapWatchableClose(api.watch(namespace, options));
      this.watch = watch;
    }

    // Notify the listener based on the initial list of resources.
    // This has to be done after setting the watch field such that if there is a close call, the watch will get closed
    // recreated again from the run() method.
    changeListener.updateResources(listObject);
    return watch;
  }

  private T decodeResource(DynamicKubernetesObject object) throws IOException {
    return getApiClient().getJSON().getGson().fromJson(object.getRaw(), resourceType);
  }

  /**
   * Wraps the {@link Watchable} such that the {@link Watchable#close()} method would just log
   * exception.
   */
  private Watchable<DynamicKubernetesObject> wrapWatchableClose(
      Watchable<DynamicKubernetesObject> watchable) {
    return new Watchable<DynamicKubernetesObject>() {
      @Override
      public void close() {
        try {
          watchable.close();
        } catch (Exception e) {
          LOG.trace("Exception raised when closing watch", e);
        }
      }

      @Override
      public Iterator<Watch.Response<DynamicKubernetesObject>> iterator() {
        return watchable.iterator();
      }

      @Override
      public boolean hasNext() {
        return watchable.hasNext();
      }

      @Override
      public Watch.Response<DynamicKubernetesObject> next() {
        return watchable.next();
      }
    };
  }

  /**
   * A {@link ResourceChangeListener} that memorize all resources it received.
   */
  private final class CachingResourceChangeListener implements ResourceChangeListener<T> {

    private final Map<String, T> resources;

    private CachingResourceChangeListener() {
      this.resources = new HashMap<>();
    }

    @Override
    public void resourceAdded(T resource) {
      LOG.trace("Resource added: {} {}", resourceType, resource.getMetadata().getName());
      resources.put(resource.getMetadata().getName(), resource);
      AbstractWatcherThread.this.resourceAdded(resource);
    }

    @Override
    public void resourceModified(T resource) {
      LOG.trace("Resource modified: {} {}", resourceType, resource.getMetadata().getName());
      resources.put(resource.getMetadata().getName(), resource);
      AbstractWatcherThread.this.resourceModified(resource);
    }

    @Override
    public void resourceDeleted(T resource) {
      LOG.trace("Resource deleted: {} {}", resourceType, resource.getMetadata().getName());
      resources.remove(resource.getMetadata().getName());
      AbstractWatcherThread.this.resourceDeleted(resource);
    }

    /**
     * Updates the resources and fire events based on the difference between the cached resources
     * and the given list of resources.
     *
     * @param listObject the list of resources to compare against the cache
     * @throws IOException if failed to decode the resource object
     */
    void updateResources(DynamicKubernetesListObject listObject) throws IOException {
      Set<String> currentSet = new HashSet<>(resources.keySet());

      // Add existing resources
      for (DynamicKubernetesObject obj : listObject.getItems()) {
        String name = obj.getMetadata().getName();
        T resource = decodeResource(obj);

        // If the resource is not known before, emit an add event
        if (!currentSet.remove(name)) {
          resourceAdded(resource);
        } else if (!Objects.equals(resource, resources.get(name))) {
          // Otherwise, if there is changes in the resource, emit a modified event
          resourceModified(resource);
        }
      }

      // Emit deleted event for resources that were deleted
      for (String deleted : currentSet) {
        T resource = resources.get(deleted);
        if (resource != null) {
          resourceDeleted(resource);
        }
      }
    }
  }
}
