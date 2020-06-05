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

import com.google.common.collect.Range;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.squareup.okhttp.Call;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.util.Watch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Abstract class to help implementing {@link Runnable} that watch for resource changes in K8s.
 *
 * @param <T> Resource type to watch
 */
public abstract class AbstractWatcherThread<T> extends Thread implements AutoCloseable, ResourceChangeListener<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractWatcherThread.class);

  private static final Range<Integer> FAILURE_RETRY_RANGE = Range.closedOpen(100, 2000);

  // The response type from K8s when a resource was added
  private static final String ADDED = "ADDED";
  // The response type from K8s when a resource was deleted
  private static final String DELETED = "DELETED";
  // The response type from K8s when a resource was modified
  private static final String MODIFIED = "MODIFIED";

  private final String namespace;
  private final Random random;
  private final Type resourceType;
  private final Type watchResponseType;
  private volatile Watch<T> watch;
  private volatile boolean stopped;

  protected AbstractWatcherThread(String threadName, String namespace) {
    super(threadName);
    this.namespace = namespace;
    this.random = new Random();

    // Resolve <T> to form the concrete type for Watch.Response<T>
    this.resourceType = TypeToken.of(getClass()).resolveType(
      AbstractWatcherThread.class.getTypeParameters()[0]).getType();

    @SuppressWarnings("unchecked")
    TypeToken<T> resourceTypeToken = (TypeToken<T>) TypeToken.of(resourceType);
    this.watchResponseType = new TypeToken<Watch.Response<T>>() { }
      .where(new TypeParameter<T>() { }, resourceTypeToken).getType();
  }

  /**
   * Returns a {@link Call} object for the watcher to use.
   *
   * @param namespace namespace for the call to operate on
   * @param labelSelector the label selector to use for selecting resource to watch
   *                      or {@code null} if not to use selector
   * @return a {@link Call} for {@link Watch} to use
   */
  protected abstract Call createCall(String namespace, @Nullable String labelSelector) throws IOException, ApiException;

  /**
   * Returns a {@link ApiClient} for the watcher to use.
   * @return a {@link ApiClient}. It must be set with appropriate timeout in order for the watch to work effectively.
   */
  protected abstract ApiClient getApiClient() throws IOException;

  /**
   * Returns a selector string for filtering on resources to watch.
   *
   * @return the selector or {@code null} if not to use selector.
   */
  @Nullable
  protected String getSelector() {
    return null;
  }

  /**
   * Reset the existing watch. Children class should call this method where there is a change on the selector
   * returned by the {@link #getSelector()} method.
   */
  protected final void resetWatch() {
    Watch<T> watch;
    synchronized (this) {
      watch = this.watch;
      this.watch = null;
    }

    if (watch != null) {
      try {
        watch.close();
      } catch (Exception e) {
        LOG.trace("Exception raised when closing watch", e);
      }
    }
  }

  @Override
  public final void run() {
    LOG.info("Start watching for changes in kubernetes resource type {}", resourceType);

    int failureCount = 0;
    while (!stopped) {
      try {
        Watch<T> watch = getWatch();

        // The watch can only be null if the watcher thread is stopping
        if (watch == null) {
          break;
        }

        // If we can create the watch, reset the failure count.
        failureCount = 0;

        // The hasNext() will block until there are new data or watch is closed
        while (!stopped && watch.hasNext()) {
          Watch.Response<T> response = watch.next();

          switch (response.type) {
            case ADDED:
              resourceAdded(response.object);
              break;
            case MODIFIED:
              resourceModified(response.object);
              break;
            case DELETED: {
              resourceDeleted(response.object);
              break;
            }
            default:
              LOG.trace("Ignore watch type {}", response.type);
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
          LOG.trace("Exception raised when watching for changes in resource of type {}", resourceType, e);
        } else {
          LOG.warn("Exception raised when watching for changes in resource of type {}", resourceType, e);
        }

        // Clear watch so that a new only will be created in next iteration.
        resetWatch();

        try {
          // If not stopped and failed more than once, sleep for some random second.
          // We only sleep when fail more than one time because exception could be thrown when a new service
          // is being added, the watch would get closed, hence throwing exception.
          if (!stopped && failureCount++ > 0) {
            // Sleep for some random milliseconds before retrying
            int sleepMs = random.nextInt(FAILURE_RETRY_RANGE.upperEndpoint()) + FAILURE_RETRY_RANGE.lowerEndpoint();
            TimeUnit.MILLISECONDS.sleep(sleepMs);
          }
        } catch (InterruptedException ex) {
          // Can only happen on stopping
          break;
        }
      }
    }
    LOG.info("Stop watching for changes in kubernetes resource type {}", resourceType);
  }

  @Override
  public final void close() {
    LOG.debug("Stop watching for kubernetes of resource type {}", resourceType);
    stopped = true;
    resetWatch();
  }

  /**
   * Gets a {@link Watch} for watching for service resources change. This method should only be called
   * from the {@link #run()} method.
   *
   * @return a {@link Watch} or {@code null} if the watching thread is already terminated.
   */
  @Nullable
  private Watch<T> getWatch() throws IOException, ApiException {
    Watch<T> watch = this.watch;
    if (watch != null) {
      return watch;
    }

    synchronized (this) {
      // Need to check here for the case where the thread that calls the close() method set the flag to true,
      // then this thread grab the lock and update the watch field.
      if (stopped) {
        return null;
      }

      // There is only single thread (the run thread) that will call this method,
      // hence if the watch was null outside of this sync block, it will stay as null here.
      String labelSelector = getSelector();
      LOG.trace("Creating watch with label selector {}", labelSelector);
      Call call = createCall(namespace, labelSelector);

      this.watch = watch = Watch.createWatch(getApiClient(), call, watchResponseType);
      return watch;
    }
  }
}
