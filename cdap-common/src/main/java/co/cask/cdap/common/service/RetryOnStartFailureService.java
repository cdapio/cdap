/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.common.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A Guava {@link Service} that wrap around another {@link Service} such that, if the wrapped service failed
 * to start, it will get restarted based on the {@link RetryStrategy}.
 */
public class RetryOnStartFailureService extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(RetryOnStartFailureService.class);

  private final Supplier<Service> delegate;
  private final String delegateServiceName;
  private final RetryStrategy retryStrategy;
  private volatile Thread startupThread;
  private volatile Service currentDelegate;
  private volatile Service startedService;
  private volatile boolean stopped = false;

  /**
   * Creates a new instance.
   *
   * @param delegate a {@link Supplier} that gives new instance of the delegating Service.
   * @param retryStrategy strategy to use for retrying
   */
  public RetryOnStartFailureService(Supplier<Service> delegate, RetryStrategy retryStrategy) {
    this.delegate = delegate;
    this.currentDelegate = delegate.get();
    this.delegateServiceName = currentDelegate.getClass().getSimpleName();
    this.retryStrategy = retryStrategy;
  }

  @Override
  protected void doStart() {
    startupThread = new Thread("Endure-Service-" + delegateServiceName) {
      @Override
      public void run() {
        int failures = 0;
        long startTime = System.currentTimeMillis();
        long delay = 0L;

        while (delay >= 0 && !stopped) {
          try {
            currentDelegate.start().get();
            // Only assigned the delegate if and only if the delegate service started successfully
            startedService = currentDelegate;
            break;
          } catch (InterruptedException e) {
            // This thread will be interrupted from the doStop() method. Don't reset the interrupt flag.
          } catch (Throwable t) {
            LOG.debug("Exception raised when starting service {}", delegateServiceName, t);

            delay = retryStrategy.nextRetry(++failures, startTime);
            if (delay < 0) {
              LOG.error("Failed to start service {} after {} retries in {}ms",
                        delegateServiceName, failures, System.currentTimeMillis() - startTime);
              notifyFailed(t);
              break;
            }

            try {
              TimeUnit.MILLISECONDS.sleep(delay);
              LOG.debug("Retry to start service {}", delegateServiceName);
              currentDelegate = delegate.get();
            } catch (InterruptedException e) {
              // This thread will be interrupted from the doStop() method. Don't reset the interrupt flag.
            }
          }
        }
      }
    };
    startupThread.start();
    notifyStarted();
  }

  @Override
  protected void doStop() {
    // doStop() won't be called until doStart() returns, hence the startupThread would never be null
    stopped = true;
    startupThread.interrupt();
    Uninterruptibles.joinUninterruptibly(startupThread);

    // Stop the started service if it exists and propagate the stop state
    // There could be a small race between the delegate service started successfully and
    // the setting of the startedService field. When that happens, the stop failure state is not propagated.
    // Nevertheless, there won't be any service left behind without stopping.
    if (startedService != null) {
      Futures.addCallback(startedService.stop(), new FutureCallback<State>() {
        @Override
        public void onSuccess(State result) {
          notifyStopped();
        }

        @Override
        public void onFailure(Throwable t) {
          notifyFailed(t);
        }
      }, Threads.SAME_THREAD_EXECUTOR);
      return;
    }

    // If there is no started service, stop the current delete, but no need to propagate the stop state
    // because if the underlying service is not yet started due to failure, it shouldn't affect the stop state
    // of this retrying service.
    if (currentDelegate != null) {
      currentDelegate.stop().addListener(new Runnable() {
        @Override
        public void run() {
          notifyStopped();
        }
      }, Threads.SAME_THREAD_EXECUTOR);
      return;
    }

    // Otherwise, if nothing has been started yet, just notify this service is stopped
    notifyStopped();
  }

  @Override
  public String toString() {
    return "EndureService{" + delegateServiceName + "}";
  }

  /**
   * Returns the {@link Service} instance that was started successfully by this service. If the underlying
   * service hasn't been started successfully, {@code null} will be returned.
   */
  @VisibleForTesting
  @Nullable
  Service getStartedService() {
    return startedService;
  }
}
