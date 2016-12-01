/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

/**
 * A Guava {@link Service} that wrap around another {@link Service} such that, if the wrapped service failed
 * to start, it will get restarted based on the {@link RetryStrategy}.
 */
public class RetryOnStartFailureService extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(RetryOnStartFailureService.class);

  private final Thread startupThread;
  private final String delegateServiceName;
  private volatile Service currentDelegate;

  /**
   * Creates a new instance.
   *
   * @param delegate a {@link Supplier} that gives new instance of the delegating Service.
   * @param retryStrategy strategy to use for retrying
   */
  public RetryOnStartFailureService(final Supplier<Service> delegate, final RetryStrategy retryStrategy) {
    final Service service = delegate.get();
    this.delegateServiceName = service.getClass().getSimpleName();
    this.startupThread = new Thread("Endure-Service-" + delegateServiceName) {
      @Override
      public void run() {
        int failures = 0;
        long startTime = System.currentTimeMillis();
        long delay = 0L;
        Service delegateService = service;

        while (delay >= 0 && !isInterrupted()) {
          try {
            currentDelegate = delegateService;
            delegateService.start().get();
            break;
          } catch (InterruptedException e) {
            // a service's start can be interrupted, so we don't want to suppress that
            interrupt();
            break;
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
              delegateService = delegate.get();
            } catch (InterruptedException e) {
              interrupt();
            }
          }
        }

        if (isInterrupted()) {
          LOG.warn("Stop requested for service {} during start.", delegateServiceName);
        }
      }
    };
  }

  @Override
  protected void doStart() {
    startupThread.start();
    notifyStarted();
  }

  @Override
  protected void doStop() {
    startupThread.interrupt();
    Uninterruptibles.joinUninterruptibly(startupThread);

    Service service = currentDelegate;
    if (service != null) {
      Futures.addCallback(service.stop(), new FutureCallback<State>() {
        @Override
        public void onSuccess(State result) {
          notifyStopped();
        }

        @Override
        public void onFailure(Throwable t) {
          notifyFailed(t);
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    } else {
      notifyStopped();
    }
  }

  @Override
  public String toString() {
    return "EndureService{" + delegateServiceName + "}";
  }
}
