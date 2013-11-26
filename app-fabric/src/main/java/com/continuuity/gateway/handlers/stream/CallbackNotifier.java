package com.continuuity.gateway.handlers.stream;

import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper class used for notifying future callbacks.
 */
final class CallbackNotifier {
  private static final Logger LOG = LoggerFactory.getLogger(CallbackNotifier.class);
  private final ExecutorService executorService;
  private final Iterable<FutureCallback<Void>> futureCallbacks;
  private final AtomicBoolean notified = new AtomicBoolean(false);

  public CallbackNotifier(ExecutorService executorService, Iterable<FutureCallback<Void>> futureCallbacks) {
    this.executorService = executorService;
    this.futureCallbacks = futureCallbacks;
  }

  public void notifySuccess() {
    // Return if already notified
    if (!notified.compareAndSet(false, true)) {
      return;
    }

    executorService.submit(
      new Runnable() {
        @Override
        public void run() {
          for (FutureCallback<Void> callback : futureCallbacks) {
            try {
              callback.onSuccess(null);
            } catch (Throwable e) {
              LOG.error("Got exception when calling callback success handler", e);
            }
          }
        }
      }
    );
  }

  public void notifyFailure(final Throwable e) {
    // Return if already notified
    if (!notified.compareAndSet(false, true)) {
      return;
    }

    executorService.submit(
      new Runnable() {
        @Override
        public void run() {
          for (FutureCallback<Void> callback : futureCallbacks) {
            try {
              callback.onFailure(e);
            } catch (Throwable e) {
              LOG.error("Got exception when calling callback failure handler", e);
            }
          }
        }
      }
    );
  }
}
