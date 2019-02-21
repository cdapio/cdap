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

package co.cask.cdap.common.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Utilities for Futures.
 */
public class Futures {

  private Futures() {
    // no-op
  }

  /**
   * Wait for all of the given futures, returning a list of their results in the order they were given.
   * If any of the given futures fail, the correspond entry in the list will be null. This is indistinguishable
   * from futures that actually return null.
   *
   * @param futures the futures to get
   * @param <T> the type of future result
   * @return list of future results
   */
  public static <T> List<T> successfulAsList(Iterable<Future<T>> futures) {
    List<T> results = new ArrayList<>();
    for (Future<T> f : futures) {
      try {
        results.add(f.get());
      } catch (Exception e) {
        results.add(null);
      }
    }
    return results;
  }

  /**
   * Wait for all of the given futures, returning a list of their results in the order they were given.
   * If any of the given futures, an ExecutionException will be thrown. If multiple fail, their causes will be added to
   * the cause of the ExecutionException as a suppressed exception.
   *
   * @param futures the futures to get
   * @param <T> the type of future result
   * @return list of future results
   * @throws ExecutionException if any of the futures failed
   */
  public static <T> List<T> allAsList(Iterable<Future<T>> futures) throws ExecutionException {
    List<T> results = new ArrayList<>();
    Throwable failure = null;
    for (Future<T> f : futures) {
      try {
        results.add(f.get());
      } catch (Exception e) {
        Throwable cause = e.getCause();
        if (failure == null) {
          failure = cause;
        } else {
          failure.addSuppressed(cause);
        }
      }
    }
    if (failure != null) {
      throw new ExecutionException(failure);
    }
    return results;
  }

  /**
   * Wait for all of the given futures, returning a list of their results in the order they were given.
   * If any of the given futures, an ExecutionException will be thrown. If multiple fail, their causes will be added to
   * the cause of the ExecutionException as a suppressed exception.
   *
   * @param futures the futures to get
   * @param dur the maximum amount of time to wait for any individual future
   * @param timeUnit the duration time unit
   * @param <T> the type of future result
   * @return list of future results
   * @throws ExecutionException if any of the futures failed
   */
  public static <T> List<T> allAsList(Iterable<Future<T>> futures, long dur,
                                      TimeUnit timeUnit) throws ExecutionException {
    List<T> results = new ArrayList<>();
    Throwable failure = null;
    for (Future<T> f : futures) {
      try {
        results.add(f.get(dur, timeUnit));
      } catch (Exception e) {
        Throwable cause = e.getCause();
        if (failure == null) {
          failure = cause;
        } else {
          failure.addSuppressed(cause);
        }
      }
    }
    if (failure != null) {
      throw new ExecutionException(failure);
    }
    return results;
  }
}
