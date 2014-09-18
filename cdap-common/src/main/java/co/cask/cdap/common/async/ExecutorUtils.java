/*
 * Copyright © 2014 Cask Data, Inc.
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

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory and utility methods for dealing with {@link Executor}.
 */
public final class ExecutorUtils {

  /**
   * Creates an {@link Executor} that always create new thread to execute runnable.
   *
   * @param threadFactory thread factory for creating new thread.
   * @return a new {@link Executor} instance
   */
  public static Executor newThreadExecutor(final ThreadFactory threadFactory) {
    return new Executor() {

      final AtomicInteger id = new AtomicInteger(0);

      @Override
      public void execute(Runnable command) {
        Thread t = threadFactory.newThread(command);
        t.start();
      }
    };
  }

  private ExecutorUtils() {
  }
}
