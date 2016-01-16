/*
 * Copyright © 2016 Cask Data, Inc.
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

import com.google.common.util.concurrent.AbstractIdleService;

import java.util.concurrent.Executor;

/**
 * An AbstractIdleService that overrides its executor to use one that doesn't print to stderr the exceptions
 * it gets, so that error messaging is left to whoever is using the service.
 */
public abstract class UnloggedExceptionIdleService extends AbstractIdleService {
  public static final Thread.UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER =
    new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        // no-op
      }
    };

  @SuppressWarnings("NullableProblems")
  @Override
  protected Executor executor(State state) {
    final String name = getClass().getSimpleName() + " " + state;
    return new Executor() {
      @Override
      public void execute(Runnable runnable) {
        Thread t = new Thread(runnable, name);
        t.setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
        t.start();
      }
    };
  }
}
