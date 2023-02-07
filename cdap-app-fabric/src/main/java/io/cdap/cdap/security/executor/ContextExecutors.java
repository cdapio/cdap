/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.security.executor;

import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Class containing utility methods for executor thread pools.
 */
public class ContextExecutors {
  static Runnable wrapRunnableWithContext(Runnable runnable) {
    String userID = SecurityRequestContext.getUserId();
    Credential userCredential = SecurityRequestContext.getUserCredential();
    String userIP = SecurityRequestContext.getUserIP();
    return () -> {
      SecurityRequestContext.setUserId(userID);
      SecurityRequestContext.setUserCredential(userCredential);
      SecurityRequestContext.setUserIP(userIP);
      try {
        runnable.run();
      } finally {
        SecurityRequestContext.reset();
      }
    };
  }

  static <T> Callable<T> wrapCallableWithContext(Callable<T> callable) {
    String userID = SecurityRequestContext.getUserId();
    Credential userCredential = SecurityRequestContext.getUserCredential();
    String userIP = SecurityRequestContext.getUserIP();
    return () -> {
      SecurityRequestContext.setUserId(userID);
      SecurityRequestContext.setUserCredential(userCredential);
      SecurityRequestContext.setUserIP(userIP);
      try {
        return callable.call();
      } finally {
        SecurityRequestContext.reset();
      }
    };
  }

  /**
   * Returns a {@link ContextInheritingScheduledThreadPoolExecutor}.
   *
   * @param corePoolSize Size of the executor thread pool
   * @param threadFactory {@link ThreadFactory} to use
   * @return a new {@link ScheduledExecutorService}
   */
  public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory) {
    return new ContextInheritingScheduledThreadPoolExecutor(corePoolSize, threadFactory);
  }
}
