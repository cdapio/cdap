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

package io.cdap.cdap.sourcecontrol.operationrunner;

import java.util.function.BiFunction;

/**
 * A provider class to help match and create underlying exception from a RemoteExecutionException.
 * The underlying exception class should have a constructor of type (string,throwable).
 *
 * @param <T> Underlying exception type for the remote exception
 */
public class RemoteExceptionProvider<T extends Exception> {
  private final Class<T> exceptionClass;
  private final BiFunction<String, Throwable, T> provider;

  public RemoteExceptionProvider(Class<T> exceptionClass, BiFunction<String, Throwable, T> provider) {
    this.exceptionClass = exceptionClass;
    this.provider = provider;
  }

  public boolean isOfClass(String className) {
    return exceptionClass.getName().equals(className);
  }

  public T createException(String message, Throwable cause) {
    return provider.apply(message, cause);
  }
}
