/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.common.plugin;

import java.util.concurrent.Callable;

/**
 * Sets the context classloader before calling a callable, and resets when finished.
 */
public class ClassLoaderCaller extends Caller {
  private final Caller delegate;
  private final ClassLoader classLoader;

  private ClassLoaderCaller(Caller delegate, ClassLoader classLoader) {
    this.delegate = delegate;
    this.classLoader = classLoader;
  }

  @Override
  public <T> T call(Callable<T> callable, CallArgs args) throws Exception {
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
    try {
      return delegate.call(callable, args);
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }
  }

  public static Caller wrap(Caller delegate, ClassLoader classLoader) {
    return delegate;
    //return new ClassLoaderCaller(delegate, classLoader);
  }
}
