/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test;

import org.junit.rules.ExternalResource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link ExternalResource} that maintain a singleton to another {@link ExternalResource} so that
 * it can be used in test suite as well as in individual test in the suite.
 */
public class SingletonExternalResource extends ExternalResource {

  private static final AtomicInteger INSTANCES = new AtomicInteger(0);
  private final ExternalResource externalResource;

  public SingletonExternalResource(ExternalResource externalResource) {
    this.externalResource = externalResource;
  }

  @Override
  protected void before() throws Throwable {
    if (INSTANCES.getAndIncrement() == 0) {
      Method before = ExternalResource.class.getDeclaredMethod("before");
      before.setAccessible(true);
      try {
        before.invoke(externalResource);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  @Override
  protected void after() {
    if (INSTANCES.decrementAndGet() == 0) {
      try {
        Method after = ExternalResource.class.getDeclaredMethod("after");
        after.setAccessible(true);
        after.invoke(externalResource);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        // Should not happen
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Returns the underlying {@link ExternalResource}.
   *
   * @param <T> The actual type of the resource.
   */
  @SuppressWarnings("unchecked")
  public <T extends ExternalResource> T get() {
    Objects.requireNonNull(externalResource);
    return (T) externalResource;
  }
}
