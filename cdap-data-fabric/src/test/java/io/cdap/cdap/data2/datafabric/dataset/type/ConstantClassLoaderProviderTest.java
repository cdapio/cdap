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

package io.cdap.cdap.data2.datafabric.dataset.type;

import java.net.URL;
import java.net.URLClassLoader;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ConstantClassLoaderProvider}.
 */
public class ConstantClassLoaderProviderTest {

  @Test
  public void testExplicitContextAndFallbackClassLoaderSelection() throws Exception {
    ClassLoader originalContextCl = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader explicitCl = new URLClassLoader(new URL[0], getClass().getClassLoader());
        URLClassLoader contextCl = new URLClassLoader(new URL[0], getClass().getClassLoader())) {
      // 1. Explicit classloader is returned directly
      try (ConstantClassLoaderProvider provider = new ConstantClassLoaderProvider(explicitCl)) {
        Assert.assertSame(explicitCl, provider.get(null, getClass().getClassLoader()));
      }

      // 2. Null classloader uses non-null context classloader
      Thread.currentThread().setContextClassLoader(contextCl);
      try (ConstantClassLoaderProvider provider = new ConstantClassLoaderProvider()) {
        Assert.assertSame(contextCl, provider.get(null, getClass().getClassLoader()));
      }

      // 3. Null classloader + null context classloader falls back to ConstantClassLoaderProvider's classloader
      Thread.currentThread().setContextClassLoader(null);
      try (ConstantClassLoaderProvider provider = new ConstantClassLoaderProvider(null)) {
        Assert.assertSame(
            ConstantClassLoaderProvider.class.getClassLoader(),
            provider.get(null, getClass().getClassLoader()));
      }
    } finally {
      Thread.currentThread().setContextClassLoader(originalContextCl);
    }
  }
}
