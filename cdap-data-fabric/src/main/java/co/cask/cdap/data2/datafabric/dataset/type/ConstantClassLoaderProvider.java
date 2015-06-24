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

package co.cask.cdap.data2.datafabric.dataset.type;

import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.proto.DatasetModuleMeta;
import com.google.common.base.Objects;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Simply returns the same {@link ClassLoader} for every dataset module. The assumption is that the
 * classloader has access to any dataset module that may be created.
 * This is true for a {@link ProgramClassLoader} for example.
 */
public class ConstantClassLoaderProvider implements DatasetClassLoaderProvider {
  private final ClassLoader classLoader;

  public ConstantClassLoaderProvider() {
    this(null);
  }

  public ConstantClassLoaderProvider(@Nullable ClassLoader classLoader) {
    this.classLoader = classLoader == null ?
      Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), getClass().getClassLoader()) :
      classLoader;
  }

  @Override
  public ClassLoader get(DatasetModuleMeta moduleMeta) throws IOException {
    return classLoader;
  }

  @Override
  public ClassLoader getParent() {
    return classLoader;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
