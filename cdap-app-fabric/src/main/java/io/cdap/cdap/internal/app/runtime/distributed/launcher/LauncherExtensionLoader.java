/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.launcher;

import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.runtime.spi.launcher.Launcher;

import java.util.Collections;
import java.util.Set;

/**
 *
 */
public class LauncherExtensionLoader extends AbstractExtensionLoader<String, Launcher> {

  LauncherExtensionLoader(String extensionDir) {
    super(extensionDir);
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(Launcher launcher) {
    return Collections.singleton(launcher.getName());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // Only allow spi classes.
    return new FilterClassLoader.Filter() {
      @Override
      //io.cdap.cdap.runtime.spi.launcher
      public boolean acceptResource(String resource) {
        return resource.startsWith("io/cdap/cdap/runtime/spi/launcher");
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return packageName.startsWith("io.cdap.cdap.runtime.spi.launcher");
      }
    };
  }
}
