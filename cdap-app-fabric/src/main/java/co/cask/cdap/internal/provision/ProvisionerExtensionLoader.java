/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.provision;

import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.extension.AbstractExtensionLoader;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Loads provisioners from the extensions directory.
 */
public class ProvisionerExtensionLoader extends AbstractExtensionLoader<String, Provisioner>
  implements ProvisionerProvider {

  public ProvisionerExtensionLoader(String extDirs) {
    super(extDirs);
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(Provisioner provisioner) {
    return Collections.singleton(provisioner.getSpec().getName());
  }

  // filter all non-spi classes to provide isolation from CDAP's classes. For example, dataproc provisioner uses
  // a different guava than CDAP's guava.
  @Override
  protected ClassLoader getExtensionParentClassLoader() {
    return new FilterClassLoader(super.getExtensionParentClassLoader(), new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return resource.startsWith("co/cask/cdap/runtime/spi");
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return packageName.startsWith("co/cask/cdap/runtime/spi");
      }
    });
  }

  @Override
  public Map<String, Provisioner> loadProvisioners() {
    return getAll();
  }
}
