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

package co.cask.cdap.data2.util.hbase;

import com.google.inject.Provider;
import com.google.inject.ProvisionException;
import org.apache.twill.internal.utils.Instances;

/**
 * Common class factory behavior for classes which need specific implementations depending on HBase versions.
 * Specific factories can subclass this class and simply plug in the class names for their implementations.
 *
 * @param <T> Version specific class provided by this factory.
 */
public abstract class HBaseVersionSpecificFactory<T> implements Provider<T> {
  @Override
  public T get() {
    T instance = null;
    try {
      switch (HBaseVersion.get()) {
        case HBASE_94:
          instance = createInstance(getHBase94Classname());
          break;
        case HBASE_96:
          instance = createInstance(getHBase96Classname());
          break;
        case HBASE_98:
          instance = createInstance(getHBase98Classname());
          break;
        case UNKNOWN:
          throw new ProvisionException("Unknown HBase version: " + HBaseVersion.getVersionString());
      }
    } catch (ClassNotFoundException cnfe) {
      throw new ProvisionException(cnfe.getMessage(), cnfe);
    }
    return instance;
  }

  protected T createInstance(String className) throws ClassNotFoundException {
    Class clz = Class.forName(className);
    return (T) Instances.newInstance(clz);
  }

  protected abstract String getHBase94Classname();
  protected abstract String getHBase96Classname();
  protected abstract String getHBase98Classname();
}
