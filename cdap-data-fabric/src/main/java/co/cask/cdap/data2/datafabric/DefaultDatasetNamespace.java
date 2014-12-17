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

package co.cask.cdap.data2.datafabric;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.dataset2.DatasetNamespace;

import javax.annotation.Nullable;

/**
 * Default dataset namespace, which namespace by configuration setting {@link Constants.Dataset#TABLE_PREFIX}.
 */
public class DefaultDatasetNamespace implements DatasetNamespace {
  private final String namespacePrefix;
  private final Namespace namespace;

  public DefaultDatasetNamespace(CConfiguration conf, Namespace namespace) {
    String namespacePrefix = conf.get(Constants.Dataset.TABLE_PREFIX);
    this.namespacePrefix = namespacePrefix + ".";
    this.namespace = namespace;
  }

  @Override
  public String namespace(String name) {
    return namespacePrefix + namespace.namespace(name);
  }

  @Override
  @Nullable
  public String fromNamespaced(String name) {
    if (!contains(name)) {
      return null;
    }
    return namespace.fromNamespaced(name.substring(namespacePrefix.length()));
  }

  @Override
  public boolean contains(String name) {
    return name.startsWith(namespacePrefix) &&
      namespace.contains(name.substring(namespacePrefix.length()));
  }

}
