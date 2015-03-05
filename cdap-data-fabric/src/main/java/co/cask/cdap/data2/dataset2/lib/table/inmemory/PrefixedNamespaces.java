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

package co.cask.cdap.data2.dataset2.lib.table.inmemory;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Joiner;

/**
 * Defines a prefix-based namespace strategy for in-memory and LevelDB tables.
 * Only used to generate internal representation (name) of datasets, in-memory and LevelDB tables.
 *
 * Note: This namespacing should always only be one-way, i.e. we should never have to derive the components
 * (root prefix, namespace id and dataset name) from the namespaced name.
 */
public class PrefixedNamespaces {

  private PrefixedNamespaces() {
  }

  /**
   * Generates a table name of the form:
   * <pre>
   *   {root-prefix}_{namespace-id}.{dataset-name}
   * </pre>
   *
   * @param cConf CDAP configuration
   * @param namespaceId the dataset's namespace
   * @param name the dataset's name
   * @return the dataset's name qualified with a root prefix and a namespace id
   */
  public static String namespace(CConfiguration cConf, String namespaceId, String name) {
    String rootPrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    String namespace = Joiner.on("_").join(rootPrefix, namespaceId);
    return Joiner.on(".").join(namespace, name);
  }
}
