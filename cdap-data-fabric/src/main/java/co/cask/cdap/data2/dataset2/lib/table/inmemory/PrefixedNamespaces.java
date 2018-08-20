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
import co.cask.cdap.proto.id.DatasetId;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Defines a prefix-based namespace strategy for in-memory and LevelDB tables.
 * Only used to generate internal representation (name) of datasets, in-memory and LevelDB tables.
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

  /**
   * Extracts the {@link DatasetId} from a dataset's namespaced name.
   *
   * @param cConf CDAP configuration
   * @param namespacedName the dataset's name qualified with a root prefix and a namespace id, as constructed
   *                       by {@link #namespace(CConfiguration, String, String)}.
   * @return {@link DatasetId} for the
   */
  public static DatasetId getDatasetId(CConfiguration cConf, String namespacedName) {
    String rootPrefix = cConf.get(Constants.Dataset.TABLE_PREFIX) + "_";
    Preconditions.checkArgument(namespacedName.startsWith(rootPrefix),
                                "Expected '%s' to start with '%s'.", namespacedName, rootPrefix);
    String namespaceAndName = namespacedName.substring(rootPrefix.length());
    String[] parts = namespaceAndName.split("\\.", 2);
    Preconditions.checkArgument(parts.length == 2,
                                "Expected there to be at least one '.' in: %s.", namespaceAndName);
    return new DatasetId(parts[0], parts[1]);
  }
}
