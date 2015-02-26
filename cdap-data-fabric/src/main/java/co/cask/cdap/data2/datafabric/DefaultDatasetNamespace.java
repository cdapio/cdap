/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.data2.dataset2.DatasetNamespace;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Default dataset namespace, which namespaces by configuration setting {@link Constants.Dataset#TABLE_PREFIX} and
 * the {@link Id.Namespace} in which the dataset instance was created
 */
public class DefaultDatasetNamespace implements DatasetNamespace {
  private final String rootPrefix;

  public DefaultDatasetNamespace(CConfiguration conf) {
    String root = conf.get(Constants.Dataset.TABLE_PREFIX);
    this.rootPrefix = root + ".";
  }

  @Override
  public Id.DatasetInstance namespace(String datasetInstanceName) {
    return namespace(Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE, datasetInstanceName));
  }

  @Override
  public Id.DatasetInstance namespace(Id.DatasetInstance datasetInstanceId) {
    String namespaced = namespace(datasetInstanceId.getNamespace(), datasetInstanceId.getId());
    return Id.DatasetInstance.from(datasetInstanceId.getNamespace(), namespaced);
  }

  @Override
  public String namespace(Id.Namespace namespaceId, String suffix) {
    return rootPrefix + namespaceId.getId() + "." + suffix;
  }

  @Override
  public Id.DatasetInstance fromNamespaced(String namespaced) {
    Preconditions.checkArgument(namespaced != null, "Dataset name should not be null");
    // Dataset name is of the format cdap.<namespace>.<dataset-name>
    String invalidFormatError = String.format("Invalid format for dataset name '%s'. " +
                                                "Expected - cdap.<namespace>.<dataset-name>", namespaced);
    String [] parts = namespaced.split("\\.", 3);
    Preconditions.checkArgument(parts.length == 3, invalidFormatError);
    return Id.DatasetInstance.from(Id.Namespace.from(parts[1]), parts[2]);
  }

  @Override
  @Nullable
  public Id.DatasetInstance fromNamespaced(Id.DatasetInstance datasetInstanceId) {
    String namespacedDatasetName = datasetInstanceId.getId();
    if (!contains(namespacedDatasetName, datasetInstanceId.getNamespaceId())) {
      return null;
    }
    String prefix = rootPrefix + datasetInstanceId.getNamespaceId() + ".";
    String nonNamespaced = namespacedDatasetName.substring(prefix.length());
    return Id.DatasetInstance.from(datasetInstanceId.getNamespace(), nonNamespaced);
  }

  @Override
  public boolean contains(String name, String namespaceId) {
    String prefix = rootPrefix + namespaceId + ".";
    return name.startsWith(prefix);
  }
}
