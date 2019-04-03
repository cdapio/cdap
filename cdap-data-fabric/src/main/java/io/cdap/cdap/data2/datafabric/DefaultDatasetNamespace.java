/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Default dataset namespace, which namespaces by configuration setting 
 * {@link co.cask.cdap.common.conf.Constants.Dataset#TABLE_PREFIX} and
 * the {@link NamespaceId} in which the dataset instance was created.
 */
public class DefaultDatasetNamespace implements DatasetNamespace {
  private final String rootPrefix;

  public DefaultDatasetNamespace(CConfiguration conf) {
    String root = conf.get(Constants.Dataset.TABLE_PREFIX);
    this.rootPrefix = root + ".";
  }

  @Override
  public DatasetId namespace(String datasetInstanceName) {
    return namespace(NamespaceId.SYSTEM.dataset(datasetInstanceName));
  }

  @Override
  public DatasetId namespace(DatasetId datasetInstanceId) {
    String namespaced = namespace(datasetInstanceId.getParent(), datasetInstanceId.getEntityName());
    return datasetInstanceId.getParent().dataset(namespaced);
  }

  @Override
  public String namespace(NamespaceId namespaceId, String suffix) {
    return rootPrefix + namespaceId.getNamespace() + "." + suffix;
  }

  @Override
  public DatasetId fromNamespaced(String namespaced) {
    Preconditions.checkArgument(namespaced != null, "Dataset name should not be null");
    // Dataset name is of the format <table-prefix>.<namespace>.<dataset-name>
    Preconditions.checkArgument(namespaced.startsWith(rootPrefix), "Dataset name should start with " + rootPrefix);
    // rootIndex is the index of the first character after the root prefix
    int rootIndex = rootPrefix.length();
    // namespaceIndex is the index of the first dot after the rootIndex
    int namespaceIndex = namespaced.indexOf(".", rootIndex);
    // This check implies also that namespace is non-empty
    // Also, '.' is not permitted in namespace name. So this should return the full namespace.
    Preconditions.checkArgument(namespaceIndex > rootIndex,
                                "Dataset name is expected to be in the format %s<namespace>.<dataset-name>. Found - %s",
                                rootPrefix, namespaced);
    NamespaceId namespace = new NamespaceId(namespaced.substring(rootIndex, namespaceIndex));
    String datasetName = namespaced.substring(namespaceIndex + 1);
    Preconditions.checkArgument(!datasetName.isEmpty(),
                                "Dataset name is expected to be in the format %s<namespace>.<dataset-name>. Found - %s",
                                rootPrefix, namespaced);
    return namespace.dataset(datasetName);
  }

  @Override
  @Nullable
  public DatasetId fromNamespaced(DatasetId datasetInstanceId) {
    String namespacedDatasetName = datasetInstanceId.getEntityName();
    if (!contains(namespacedDatasetName, datasetInstanceId.getNamespace())) {
      return null;
    }
    String prefix = rootPrefix + datasetInstanceId.getNamespace() + ".";
    String nonNamespaced = namespacedDatasetName.substring(prefix.length());
    return datasetInstanceId.getParent().dataset(nonNamespaced);
  }

  @Override
  public boolean contains(String name, String namespaceId) {
    String prefix = rootPrefix + namespaceId + ".";
    return name.startsWith(prefix);
  }
}
