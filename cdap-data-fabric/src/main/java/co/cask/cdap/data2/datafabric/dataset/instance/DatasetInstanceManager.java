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

package co.cask.cdap.data2.datafabric.dataset.instance;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.data2.datafabric.dataset.service.mds.MDSDatasets;
import co.cask.cdap.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import co.cask.cdap.data2.dataset2.tx.TxCallable;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;

import java.util.Collection;

/**
 * Manages dataset instances metadata
 */
public class DatasetInstanceManager {

  private final MDSDatasetsRegistry mdsDatasets;

  @Inject
  public DatasetInstanceManager(MDSDatasetsRegistry mdsDatasets) {
    this.mdsDatasets = mdsDatasets;
  }

  /**
   * Adds dataset instance metadata
   * @param namespaceId the {@link Id.Namespace} to add the dataset instance to
   * @param spec {@link DatasetSpecification} of the dataset instance to be added
   */
  public void add(final Id.Namespace namespaceId, final DatasetSpecification spec) {
    mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, Void>() {
      @Override
      public Void call(MDSDatasets datasets) throws Exception {
        datasets.getInstanceMDS().write(namespaceId, spec);
        return null;
      }
    });
  }

  /**
   * @param datasetInstanceId {@link Id.DatasetInstance} of the dataset instance
   * @return dataset instance's {@link DatasetSpecification}
   */
  public DatasetSpecification get(final Id.DatasetInstance datasetInstanceId) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, DatasetSpecification>() {
      @Override
      public DatasetSpecification call(MDSDatasets datasets) throws Exception {
        return datasets.getInstanceMDS().get(datasetInstanceId);
      }
    });
  }

  /**
   * @param namespaceId {@link Id.Namespace} for which dataset instances are required
   * @return collection of {@link DatasetSpecification} of all dataset instances in the given namespace
   */
  public Collection<DatasetSpecification> getAll(final Id.Namespace namespaceId) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, Collection<DatasetSpecification>>() {
      @Override
      public Collection<DatasetSpecification> call(MDSDatasets datasets) throws Exception {
        return datasets.getInstanceMDS().getAll(namespaceId);
      }
    });
  }

  /**
   * Deletes dataset instance
   * @param datasetInstanceId {@link Id.DatasetInstance} of the instance to delete
   * @return true if deletion succeeded, false otherwise
   */
  public boolean delete(final Id.DatasetInstance datasetInstanceId) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, Boolean>() {
      @Override
      public Boolean call(MDSDatasets datasets) throws Exception {
        return datasets.getInstanceMDS().delete(datasetInstanceId);
      }
    });
  }
}
