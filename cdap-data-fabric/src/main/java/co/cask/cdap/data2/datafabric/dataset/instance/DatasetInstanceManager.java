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
   * @param spec {@link co.cask.cdap.api.dataset.DatasetSpecification} of the dataset instance to be added
   */
  public void add(final DatasetSpecification spec) {
    mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, Void>() {
      @Override
      public Void call(MDSDatasets datasets) throws Exception {
        datasets.getInstanceMDS().write(spec);
        return null;
      }
    });
  }

  /**
   * @param instanceName name of the dataset instance
   * @return dataset instance's {@link co.cask.cdap.api.dataset.DatasetSpecification}
   */
  public DatasetSpecification get(final String instanceName) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, DatasetSpecification>() {
      @Override
      public DatasetSpecification call(MDSDatasets datasets) throws Exception {
        return datasets.getInstanceMDS().get(instanceName);
      }
    });
  }

  /**
   * @return collection of {@link co.cask.cdap.api.dataset.DatasetSpecification} of all dataset instances
   */
  public Collection<DatasetSpecification> getAll() {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, Collection<DatasetSpecification>>() {
      @Override
      public Collection<DatasetSpecification> call(MDSDatasets datasets) throws Exception {
        return datasets.getInstanceMDS().getAll();
      }
    });
  }

  /**
   * Deletes dataset instance
   * @param instanceName name of the instance to delete
   * @return true if deletion succeeded, false otherwise
   */
  public boolean delete(final String instanceName) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, Boolean>() {
      @Override
      public Boolean call(MDSDatasets datasets) throws Exception {
        return datasets.getInstanceMDS().delete(instanceName);
      }
    });
  }
}
