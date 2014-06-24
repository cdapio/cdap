package com.continuuity.data2.datafabric.dataset.instance;

import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.data2.datafabric.dataset.service.mds.MDSDatasets;
import com.continuuity.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import com.continuuity.data2.dataset2.tx.TxCallable;
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
   * @param spec {@link com.continuuity.api.dataset.DatasetSpecification} of the dataset instance to be added
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
   * @return dataset instance's {@link com.continuuity.api.dataset.DatasetSpecification}
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
   * @return collection of {@link com.continuuity.api.dataset.DatasetSpecification} of all dataset instances
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
