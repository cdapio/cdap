package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;

import java.io.IOException;

/**
 * Provides access to {@link DatasetDefinition} while removing burden of managing classloader separatelly.
 * @param <D> type of {@link Dataset} that {@link com.continuuity.api.dataset.DatasetDefinition} creates
 * @param <A> type of {@link DatasetAdmin} that {@link com.continuuity.api.dataset.DatasetDefinition} creates
 */
public final class DatasetType<D extends Dataset, A extends DatasetAdmin> {

  private final DatasetDefinition<D, A> delegate;
  private final ClassLoader classLoader;

  public DatasetType(DatasetDefinition<D, A> delegate, ClassLoader classLoader) {
    this.delegate = delegate;
    this.classLoader = classLoader;
  }

  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return delegate.configure(instanceName, properties);
  }

  public A getAdmin(DatasetSpecification spec) throws IOException {
    return delegate.getAdmin(spec, classLoader);
  }

  public D getDataset(DatasetSpecification spec) throws IOException {
    return delegate.getDataset(spec, classLoader);
  }
}
