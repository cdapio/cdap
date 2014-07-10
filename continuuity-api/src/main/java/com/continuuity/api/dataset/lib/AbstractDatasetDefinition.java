package com.continuuity.api.dataset.lib;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;

/**
 * Basic abstract implementation of {@link DatasetDefinition}.
 * @param <D> defines data operations that can be performed on this dataset instance
 * @param <A> defines administrative operations that can be performed on this dataset instance
 */
@Beta
public abstract class AbstractDatasetDefinition<D extends Dataset, A extends DatasetAdmin>
  implements DatasetDefinition<D, A> {

  private final String name;

  /**
   * Ctor that takes in name of this dataset type.
   * @param name this dataset type name
   */
  protected AbstractDatasetDefinition(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }
}
