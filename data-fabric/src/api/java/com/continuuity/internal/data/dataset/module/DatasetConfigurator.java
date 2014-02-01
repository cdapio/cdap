package com.continuuity.internal.data.dataset.module;

import com.continuuity.internal.data.dataset.DatasetDefinition;

/**
 * Configures instance of {@link com.continuuity.internal.data.dataset.DatasetDefinition}.
 *
 * This allows to plug support for different third-party system dependencies for datasets.
 */
public interface DatasetConfigurator {
  /**
   * Configures instance of {@link com.continuuity.internal.data.dataset.DatasetDefinition}
   * @param dataset to configure
   * @param <T> type of the dataset
   * @return false if this should be the last configurator in chain to be used for this dataset, true otherwise
   */
  <T extends DatasetDefinition> boolean configure(T dataset);
}
