package com.continuuity.data2.dataset2;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;

/**
 * Builds {@link DatasetDefinitionRegistry}.
 */
public interface DatasetDefinitionRegistryFactory {
  /**
   * @return instance of {@link DatasetDefinitionRegistry}
   */
  DatasetDefinitionRegistry create();
}
