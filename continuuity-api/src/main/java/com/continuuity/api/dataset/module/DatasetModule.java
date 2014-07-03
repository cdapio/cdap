package com.continuuity.api.dataset.module;

import com.continuuity.api.annotation.Beta;

/**
 * Implementation of {@link DatasetModule} announces dataset types and other components to the system.
 */
@Beta
public interface DatasetModule {
  /**
   * Registers dataset types and other components in the system.
   * @param registry instance of {@link DatasetDefinitionRegistry} to be used for registering components
   */
  void register(DatasetDefinitionRegistry registry);
}
