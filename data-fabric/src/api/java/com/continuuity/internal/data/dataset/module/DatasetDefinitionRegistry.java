package com.continuuity.internal.data.dataset.module;

import com.continuuity.internal.data.dataset.DatasetDefinition;

/**
 * Registry of dataset definitions and other components in a Datasets System.
 *
 * The implementation of this interface doesn't have to be thread-safe.
 */
public interface DatasetDefinitionRegistry {
  /**
   * Adds {@link DatasetDefinition} to the registry.
   *
   * After it was added it is available thru {@link #get(String)} method.
   *
   * @param def definition to add
   */
  void add(DatasetDefinition def);

  /**
   * Adds {@link DatasetConfigurator} to the registry.
   *
   * This {@link DatasetConfigurator} will be added to the end of the dataset configuration chain. It will be applied to
   * all {@link DatasetDefinition} added after it within this module.
   *
   * @param configurator configurator to add
   */
  void add(DatasetConfigurator configurator);

  /**
   * Gets {@link DatasetDefinition} previously added to the registry.
   * @param datasetTypeName dataset type name, should be same as
   *                        {@link com.continuuity.internal.data.dataset.DatasetDefinition#getName()}
   * @param <T> type of the returned {@link DatasetDefinition}
   * @return instance of {@link DatasetDefinition}
   */
  <T extends DatasetDefinition> T get(String datasetTypeName);
}
