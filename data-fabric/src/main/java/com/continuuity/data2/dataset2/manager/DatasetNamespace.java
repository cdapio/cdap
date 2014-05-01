package com.continuuity.data2.dataset2.manager;

/**
 * Performs namespacing for data set names.
 */
public interface DatasetNamespace {
  /**
   * @param name name of the dataset
   * @return namespaced name of the dataset
   */
  String namespace(String name);
}
