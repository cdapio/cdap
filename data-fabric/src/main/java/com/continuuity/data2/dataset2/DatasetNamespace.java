package com.continuuity.data2.dataset2;

/**
 * Performs namespacing for data set names.
 */
public interface DatasetNamespace {
  /**
   * @param name name of the dataset
   * @return namespaced name of the dataset
   */
  String namespace(String name);

  /**
   * @param name namespaced name of the dataset
   * @return original name of the dataset
   */
  // TODO: possible design issue, see REACTOR-217
  String fromNamespaced(String name);
}
