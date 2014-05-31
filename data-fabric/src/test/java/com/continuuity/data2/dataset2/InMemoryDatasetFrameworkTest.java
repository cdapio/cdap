package com.continuuity.data2.dataset2;

/**
 *
 */
public class InMemoryDatasetFrameworkTest extends AbstractDatasetFrameworkTest {
  @Override
  protected DatasetFramework getFramework() {
    return new InMemoryDatasetFramework();
  }
}
