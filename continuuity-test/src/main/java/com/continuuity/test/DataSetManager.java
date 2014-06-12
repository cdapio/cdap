package com.continuuity.test;

/**
 * Instance of this class is for managing {@link com.continuuity.api.data.DataSet}.
 *
 * NOTE: changes made with the instance of the dataset acquired via {@link #get()} are not visible to other components
 *       unless {@link #flush()} is called.
 *
 * Typical usage for read:
 * {@code
      ApplicationManager appManager = deployApplication(AppWithTable.class);
      DataSetManager<Table> myTableManager = appManager.getDataSet("my_table");
      myTableManager = appManager.getDataSet("my_table");
      String value = myTableManager.get().get(new Get("key1", "column1")).getString("column1");
 * }
 *
 * Typical usage for write:
 * {@code
      ApplicationManager appManager = deployApplication(AppWithTable.class);
      DataSetManager<Table> myTableManager = appManager.getDataSet("my_table");
      myTableManager.get().put(new Put("key1", "column1", "value1"));
      myTableManager.flush();
 * }
 *
 * @param <T> actual type of the dataset
 */
public interface DataSetManager<T> {
  /**
   * @return dataset instance.
   * NOTE: the returned instance of dataset will see only changes made before it was acquired.
   */
  T get();

  /**
   * Makes changes performed using dataset instance acquired via {@link #get()} visible to all other components.
   * Can be called multiple times on same instance of the dataset.
   */
  void flush();
}
