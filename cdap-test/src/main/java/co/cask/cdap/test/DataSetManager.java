/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.test;

/**
 * Instances of this class are for managing {@link co.cask.cdap.api.dataset.DataSet Datasets}.
 *
 * <p>
 * <b>Note:</b>
 * Changes made with the instance of the dataset acquired via {@link #get()} are not visible to other components
 * unless {@link #flush()} is called.
 * </p>
 *
 * <p>
 * Typical usage for read:
 * </p>
 *
 * <pre><code> 
 *   ApplicationManager appManager = deployApplication(AppWithTable.class);
 *   DataSetManager{@literal <}Table> myTableManager = appManager.getDataset("my_table");
 *   myTableManager = appManager.getDataset("my_table");
 *   String value = myTableManager.get().get(new Get("key1", "column1")).getString("column1");
 * </code></pre>
 *
 * <p>
 * Typical usage for write:
 * </p>
 *
 * <pre><code> 
 *   ApplicationManager appManager = deployApplication(AppWithTable.class);
 *   DataSetManager{@literal <}Table> myTableManager = appManager.getDataset("my_table");
 *   myTableManager.get().put(new Put("key1", "column1", "value1"));
 *   myTableManager.flush();
 * </code></pre>
 *
 * @param <T> actual type of the dataset
 */
public interface DataSetManager<T> {
  /**
   * The returned instance of the dataset will see only changes made before the manager was acquired, or before
   * the latest call to {@link #flush()}.
   *
   * @return the dataset instance
   */
  T get();

  /**
   * Makes changes performed using the dataset instance acquired via {@link #get()} visible to all other components.
   * Can be called multiple times on the same instance of the dataset.
   */
  void flush();
}
