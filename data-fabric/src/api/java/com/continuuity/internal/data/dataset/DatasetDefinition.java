package com.continuuity.internal.data.dataset;

import java.io.IOException;

/**
 * The instance of the class that implements this interface defines concrete named dataset type implementation.
 *
 * The dataset implementation defines:
 * <ul>
 *   <li>
 *     a way to configure new dataset instance by using {@link #configure(String, DatasetInstanceProperties)} method
 *   </li>
 *   <li>
 *     a way to perform administrative operations on the dataset instance (e.g. create, drop etc.) by providing
 *     implementation of {@link DatasetAdmin} via {@link #getAdmin(DatasetInstanceSpec)} method
 *   </li>
 *   <li>
 *     a way to perform operations to manipulate data of the dataset instance (e.g. read, write etc.) by providing
 *     implementation of {@link Dataset} via {@link #getDataset(DatasetInstanceSpec)} method
 *   </li>
 * </ul>
 *
 * @param <D> defines data operations that can be performed on this dataset instance
 * @param <A> defines administrative operations that can be performed on this dataset instance
 */
public interface DatasetDefinition<D extends Dataset, A extends DatasetAdmin> {

  /**
   * @return name of this dataset implementation
   */
  String getName();

  /**
   * Configures new instance of the dataset.
   * @param instanceName name of the instance
   * @param properties instance configuration properties
   * @return instance of {@link DatasetInstanceSpec} that fully describes dataset instance.
   *         The {@link DatasetInstanceSpec} can be used to create {@link DatasetAdmin} and {@link Dataset} to perform
   *         administrative and data operations respectively, see {@link #getAdmin(DatasetInstanceSpec)} and
   *         {@link #getDataset(DatasetInstanceSpec)}.
   */
  DatasetInstanceSpec configure(String instanceName, DatasetInstanceProperties properties);

  /**
   * Provides dataset admin to be used to perform administrative operations on the dataset instance defined by passed
   * {@link DatasetInstanceSpec}.
   * @param spec specification of the dataset instance.
   * @return dataset admin to perform administrative operations
   * @throws IOException
   */
  A getAdmin(DatasetInstanceSpec spec) throws IOException;

  /**
   * Provides dataset to be used to perform data operations on the dataset instance data defined by passed
   * {@link DatasetInstanceSpec}.
   * @param spec specification of the dataset instance.
   * @return dataset to perform object operations
   * @throws IOException
   */
  D getDataset(DatasetInstanceSpec spec) throws IOException;
}
