package com.continuuity.api.data;

/**
 * This is the abstract base class for all data sets. A data set is an
 * implementation of a data pattern that can be reused across programs and
 * applications. The life cycle of a data set is as follows:
 * <li>An application declares the data sets that its programs use.</li>
 * <li>When the application is deployed, the DataSet object is created and
 *   its configure() method is called. This method returns a specification
 *   that contains all information needed to instantiate the data set at
 *   runtime.</li>
 * <li>At runtime (in a flow or procedure) the data set is instantiated
 *   by dependency injection using @UseDataSet annotation. This uses the
 *   constructor of the the data set that takes the above specification
 *   as argument. It is important that the data set is instantiated through
 *   the context: This also makes sure that the data fabric runtime is
 *   properly injected into the data set.
 *   </li>
 * <li>Hence every DataSet must implement a configure() method and a
 *   constructor from DataSetSpecification.</li>
 */
public abstract class DataSet {

  // the name of the data set (instance)
  private final String name;

  /**
   * Get the name of this data set.
   * @return the name of the data set
   */
  public final String getName() {
    return this.name;
  }

  /**
   * Base constructor that only sets the name of the data set.
   * @param name the name of the data set
   */
  public DataSet(String name) {
    this.name = name;
  }

  /**
   * Constructor to instantiate the data set at runtime.
   * @param spec the data set specification for this data set
   */
  public DataSet(DataSetSpecification spec) {
    this(spec.getName());
  }

  /**
   * This method is called at deployment time and must return the complete
   * specification that is needed to instantiate the data set at runtime
   * (@see #DataSet(DataSetSpecification)).
   * @return a data set spec that has all meta data needed for runtime
   *         instantiation
   */
  public abstract DataSetSpecification configure();

  /**
   * This method is called at runtime to release all resources that the dataset may have acquired.
   * After this is called, it is guaranteed that this instance of the dataset will not be used any more.
   * The base implementation is to do nothing.
   */
  public void close() {
  }
}
